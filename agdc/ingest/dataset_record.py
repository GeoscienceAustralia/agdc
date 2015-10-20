#!/usr/bin/env python

# ===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

"""
DatasetRecord: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""
from __future__ import absolute_import
from __future__ import division

import logging
import os
import re
from math import floor

from osgeo import osr

from agdc.cube_util import DatasetError, DatasetSkipError
from .ingest_db_wrapper import IngestDBWrapper
from .ingest_db_wrapper import TC_PENDING, TC_SINGLE_SCENE, TC_SUPERSEDED
from .ingest_db_wrapper import TC_MOSAIC
from .mosaic_contents import MosaicContents
from .tile_record import TileRecord, TileRepository


# Set up logger.
LOGGER = logging.getLogger(__name__)


class DatasetRecord(object):
    """DatasetRecord database interface class."""

    DATASET_METADATA_FIELDS = ['dataset_path',
                               'datetime_processed',
                               'dataset_size',
                               'll_x',
                               'll_y',
                               'lr_x',
                               'lr_y',
                               'ul_x',
                               'ul_y',
                               'ur_x',
                               'ur_y',
                               'x_pixels',
                               'y_pixels',
                               'xml_text'
                               ]

    def __init__(self, collection, acquisition, dataset):

        self.collection = collection
        self.datacube = collection.datacube
        self.db = IngestDBWrapper(self.datacube.db_connection)

        dataset_key = collection.get_dataset_key(dataset)
        self.dataset_bands = collection.new_bands[dataset_key]

        self.dataset = dataset
        self.mdd = dataset.metadata_dict

        self.dataset_dict = {}
        for field in self.DATASET_METADATA_FIELDS:
            self.dataset_dict[field] = self.mdd[field]

        self.dataset_dict['acquisition_id'] = acquisition.acquisition_id
        self.dataset_dict['crs'] = self.mdd['projection']
        self.dataset_dict['level_name'] = self.mdd['processing_level']
        self.dataset_dict['level_id'] = \
            self.db.get_level_id(self.dataset_dict['level_name'])

        self.dataset_dict['dataset_id'] = \
            self.db.get_dataset_id(self.dataset_dict)
        if self.dataset_dict['dataset_id'] is None:
            # create a new dataset record in the database
            self.dataset_dict['dataset_id'] = \
                self.db.insert_dataset_record(self.dataset_dict)
            self.needs_update = False
        else:
            # check that the old dataset record can be updated
            self.__check_update_ok()
            self.needs_update = True

        self.dataset_id = self.dataset_dict['dataset_id']

    def remove_mosaics(self, dataset_filter):
        """Remove mosaics associated with the dataset.

        This will mark mosaic files for removal, delete mosaic database
        records if they exist, and update the tile class of overlapping
        tiles (from other datasets) to reflect the lack of a mosaic. The
        'dataset_filter' is a list of dataset_ids to filter on. It should
        be the list of dataset_ids that have been locked (including this
        dataset). It is used to avoid operating on the tiles of an
        unlocked dataset.
        """

        # remove new mosaics (those with database records)
        overlap_dict = self.db.get_overlapping_tiles_for_dataset(
            self.dataset_id,
            input_tile_class_filter=(TC_SINGLE_SCENE,
                                     TC_SUPERSEDED,
                                     TC_MOSAIC),
            output_tile_class_filter=(TC_MOSAIC,),
            dataset_filter=dataset_filter
            )

        for tile_record_list in overlap_dict.values():
            for tr in tile_record_list:
                self.db.remove_tile_record(tr['tile_id'])
                self.collection.mark_tile_for_removal(tr['tile_pathname'])

        # build a dictionary of overlaps (ignoring mosaics)
        overlap_dict = self.db.get_overlapping_tiles_for_dataset(
            self.dataset_id,
            input_tile_class_filter=(TC_SINGLE_SCENE,
                                     TC_SUPERSEDED),
            output_tile_class_filter=(TC_SINGLE_SCENE,
                                      TC_SUPERSEDED),
            dataset_filter=dataset_filter
            )

        # update tile classes for overlap tiles from other datasets
        for tile_record_list in overlap_dict.values():
            if len(tile_record_list) > 2:
                raise DatasetError("Attempt to update a mosaic of three or " +
                                   "more datasets. Handling for this case " +
                                   "is not yet implemented.")
            for tr in tile_record_list:
                if tr['dataset_id'] != self.dataset_id:
                    self.db.update_tile_class(tr['tile_id'], TC_SINGLE_SCENE)

        # remove old mosaics (those without database records)
        for tile_record_list in overlap_dict.values():
            if len(tile_record_list) > 1:
                # tile_record_list is sorted by acquisition start time, so
                # the first record should be the one the mosaic filename is
                # based on.
                tr = tile_record_list[0]
                mosaic_pathname = \
                    self.__make_mosaic_pathname(tr['tile_pathname'])
                if os.path.isfile(mosaic_pathname):
                    self.collection.mark_tile_for_removal(mosaic_pathname)

    def remove_tiles(self):
        """Remove the tiles associated with the dataset.

        This will remove ALL the tiles belonging to this dataset, deleting
        database records and marking tile files for removal on commit. Mosaics
        should be removed BEFORE calling this (as it will delete the tiles
        needed to figure out the overlaps, but may not delete all the mosaics).
        """

        tile_list = self.db.get_dataset_tile_ids(self.dataset_id)

        for tile_id in tile_list:
            tile_pathname = self.db.get_tile_pathname(tile_id)
            self.db.remove_tile_record(tile_id)
            self.collection.mark_tile_for_removal(tile_pathname)

    def update(self):
        """Update the dataset record in the database.

        This first checks that the new dataset is more recent than
        the record in the database. If not it raises a dataset error.
        """

        self.__check_update_ok()
        self.db.update_dataset_record(self.dataset_dict)

    def make_tiles(self, tile_type_id, band_stack):
        """Tile the dataset, returning a list of tile_content objects.

        :rtype list of TileContents
        """

        tile_list = []
        tile_footprint_list = sorted(self.get_coverage(tile_type_id))
        LOGGER.info('%d tile footprints cover dataset', len(tile_footprint_list))

        for tile_footprint in tile_footprint_list:
            tile_contents = self.collection.create_tile_contents(
                tile_type_id,
                tile_footprint,
                band_stack
                )
            tile_contents.reproject()

            if tile_contents.has_data():
                tile_list.append(tile_contents)
            else:
                tile_contents.remove()

        LOGGER.info('%d non-empty tiles created', len(tile_list))
        return tile_list

    def store_tiles(self, tile_list):
        """Store tiles in the database and file store.

        'tile_list' is a list of tile_contents objects. This
        method will create the corresponding database records and
        mark tiles for creation when the transaction commits.

        :type tile_list: list of TileContents
        """
        return [self.create_tile_record(tile_contents) for tile_contents in tile_list]

    def create_mosaics(self, dataset_filter):
        """Create mosaics associated with the dataset.

        'dataset_filter' is a list of dataset_ids to filter on. It should
        be the list of dataset_ids that have been locked (including this
        dataset). It is used to avoid operating on the tiles of an
        unlocked dataset.
        """

        # Build a dictionary of overlaps (ignoring mosaics, including pending).
        overlap_dict = self.db.get_overlapping_tiles_for_dataset(
            self.dataset_id,
            input_tile_class_filter=(TC_PENDING,
                                     TC_SINGLE_SCENE,
                                     TC_SUPERSEDED),
            output_tile_class_filter=(TC_PENDING,
                                      TC_SINGLE_SCENE,
                                      TC_SUPERSEDED),
            dataset_filter=dataset_filter
            )

        # Make mosaics and update tile classes as needed.
        for tile_record_list in overlap_dict.values():
            if len(tile_record_list) > 2:
                raise DatasetError("Attempt to create a mosaic of three or " +
                                   "more datasets. Handling for this case " +
                                   "is not yet implemented.")
            elif len(tile_record_list) == 2:
                self.__make_one_mosaic(tile_record_list)
                for tr in tile_record_list:
                    self.db.update_tile_class(tr['tile_id'], TC_SUPERSEDED)
            else:
                for tr in tile_record_list:
                    self.db.update_tile_class(tr['tile_id'], TC_SINGLE_SCENE)

    def get_removal_overlaps(self):
        """Returns a list of overlapping dataset ids for mosaic removal."""

        tile_class_filter = (TC_SINGLE_SCENE,
                             TC_SUPERSEDED,
                             TC_MOSAIC)
        return self.get_overlaps(tile_class_filter)

    def get_creation_overlaps(self):
        """Returns a list of overlapping dataset_ids for mosaic creation."""

        tile_class_filter = (TC_PENDING,
                             TC_SINGLE_SCENE,
                             TC_SUPERSEDED)
        return self.get_overlaps(tile_class_filter)

    def get_overlaps(self, tile_class_filter):
        """Returns a list of overlapping dataset ids, including this dataset.

        A dataset is overlapping if it contains tiles that overlap with
        tiles belonging to this dataset. Only tiles in the tile_class_filter
        are considered.
        """

        dataset_list = self.db.get_overlapping_dataset_ids(
            self.dataset_id,
            tile_class_filter=tile_class_filter
            )

        if not dataset_list:
            dataset_list = [self.dataset_id]

        return dataset_list

    def create_tile_record(self, tile_contents):
        """Factory method to create an instance of the TileRecord class.

        The created object will be responsible for inserting tile table records
        into the database for reprojected or mosaiced tiles."""
        self.collection.mark_tile_for_creation(tile_contents)
        tile = TileRecord(
            self.dataset_id,
            tile_footprint=tile_contents.tile_footprint,
            tile_type_id=tile_contents.tile_type_id,
            path=tile_contents.get_output_path(),
            size_mb=tile_contents.get_output_size_mb(),
            tile_extents=tile_contents.tile_extents
        )
        TileRepository(self.collection).persist_tile(tile)
        return tile

    def mark_as_tiled(self):
        """Flag the dataset record as tiled in the database.

        This flag does not exist in the current database schema,
        so this method does nothing at the moment."""

        pass

    def list_tile_types(self):
        """Returns a list of the tile type ids for this dataset."""

        return self.dataset_bands.keys()

    def get_tile_bands(self, tile_type_id):
        """Returns a dictionary containing the band info for one tile type.

        The tile_type_id must valid for this dataset, available from
        list_tile_types above.
        """

        return self.dataset_bands[tile_type_id]

    def get_coverage(self, tile_type_id):
        """Given the coordinate reference system of the dataset and that of the
        tile_type_id, return a list of tiles within the dataset footprint"""
        tile_type_info = self.collection.datacube.tile_type_dict[tile_type_id]
        #Get geospatial information from the dataset.
        dataset_crs = self.mdd['projection']
        dataset_geotransform = self.mdd['geo_transform']
        pixels = self.mdd['x_pixels']
        lines = self.mdd['y_pixels']
        #Look up the datacube's projection information for this tile_type
        tile_crs = tile_type_info['crs']
        #Get the transformation between the two projections
        transformation = self.define_transformation(dataset_crs, tile_crs)
        #Determine the bounding quadrilateral of the dataset extent
        #in tile coordinates
        dataset_bbox = self.get_bbox(transformation, dataset_geotransform,
                                     pixels, lines)
        #Determine maximum inner rectangle, which is guaranteed to need  tiling
        #and the minimum outer rectangle outside which no tiles will exist.
        cube_origin = (tile_type_info['x_origin'], tile_type_info['y_origin'])
        cube_tile_size = (tile_type_info['x_size'], tile_type_info['y_size'])
        coverage = self.get_touched_tiles(dataset_bbox,
                                          cube_origin, cube_tile_size)
        return coverage

    #
    # worker methods
    #

    def __check_update_ok(self):
        """Checks if an update is possible, raises a DatasetError otherwise.

        Note that dataset_older_than_database returns a tuple
        (disk_datetime_processed, database_datetime_processed, tile_ingested_datetime)
        if no ingestion required"""

        tile_class_filter = (TC_SINGLE_SCENE,
                             TC_SUPERSEDED)
        time_tuple = self.db.dataset_older_than_database(
                self.dataset_dict['dataset_id'],
                self.dataset_dict['datetime_processed'],
                tile_class_filter)

        if time_tuple is not None:
            disk_datetime_processed, database_datetime_processed, tile_ingested_datetime = time_tuple
            if (disk_datetime_processed == database_datetime_processed):
                skip_message = 'Dataset has already been ingested'
            elif disk_datetime_processed < database_datetime_processed:
                    skip_message = 'Dataset on disk is older than dataset in DB'
            else:
                skip_message = 'Dataset on disk was created after currently ingested contents'

            skip_message += ' (Disk = %s, DB = %s, Ingested = %s)' % time_tuple

            raise DatasetSkipError(skip_message)

    def __make_one_mosaic(self, tile_record_list):
        """Create a single mosaic.

        This create the mosaic contents, creates the database record,
        and marks the mosaic contents for creation on transaction commit.
        """
        mosaic = MosaicContents(
            tile_record_list,
            self.datacube.tile_type_dict,
            self.dataset_dict['level_name'],
            self.collection.get_temp_tile_directory()
            )
        mosaic.create_record(self.db)
        self.collection.mark_tile_for_creation(mosaic)

    def __make_mosaic_pathname(self, tile_pathname):
        """Return the pathname of the mosaic corresponding to a tile."""

        (tile_dir, tile_basename) = os.path.split(tile_pathname)

        mosaic_dir = os.path.join(tile_dir, 'mosaic_cache')
        if self.dataset_dict['level_name'] == 'PQA':
            mosaic_basename = tile_basename
        else:
            mosaic_basename = re.sub(r'\.\w+$', '.vrt', tile_basename)

        return os.path.join(mosaic_dir, mosaic_basename)

#
# Worker methods for coverage.
#
# These are public so that they can be called by test_dataset_record.
#

    def define_transformation(self, dataset_crs, tile_crs):
        """Return the transformation between dataset_crs
        and tile_crs projections"""
        osr.UseExceptions()
        try:
            dataset_spatial_reference = self.create_spatial_ref(dataset_crs)
            tile_spatial_reference = self.create_spatial_ref(tile_crs)
            if dataset_spatial_reference is None:
                raise DatasetError('Unknown projecton %s'
                                   % str(dataset_crs))
            if tile_spatial_reference is None:
                raise DatasetError('Unknown projecton %s'
                                   % str(tile_crs))
            return osr.CoordinateTransformation(dataset_spatial_reference,
                                                tile_spatial_reference)
        except Exception:
            raise DatasetError('Coordinate transformation error ' +
                               'for transforming %s to %s' %
                               (str(dataset_crs), str(tile_crs)))

    @staticmethod
    def create_spatial_ref(crs):
        """Create a spatial reference system for projecton crs.
        Called by define_transformation()"""
        # pylint: disable=broad-except

        osr.UseExceptions()
        try:
            spatial_ref = osr.SpatialReference()
        except Exception:
            raise DatasetError('No spatial reference done for %s' % str(crs))
        try:
            spatial_ref.ImportFromWkt(crs)
            return spatial_ref
        except Exception:
            pass
        try:
            matchobj = re.match(r'EPSG:(\d+)', crs)
            epsg_code = int(matchobj.group(1))
            spatial_ref.ImportFromEPSG(epsg_code)
            return spatial_ref
        except Exception:
            return None

    @staticmethod
    def get_bbox(transform, geotrans, pixels, lines):
        """Return the coordinates of the dataset footprint in clockwise order
        from upper-left"""
        xul, yul, dummy_z =  \
            transform.TransformPoint(geotrans[0], geotrans[3], 0)
        xur, yur, dummy_z = \
            transform.TransformPoint(geotrans[0] + geotrans[1] * pixels,
                                     geotrans[3] + geotrans[4] * pixels, 0)
        xll, yll, dummy_z = \
            transform.TransformPoint(geotrans[0] + geotrans[2] * lines,
                                     geotrans[3] + geotrans[5] * lines, 0)
        xlr, ylr, dummy_z = \
            transform.TransformPoint(
                geotrans[0] + geotrans[1] * pixels + geotrans[2] * lines,
                geotrans[3] + geotrans[4] * pixels + geotrans[5] * lines, 0)
        return [(xul, yul), (xur, yur), (xlr, ylr), (xll, yll)]

    def get_touched_tiles(self, dataset_bbox, cube_origin, cube_tile_size):
        """Return a list of tuples (itile, jtile) comprising all tiles
        footprints that intersect the dataset bounding box"""
        definite_tiles, possible_tiles = \
            self.get_definite_and_possible_tiles(dataset_bbox,
                                                 cube_origin, cube_tile_size)
        coverage_set = definite_tiles
        #Check possible tiles:
        #Check if the tile perimeter intersects the dataset bbox perimeter:
        intersected_tiles = \
            self.get_intersected_tiles(possible_tiles, dataset_bbox,
                                       cube_origin, cube_tile_size)
        coverage_set = coverage_set.union(intersected_tiles)
        possible_tiles = possible_tiles.difference(intersected_tiles)
        #Otherwise the tile might be wholly contained in the dataset bbox
        contained_tiles = \
            self.get_contained_tiles(possible_tiles, dataset_bbox,
                                     cube_origin, cube_tile_size)
        coverage_set = coverage_set.union(contained_tiles)
        return coverage_set

    @staticmethod
    def get_definite_and_possible_tiles(bbox, cube_origin, cube_tile_size):
        """Return two lists of tile footprints: from the largest rectangle
        wholly contained within the dataset bbox and the smallest rectangle
        containing the bbox."""
        #pylint: disable=too-many-locals
        #unpack the bbox vertices in clockwise order from upper-left
        xyul, xyur, xylr, xyll = bbox
        xul, yul = xyul
        xur, yur = xyur
        xlr, ylr = xylr
        xll, yll = xyll
        #unpack the origin of the tiled datacube (e.g. lat=0, lon=0) and the
        #datacube tile size
        xorigin, yorigin = cube_origin
        xsize, ysize = cube_tile_size
        #Define the largest rectangle wholly contained within footprint
        xmin = max(xll, xul)
        xmax = min(xlr, xur)
        ymin = max(yll, ylr)
        ymax = min(yul, yur)
        xmin_index = int(floor((xmin - xorigin) / xsize))
        xmax_index = int(floor((xmax - xorigin) / xsize))
        ymin_index = int(floor((ymin - yorigin) / ysize))
        ymax_index = int(floor((ymax - yorigin) / ysize))
        definite_tiles = set([(itile, jtile)
                              for itile in range(xmin_index, xmax_index + 1)
                              for jtile in range(ymin_index, ymax_index + 1)])
        #Define the smallest rectangle which is guaranteed to include all tiles
        #in the foorprint.
        xmin = min(xll, xul)
        xmax = max(xlr, xur)
        ymin = min(yll, ylr)
        ymax = max(yul, yur)
        xmin_index = int(floor((xmin - xorigin) / xsize))
        xmax_index = int(floor((xmax - xorigin) / xsize))
        ymin_index = int(floor((ymin - yorigin) / ysize))
        ymax_index = int(floor((ymax - yorigin) / ysize))
        possible_tiles = set([(itile, jtile)
                              for itile in range(xmin_index, xmax_index + 1)
                              for jtile in range(ymin_index, ymax_index + 1)
                              ]).difference(definite_tiles)
        return (definite_tiles, possible_tiles)

    def get_intersected_tiles(self, candidate_tiles, dset_bbox,
                              cube_origin, cube_tile_size):
        """Return the subset of candidate_tiles that have an intersection with
        the dataset bounding box"""
        #pylint: disable=too-many-locals
        xorigin, yorigin = cube_origin
        xsize, ysize = cube_tile_size
        keep_list = []
        for itile, jtile in candidate_tiles:
            intersection_exists = False
            (x0, y0) = (xorigin + itile * xsize,
                        yorigin + (jtile + 1) * ysize)
            tile_bbox = [(x0, y0), (x0 + xsize, y0),
                         (x0 + xsize, y0 - ysize), (x0, y0 - ysize)]
            tile_vtx_number = len(tile_bbox)
            dset_vtx_number = len(dset_bbox)
            for tile_vtx in range(tile_vtx_number):
                x1, y1 = tile_bbox[tile_vtx]
                x2, y2 = tile_bbox[(tile_vtx + 1) % tile_vtx_number]
                for dset_vtx in range(dset_vtx_number):
                    x3, y3 = dset_bbox[dset_vtx]
                    x4, y4 = dset_bbox[(dset_vtx + 1) % dset_vtx_number]
                    xcoords = [x1, x2, x3, x4]
                    ycoords = [y1, y2, y3, y4]
                    intersection_exists = \
                        self.check_intersection(xcoords, ycoords)
                    if intersection_exists:
                        keep_list.append((itile, jtile))
                        break
                if intersection_exists:
                    break
        return set(keep_list)

    @staticmethod
    def get_contained_tiles(candidate_tiles, dset_bbox,
                            cube_origin, cube_tile_size):
        """Return the subset of candidate tiles that lie wholly within the
        dataset bounding box"""
        #pylint: disable=too-many-locals
        xorigin, yorigin = cube_origin
        xsize, ysize = cube_tile_size
        keep_list = []
        for itile, jtile in candidate_tiles:
            tile_vtx_inside = []
            (x0, y0) = (xorigin + itile * xsize,
                        yorigin + (jtile + 1) * ysize)
            tile_bbox = [(x0, y0), (x0 + xsize, y0),
                         (x0 + xsize, y0 - ysize), (x0, y0 - ysize)]
            dset_vtx_number = len(dset_bbox)
            for x, y in tile_bbox:
                #Check if this vertex lies within the dataset bounding box:
                winding_number = 0
                for dset_vtx in range(dset_vtx_number):
                    x1, y1 = dset_bbox[dset_vtx]
                    x2, y2 = dset_bbox[(dset_vtx + 1) % dset_vtx_number]
                    if y >= y1 and y < y2:
                        if (x - x1) * (y2 - y1) > (x2 - x1) * (y - y1):
                            winding_number += 1
                    elif y <= y1 and y > y2:
                        if (x - x1) * (y2 - y1) < (x2 - x1) * (y - y1):
                            winding_number += 1
                tile_vtx_inside.append(winding_number % 2 == 1)
            if tile_vtx_inside.count(True) == len(tile_bbox):
                keep_list.append((itile, jtile))
            assert tile_vtx_inside.count(True) == 4 or \
                tile_vtx_inside.count(True) == 0, \
                "Tile partially inside dataset bounding box but has" \
                "no intersection"
        return set(keep_list)

    @staticmethod
    def check_intersection(xpts, ypts):
        """Determines if the line segments
        (xpts[0], ypts[0]) to (xpts[1], ypts[1]) and
        (xpts[2], ypts[2]) to (xpts[3], ypts[3]) intersect"""
        pvec = (xpts[0], ypts[0])
        qvec = (xpts[2], ypts[2])
        rvec = (xpts[1] - xpts[0], ypts[1] - ypts[0])
        svec = (xpts[3] - xpts[2], ypts[3] - ypts[2])
        rvec_cross_svec = rvec[0] * svec[1] - rvec[1] * svec[0]
        if rvec_cross_svec == 0:
            return False
        qminusp_cross_svec = \
            (qvec[0] - pvec[0]) * svec[1] - (qvec[1] - pvec[1]) * svec[0]
        qminusp_cross_rvec = \
            (qvec[0] - pvec[0]) * rvec[1] - (qvec[1] - pvec[1]) * rvec[0]

        tparameter = qminusp_cross_svec / rvec_cross_svec
        uparameter = qminusp_cross_rvec / rvec_cross_svec

        return (0 < tparameter < 1) and (0 < uparameter < 1)

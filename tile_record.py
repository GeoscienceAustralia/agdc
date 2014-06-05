"""
TileRecord: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging
import os
from ingest_db_wrapper import IngestDBWrapper
import cube_util
from cube_util import DatasetError

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class TileRecord(object):
    # pylint: disable=too-many-instance-attributes
    """TileRecord database interface class."""

    TILE_METADATA_FIELDS = ['tile_id',
                            'x_index',
                            'y_index',
                            'tile_type_id',
                            'dataset_id',
                            'tile_pathname',
                            'tile_class_id',
                            'tile_size',
                            'ctime'
                            ]
    def __init__(self, collection, dataset_record, tile_contents):
        self.collection = collection
        self.datacube = collection.datacube
        self.dataset_record = dataset_record
        self.tile_contents = tile_contents
        self.tile_footprint = tile_contents.tile_footprint
        self.tile_type_id = tile_contents.tile_type_id
        #Set tile_class_id to 1 indicating non-mosaiced tile
        self.tile_class_id = 1
        #Set tile_id, determined below from database query
        self.tile_id = None
        self.db = IngestDBWrapper(self.datacube.db_connection)
        # Fill a dictionary with data for the tile
        tile_dict = {}
        self.tile_dict = tile_dict
        tile_dict['x_index'] = self.tile_footprint[0]
        tile_dict['y_index'] = self.tile_footprint[1]
        tile_dict['tile_type_id'] = self.tile_type_id
        tile_dict['dataset_id'] = self.dataset_record.dataset_id
        # Store final destination in the 'tile_pathname' field
        tile_dict['tile_pathname'] = self.tile_contents.tile_output_path
        tile_dict['tile_class_id'] = 1
        # The physical file is currently in the temporary location
        tile_dict['tile_size'] = \
            cube_util.get_file_size_mb(self.tile_contents
                                       .temp_tile_output_path)
        # Update the tile_footprint entry on the datbase:
        if not self.db.tile_footprint_exists(tile_dict):
            footprint_dict = {'x_index': self.tile_footprint[0],
                              'y_index': self.tile_footprint[1],
                              'tile_type_id': self.tile_type_id,
                              'x_min': tile_contents.tile_extents[0],
                              'y_min': tile_contents.tile_extents[1],
                              'x_max': tile_contents.tile_extents[2],
                              'y_max': tile_contents.tile_extents[3],
                              'bbox': 'Populate this within sql query?'}
            self.db.insert_tile_footprint(footprint_dict)
        # Make the tile record entry on the database:
        self.tile_id = self.db.get_tile_id(tile_dict)
        if self.tile_id  is None:
            self.tile_id = self.db.insert_tile_record(tile_dict)
        else:
            # If there is any existing tile corresponding to tile_dict then
            # will have been removed by dataset_record.__remove_dataset_tiles()
            # and its associated calls to methods in self.db.
            assert 1 == 2, "Should not be in tiling process"
        tile_dict['tile_id'] = self.tile_id

    def make_mosaics(self):
        """Query the database to determine if this tile_record should be
        mosaiced with tiles from previously ingested datasets."""
        tile_record_tuples = \
            self.db.get_overlapping_tiles(self.tile_dict)

        if len(tile_record_tuples) < 1:
            raise DatasetError("Mosaic process for dataset_id=%d, x_index=%d,"\
                               " y_index=%d did not find any tile records,"\
                               " including current tile_record!"\
                               %(self.dataset_record.dataset_id,
                                 self.tile_dict['x_index'],
                                 self.tile_dict['y_index']))

        if len(tile_record_tuples) == 1:
            return False

        tile_dict_list = []
        for tile_tuple in tile_record_tuples:
            tile_record = list(tile_tuple)
            if tile_record[4] == self.dataset_record.dataset_id:
                #For the constituent tile deriving from the current dataset id,
                #we need to change its location to the value stored in the
                #tile_contents object.
                tile_record[5] = self.tile_contents.temp_tile_output_path
            tile_dict_list.append(dict(zip(self.TILE_METADATA_FIELDS,
                                           tile_record)))
        tile_record_tuples = None

        # Make the temporary and permanent locations of the mosaic
        mosaic_basename = \
                    os.path.basename(tile_dict_list[0]['tile_pathname'])
        mosaic_temp_dir = \
            os.path.join(os.path.dirname(self.tile_contents.
                                         temp_tile_output_path),
                         'mosaic_cache')
        cube_util.create_directory(mosaic_temp_dir)
        mosaic_final_dir = \
            os.path.join(os.path.dirname(self.tile_contents.
                                         tile_output_path),
                        'mosaic_cache')
        cube_util.create_directory(mosaic_final_dir)
        mosaic_temp_pathname = os.path.join(mosaic_temp_dir, mosaic_basename)
        mosaic_final_pathname = os.path.join(mosaic_final_dir, mosaic_basename)
        # Make the physical tile for PQA or a vrt otherwise
        if self.dataset_record.mdd['processing_level'] == 'PQA':
            self.tile_contents.make_pqa_mosaic_tile(tile_dict_list,
                                                    mosaic_temp_pathname)
        else:
            self.tile_contents.make_mosaic_vrt(tile_dict_list,
                                               mosaic_temp_pathname)

        # Update the tile table by changing the tile_class_id on the source
        # tiles and adding a record for the mosaic tile.
        # First change the pathname of tile deriving from this dataset_id back
        # to the permanent location.
        for tile_dict in tile_dict_list:
            tile_dict['tile_class_id'] = 3
            if tile_dict['dataset_id'] == self.dataset_record.dataset_id:
                tile_dict['tile_pathname'] =  \
                    self.tile_contents.tile_output_path

        # Set a dictionary of parameters for the mosaic.
        mosaic_dict = dict(tile_dict_list[0])
        mosaic_dict['tile_pathname'] = mosaic_final_pathname
        mosaic_dict['tile_class_id'] = 4
        mosaic_dict['tile_size'] = \
            cube_util.get_file_size_mb(mosaic_temp_pathname)

        self.db.update_tile_records_post_mosaic(tile_dict_list, mosaic_dict)
        # Set the temp and final locations so that the move may be done during
        # commit of this transaction
        self.tile_contents.mosaic_temp_pathname = mosaic_temp_pathname
        self.tile_contents.mosaic_final_pathname = mosaic_final_pathname
        return True



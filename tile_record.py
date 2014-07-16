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
from ingest_db_wrapper import IngestDBWrapper, TC_PENDING
import cube_util
from cube_util import MosaicError
import re
import psycopg2

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
        #Set tile_class_id to pending.
        self.tile_class_id = TC_PENDING
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

        self.update_tile_footprint()

        # Make the tile record entry on the database:
        self.tile_id = self.db.get_tile_id(tile_dict)
        if self.tile_id  is None:
            self.tile_id = self.db.insert_tile_record(tile_dict)
        else:
            # If there was any existing tile corresponding to tile_dict then
            # it should alreay have been removed.
            raise AssertionError("Attempt to recreate an existing tile.")
        tile_dict['tile_id'] = self.tile_id
        self.mosaicing_tile_info = (None, None, None)

    def update_tile_footprint(self):
        """Update the tile footprint entry in the database"""

        if not self.db.tile_footprint_exists(self.tile_dict):
            # We may need to create a new footprint record.
            footprint_dict = {'x_index': self.tile_footprint[0],
                              'y_index': self.tile_footprint[1],
                              'tile_type_id': self.tile_type_id,
                              'x_min': self.tile_contents.tile_extents[0],
                              'y_min': self.tile_contents.tile_extents[1],
                              'x_max': self.tile_contents.tile_extents[2],
                              'y_max': self.tile_contents.tile_extents[3],
                              'bbox': 'Populate this within sql query?'}

            # Create an independant database connection for this transaction.
            my_db = IngestDBWrapper(self.datacube.create_connection())
            try:
                with self.collection.transaction(my_db):
                    if not my_db.tile_footprint_exists(self.tile_dict):
                        my_db.insert_tile_footprint(footprint_dict)

            except psycopg2.IntegrityError:
                # If we get an IntegrityError we assume the tile_footprint
                # is already in the database, and we do not need to add it.
                pass

            finally:
                my_db.close()

    def make_mosaic_db_changes(self):
        """Communicate to any other dataset transaction via the tile table when
        performing  mosaicking."""

#        print '%f checking my tile class' %time.time()
        if self.db.get_tile_class_id(self.tile_id) == 3:
            # another dataset transaction has marked me as overlap
#            print '%f My tile class is 3' %time.time()
            return
#        print '%f Checking for overlaps on %s' %(time.time(), self.tile_footprint)
        tile_record_tuples = \
            self.db.get_overlapping_tiles(self.tile_dict)
#        print '%f done' %time.time()
        # If  there are no other overlapping tiles apart from this one:
        if len(tile_record_tuples) == 1:
            return
        # Check whether another dataset has marked this tile as overlap while
        # get_overlapping_tiles() was executing:
#        print '%f checking my tile class' %time.time()
        if self.db.get_tile_class_id(self.tile_id) == 3:
            # another dataset transaction has marked me as overlap
            return
#        print self.db.get_tile_class_id(self.tile_id)
        overlaps_with_class_1 = tuple([t[0] for t in tile_record_tuples
                                       if t[6] == 1])
#        print '%f updating overlapping tiles with tile_class_id=3' %time.time()
        self.db.update_tiles("tile_id", ("tile_class_id",),
                             overlaps_with_class_1, (3,))
#        print '%f done' %time.time()
        self.db.commit()
#        print '%f commited' %time.time()
        overlaps_with_class_4 = tuple([t[0] for t in tile_record_tuples
                                       if t[6] == 4])
        if len(overlaps_with_class_4) > 1:
            tile_id_str = \
                "%d, " * len(overlaps_with_class_4) % overlaps_with_class_4
            raise MosaicError("More than one mosaic record: tile_ids = %s" \
                                  %tile_id_str)
        if len(overlaps_with_class_4) == 0:
            # Create the mosaic tile record and return it along with the
            # overlapping tile information,
            mosaic_tile_dict = dict(zip(self.TILE_METADATA_FIELDS,
                                   tile_record_tuples[0]))
            mosaic_tile_dict['tile_class_id'] = 4
            mosaic_tile_dict['tile_pathname'] += '_placeholder_pathname'
            mosaic_tile_id = \
                self.db.insert_tile_record(mosaic_tile_dict)
            self.db.commit()
            delete_on_rollback = True
        else:
            mosaic_tile_id = overlaps_with_class_4[0]
            delete_on_rollback = False
        self.mosaicing_tile_info = \
            (tile_record_tuples, mosaic_tile_id, delete_on_rollback)

    def make_mosaic_tiles(self):
        """Query the database to determine if this tile_record should be
        mosaiced with tiles from previously ingested datasets."""

        # Check the database to see if the overlapping dataset has is already
        # creating the
        tile_dict_list = []
        tile_record_tuples, mosaic_tile_id, dummy_delete_on_rollback = \
            self.mosaicing_tile_info
        for tile_tuple in tile_record_tuples:
            tile_record = list(tile_tuple)
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
            print 'mosaic_temp_pathname'
            print mosaic_temp_pathname
            print cube_util.get_file_size_mb(mosaic_temp_pathname)
        else:
            src_file_extn = self.tile_contents.tile_type_info['file_extension']
            mosaic_temp_pathname = \
                re.sub('%s$' %src_file_extn, '.vrt', mosaic_temp_pathname)
            mosaic_final_pathname = \
                re.sub('%s$' %src_file_extn, '.vrt', mosaic_final_pathname)

            # The vrt should reference the final destination of the source tile
            # from this dataset.
            for tile_dict in tile_dict_list:
                tile_dict['tile_class_id'] = 3
                if tile_dict['dataset_id'] == self.dataset_record.dataset_id:
                    tile_dict['tile_pathname'] =  \
                        self.tile_contents.tile_output_path
            self.tile_contents.make_mosaic_vrt(tile_dict_list,
                                               mosaic_temp_pathname)

        # TODO: delete following since redundant
        # # Update the tile table by changing the tile_class_id on the source
        # # tiles and adding a record for the mosaic tile.
        # # First change the pathname of tile deriving from this dataset_id back
        # # to the permanent location.
        # for tile_dict in tile_dict_list:
        #     tile_dict['tile_class_id'] = 3
        #     # Following can be deleted since we finalize source tiles before
        #     # doing mosaics.
        #     # if tile_dict['dataset_id'] == self.dataset_record.dataset_id:
        #     #     tile_dict['tile_pathname'] =  \
        #     #         self.tile_contents.tile_output_path

        # Set a dictionary of parameters for the mosaic.
        mosaic_dict = dict(tile_dict_list[0])
        mosaic_dict['tile_id'] = mosaic_tile_id
        mosaic_dict['tile_pathname'] = mosaic_final_pathname
        mosaic_dict['tile_class_id'] = 4
        mosaic_dict['tile_size'] = \
            cube_util.get_file_size_mb(mosaic_temp_pathname)

        self.db.update_tile_record(mosaic_dict)
        # Set the temp and final locations so that the move may be done during
        # commit of this transaction
        self.tile_contents.mosaic_temp_pathname = mosaic_temp_pathname
        self.tile_contents.mosaic_final_pathname = mosaic_final_pathname
        return True



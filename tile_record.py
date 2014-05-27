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

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class TileRecord(object):
    """TileRecord database interface class."""
    
    TILE_METADATA_FIELDS = ['tile_id',
                            'x_index',
                            'y_index',
                            'tile_type_id',
                            'dataset_id',
                            'tile_pathname',
                            'tile_class_id',
                            'tile_size'
                            'ctime'
                            ]

    def __init__(self, collection, acquisition_record,
                 dataset_record, tile_contents):
        self.collection = collection
        self.acquisition_record = acquisition_record
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
        tile_dict['x_index'] = self.tile_footprint[0]
        tile_dict['y_index'] = self.tile_footprint[1]
        tile_dict['tile_type_id'] = self.tile_type_id
        tile_dict['dataset_id'] = self.dataset_record.dataset_id
        # Store final destination in the 'tile_pathname' field
        tile_dict['tile_pathname'] = self.tile_contents.tile_output_path
        tile_dict['tile_class_id'] = 1
        # The physical file is currently in the temporary location
        tile_dict['tile_size'] = \
            cube_util.getFileSizeMB(self.tile_contents.temp_tile_output_path)
        #Make the tile record entry on the database:
        self.tile_id = self.db.get_tile_id(self.tile_dict)
        if self.tile_id  is None:
            self.tile_id = self.db.insert_tile_record(self.tile_dict)
        else:
            #If there is any existing tile corresponding to tile_dict then
            #it will have been removed by dataset_record.__remove_dataset_tiles()
            #and its associated calls to methods in self.db.
            assert 1 == 2, "Should not be in tiling process"
        tile_dict['tile_id'] = self.tile_id
        self.tile_dict = tile_dict
        
    def make_mosaics(self):
        """Query the database to determine if this tile_record should be
        mosaiced with tiles from previously ingested datasets."""
        tile_record_list = \
            self.db.get_overlapping_tiles(self.tile_dict)
        if len(tile_record_list) < 1:
            raise DatasetError("Mosaic process for dataset_id=%d, x_index=%d,"\
                               " y_index=%d did not find any tile records,"\
                               " including current tile_record!"\
                               %(self.dataset_record.dataset_id,
                                 self.tile_dict['x_index'],
                                 self.tile_dict['y_index']))

        if len(tile_record_list) == 1:
            return
        tile_dict_list = []
        for tile_record in tile_record_list:
            if tile_record[4] == self.dataset_record.dataset_id:
                #For the constituent tile deriving from the current dataset id,
                #we need to change its location to the value stored in the
                #tile_contents object.
                tile_record[5] = self.tile_contents.temp_tile_output_path
                tile_dict_list.append(dict(zip(TILE_DICT_METADATA_FIELDS,
                                               tile_record)))

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
            self.tile_contents.make_pqa_mosaic_tile(tile_dict_list, mosaic_temp_pathname)
        else:
            self.tile_contents.make_mosaic_vrt(tile_dicy_list, mosaic_temp_pathname)
                               
        self.db.update_tile_records_post_mosaic(tile_dict_list,
                                                mosaic_final_pathname)
        
    #
    #Methods that query and update the database:
    #

    def update_tile_table(self, tile_list, mosaic_pathname):
        """Given a list of tiles that have been mosaiced, update their
        tile_class_id field and create a new record for the mosaiced tile"""
        db_cursor = self.conn.cursor
        x_index, y_index = self.tile_footprint
        #Update the existing tiles tile_class_id
        params = [tile['tile_id'] for tile in tile_list]
        sql = """update tile
            set tile_class_id = 3
            where tile_id in (%s)"""
            % ','.join('%s' for id in params)
        db_cursor.execute(sql, params)
        #Create a new tile record for the mosaiced tile
        #Determine mosaic's final pathname:
        mosaic_dir = \
            os.path.join(os.path.dirname(self.tile_contents.tile_output_path),
                         'mosaic_cache')
        mosaic_basename = os.path.basename(mosaic_pathname)
        params = {'x_index': x_index,
                  'y_index': y_index,
                  'tile_type_id': tile_type_id,
                  'dataset_id': self.dataset_record.dataset_id
                  'tile_pathname': os.path.join(mosaic_dir, mosaic_basename)
                  'tile_class_id': 4
                  'tile_size': cube_util.getFileSizeMB(mosaic_pathname)
                  }

        sql = """ insert into tile(
            tile_id,
            x_index,
            y_index,
            tile_type_id,
            dataset_id,
            tile_pathname,
            tile_class_id,
            tile_size,
            ctime
            )
            select
            nextval('tile_id_seq::regclass),
            %(x_index)s,
            %(y_index)s,
            %(tile_type_id)s,
            %(dataset_id)s,
            %(tile_pathname)s,
            %(tile_class_id)s,
            %(tile_size)s,
            %now()
            where not exists
                (select tile_id
                 from tile
                 where 
                 x_index = %(x_index)s and
                 y_index = %(y_index)s and
                 tile_type_id = %(tile_type_id)s and
                 dataset_id = %(dataset_id)s
                 );"""

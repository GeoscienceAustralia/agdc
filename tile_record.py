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
    
    TILE_METADATA_FIELDS = ['x_index',
                            'y_index',
                            'tile_type_id',
                            'dataset_id',
                            'tile_pathname',
                            'tile_class_id',
                            'tile_size'
                            'ctime'
                            ]

    def __init__(self, collection, acquisition_record,
                 dataset_record, tile_type_id, tile_footprint, tile_contents):
        self.collection = collection
        self.acquisition_record = acquisition_record
        self.dataset_record = dataset_record
        self.tile_footprint = tile_footprint
        self.tile_contents = tile_contents
        self.db = IngestDBWrapper(self.datacube.db_connection)
        # Fill a dictionary with data for the tile
        tile_dict['x_index'] = tile_footprint[0]
        tile_dict['y_index'] = tile_footprint[1]
        tile_dict['tile_type_id'] = tile_type_id
        tile_dict['dataset_id'] = dataset_record.dataset_id
        tile_dict['tile_pathname'] = self.tile_contents.tile_output_path
        tile_dict['tile_class_id'] = tile_class_id
        tile_dict['tile_size'] = cube_util
        

    def make_mosaics(self):
        tile_record_list = \
            self.db.get_overlappiong_tiles(self.tile_contents.tile_type_id,
                                           self.tile_contents.tile_footprint,
                                           self.dataset_record.dataset_id)
        for tile_record in tile_record_list:
            if tile_record[4] == self.dataset_record.dataset_id:
                #For the constituent tile deriving from the curent dataset id,
                #will need to change its location in tile_list from the value
                #stored in the database to the value stored in the
                #tile_contents object.
                tile_basename = os.path.basename(record[5])
                tile_dir = os.path.dirname(self.tile_contents.
                                           temp_tile_output_path)
                record[5] = os.path.join(tile_dir, tile_basename)
                tile_record_list.append(dict(zip(tile_table_column_names, record)))
        assert len(tile_list) >= 1, \
            "Mosaic process did not find any tile records"
        assert len(tile_list) > 2, \
            "Mosaic process found more than 2 tile records"

        if len(tile_list) == 1:
            return
        mosaic_basename = os.path.basename(tile_list[0]['tile_pathname'])
        mosaic_dir = \
            os.path.join(os.path.dirname(self.tile_contents.
                                         temp_tile_output_path),
                         'mosaic_cache')
        if not os.path.isdir(mosaic_dir):
            mkdir_cmd = ["mkdir -p", "%s" % mosaic_dir]
            result = cube_util.execute(mkdir_cmd)
        mosaic_pathname = os.path.join(mosaic_dir, mosaic_basename)
        if self.dataset_record.mdd['processing_level'] == 'PQA':
            self.tile_contents.make_pqa_mosaic_tile(tile_list, mosaic_pathname)
        else:
            self.tile_contents.make_mosaic_vrt(tile_list, mosaic_pathname)
        self.update_tile_table(tile_list, mosaic_pathname):
        
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

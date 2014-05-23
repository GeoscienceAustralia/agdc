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

PQA_NO_DATA_VALUE = 16127 # All ones except for contiguity (bit 8)

class TileRecord(object):
    """TileRecord database interface class."""

    def __init__(self, collection, acquisition_record,
                 dataset_record, tile_type_id, tile_footprint, tile_contents):
        self.collection = collection
        self.acquisition_record = acquisition_record
        self.dataset_record = dataset_record
        self.tile_footprint = tile_footprint
        self.tile_contents = tile_contents
        self.conn = self.collection.datacube.db_connection

    def make_mosaics(self):
        tile_list = \
            self.get_overlappiong_tiles(self.tile_type_id, self.tile_footprint,
                                        self.dataset_record.dataset_id)
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
        if self.dataset_record['processing_level'] == 'PQA':
            self.make_pqa_mosaic_tile(tile_list, mosaic_pathname)
        else:
            self.make_mosaic_vrt(tile_list, mosaic_pathname)
        self.update_tile_table(tile_list, mosaic_pathname):
        
    #
    #Methods that query and update the database:
    #
    def get_overlapping_tiles(self, tile_type_id, tile_footprint, dataset_id):
        """Return a dictionary of those tiles within the footprint for which
        there is already data from another existing dataset. Dictionary keys
        are tile_id and values are corresponding rows of the tile table. One of
        the constituent tiles will be from the current dataset_id and will thus
        not yet be fully committed; it will be in its temporary location, to be
        moved to its permanent location once the transaction for this dataset
        is committed."""
        #TODO: see if can use tile_table_column_names in select clause
        #TODO: check assumption that the tiles of this (uncommited)
        #transaction are visible
        x_index, y_index = self.tile_footprint
        tile_table_column_names = [tile_id, x_index, y_index,
                                   tile_type_id, dataset_id, tile_pathname,
                                   tile_class_id, tile_size, ctime]
        db_cursor = self.conn.cursor
        sql = """-- Find all scenes that might contribute data to this tile
            -- TODO: specify exact items
            -- select o.*, od.*, oa.*
            select o.tile_id, o.x_index, o.y_index,
                   o.tile_type_id, o.dataset_id, o.tile_pathname,
                   o.tile_class_id, o.tile_size, o.ctime
            from tile t
            inner join dataset d using(dataset_id)
            inner join acquisition a using(acquisition_id)
            inner join tile o using(x_index, y_index, tile_type_id)
            inner join dataset od on
                od.dataset_id = o.dataset_id and
                od.level_id = d.level_id
            inner join acquisition oa on
                oa.acquisition_id = od.acquisition_id and
                oa.satellite_id = a.satellite_id
            /*
            -- Use tile_id to specify tile
            where t.tile_id = 460497
            */

            -- Use tile spatio-temporal parameters to specify tile
            where
            t.tile_class_id = 1 and
            o.tile_class_id = 1 and
            t.tile_type_id = %(tile_type_id)s and
            t.x_index = %(x_index)s and
            t.y_index = %(y_index)s and
            -- MPH comment out following two conditions:
            -- a.start_datetime = %(start_datetime)s and
            -- d.level_id = %(level_id)s
            -- and replace with: (possible since there is a one-to-one 
            -- (corrspondence between (acquisiton, level_id) and dataset
            d.dataset_id = %(dataset_id)s
            and o.tile_id <> t.tile_id -- Uncomment this to exclude original
            -- TODO check that tile created for current dataset is present
            -- i.e. is it visible if it is part of an uncommited transaction
            -- Use temporal extents to find overlaps
            and (oa.start_datetime between
            a.start_datetime - interval '1 hour' and
            a.end_datetime + interval '1 hour'
            or oa.end_datetime between a.start_datetime - interval '1 hour'  
            and a.end_datetime + interval '1 hour')
            order by oa.start_datetime
        """

        params = {'tile_type_id': tile_type_id,
                  'x_index': x_index,
                  'y_index': y_index,
                  'dataset_id': dataset_id}
        db_cursor.execute(sql, params)
        #initialise list of tiles to be mosaiced
        tile_list = []
        for record in db_cursor:
            #Form a list of dictionaries so that we can keep the datetime order
            if record[4] == self.dataset_record.dataset_id:
                #For the constituent tile deriving from the curent dataset id,
                #will need to change its location in tile_list from the value
                #stored in the database to the value stored in the
                #tile_contents object.
                tile_dir, tile_basename = os.path.split(record[5])
                tile_dir = os.path.dirname(self.tile_contents.bandstack.vrt_name)
                record[5] = os.path.join(tile_dir, tile_basename)
            tile_list.append(dict(zip(tile_table_column_names, record)))
        db_cursor.close()
        return tile_list

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
    #
    #Methods that mosaic several tiles together
    #
    def make_pqa_mosaic_tile(self, tile_list, mosaic_pathname):
        template_dataset = gdal.Open(tile_list[0]['tile_pathname'])
        gdal_driver = gdal.GetDriverByName(self.tile_contents.
                                           tile_type_info['file_format'])
        #Set datatype formats appropriate to Create() and numpy
        gdal_dtype = template_dataset.GetRasterBand(1).DataType
        numpy_dtype = gdal.GetDataTypeName(gdal_dtype)
        mosaic_dataset = gdal_driver.Create(mosaic_pathname,
                                            template_dataset.RasterXSize,
                                            template_dataset.RasterYSize,
                                            1, gdal_dtype,
                                            self.tile_contents.
                                            tile_type_info
                                            ['format_options'].split(','))

        assert mosaic_dataset, \
            'Unable to open output dataset %s'% output_dataset
        
        mosaic_dataset.SetGeoTransform(template_dataset.GetGeoTransform())
        mosaic_dataset.SetProjection(template_dataset.GetProjection())

        # if tile_type_info['file_format'] == 'netCDF':
        #     pass
        #TODO: make vrt here - not really needed for single-layer file

        output_band = mosaic_dataset.GetRasterBand(1)
        data_array=numpy.zeros(shape=(mosaic_dataset.RasterYSize,
                                      mosaic_dataset.RasterXSize),
                               dtype=numpy_dtype)
        data_array[...] = -1 # Set all background values to FFFF
    
        overall_data_mask = numpy.zeros(shape=(mosaic_dataset.RasterYSize, 
                                               mosaic_dataset.RasterXSize),
                                        dtype=numpy.bool)
        del template_dataset

        # Populate data_array with -masked PQA data
        for pqa_dataset_index in range(len(tile_list)):
            pqa_dataset_path = tile_list[pqa_dataset_index]
            pqa_dataset = gdal.Open(pqa_dataset_path)
            assert pqa_dataset, 'Unable to open %s' % pqa_dataset_path
            pqa_array = pqa_dataset.ReadAsArray()
            del pqa_dataset
            # Set all data-containing pixels to true in data_mask
            pqa_data_mask = (pqa_array != PQA_NO_DATA_VALUE) & \
                            (pqa_array != 0)
            # Update overall_data_mask to true for all valid-data pixels
            overall_data_mask = overall_data_mask | pqa_data_mask
            # Set bits which are true in all source arrays
            data_array[pqa_data_mask] = \
                        numpy.bitwise_and(data_array[pqa_data_mask],
                                          pqa_array[pqa_data_mask])
            # Set all pixels which don't contain data to PQA_NO_DATA_VALUE
        data_array[~overall_data_mask] = PQA_NO_DATA_VALUE
        output_band.WriteArray(data_array)  
        mosaic_dataset.FlushCache()
    
    def make_mosaic_vrt(self, tile_list, mosaic_pathname):
        """Form two or more source tile's create a vrt"""
        gdalbuildvrt_cmd = ["gdalbuildvrt -q",
                            "-overwrite",
                            "%s" %mosaic_pathname,
                            "%s" %(" ".join([t['tile_pathname']
                                             for t in tile_list]))]
        result = cube_util.execute(gdalbuildvrt_cmd)

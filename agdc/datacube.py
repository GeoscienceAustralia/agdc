#!/usr/bin/env python

#===============================================================================
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
#===============================================================================

'''
Prototype implementation of base class Datacube with DB connectivity and helper 
functions.

Created on 05/10/2012

@author: Alex Ip
'''
from __future__ import absolute_import
import os
import sys
import argparse
import ConfigParser
import logging
import errno
import psycopg2
import socket

from eotools.utils import log_multiline

logger = logging.getLogger(__name__)

class DataCube(object):
    SECTION_NAME = 'datacube'
    LOCK_WAIT = 10 # Seconds to sleep between checking for file unlock
    MAX_RETRIES = 30 # Maximum number of checks for file unlock
    MAX_BLOCK_SIZE = 536870912 # Maximum blocksize for array operations (0.5GB)

    def create_directory(self, dirname):
        try:
            os.makedirs(dirname)
            logger.info('Created directory %s', dirname)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            
    def remove(self, filename):
        """ Function to remove a file but do not error if it doesn't exist"""
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
    
    def remove_files(self, file_list):
        for filename in file_list:
            self.remove(filename)

    def parse_args(self):
        """Virtual function to parse command line arguments.
    
        Returns:
            argparse namespace object
        """
        logger.debug('  Calling parse_args()')
    
        _arg_parser = argparse.ArgumentParser('datacube')
        
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(self.agdc_root, 'agdc_default.conf'),
            help='DataCube configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
    
        args, unknown_args = _arg_parser.parse_known_args()
        return args
    
    def create_connection(self, autocommit=True):
        db_connection = psycopg2.connect(host=self.host, 
                                              port=self.port, 
                                              dbname=self.dbname, 
                                              user=self.user, 
                                              password=self.password)
        if autocommit:
            db_connection.autocommit = True
            db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        if self.schemas:
            db_connection.cursor().execute(
                "set search_path = {schemas}".format(schemas=self.schemas)
            )

        return db_connection
    
    def get_intersecting_tiles(self, geometry_wkt, geometry_srid=4326):
        """
        Function to return all tile_footprint indexes that intersect the specified geometry.
        Arguments: 
            geometry_wkt - A Well Known Text geometry specification
            geometry_srid - The spatial reference system ID (EPSG code) that geometry_wkt uses. Defaults to 4326
        Returns:
            A list of tuples in the form (x_index, y_index, tile_type_id)
            x_index - Integer x-index
            y_index - Integer y-index
            tile_type_id - Integer tile type ID
        """
        db_cursor2 = self.db_connection.cursor()
        
        sql = """-- Find the tile_footprints that intersect geometry_wkt
        select
          x_index,
          y_index,
          tile_type_id
        from
          tile_footprint
        where
          bbox && ST_GeomFromText(%(geometry_wkt)s, %(geometry_srid)s)
        order by
          x_index,
          y_index
        """
        
        params = {'geometry_wkt' : geometry_wkt, 'geometry_srid' : geometry_srid}
        
        log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
        db_cursor2.execute(sql, params)
        
        resultArray = []
        for record in db_cursor2:
            assert record, 'No data found for this tile and temporal range'
            resultArray.append((record[0], record[1], record[2]))
            
        return resultArray
    
    def get_tile_ordinates(self, point_x, point_y, point_date, 
                      processing_level='NBAR', satellite=None, tile_type_id=None):
        """
        Function to return tile path and pixel coordinates.
        Arguments should be self explanatory
        Returns:
            tile_pathname
            (pixel_x, pixel_y): Pixel coordinates from top-left
            
        NB: There is a KNOWN ISSUE with N-S overlaps where the Southernmost tile may contain
        only no-data for the coordinate. This will be fixed when the original mosiac cache data is catalogued 
        in the tile table.
        """
        
        db_cursor2 = self.db_connection.cursor()
            
        sql = """-- Find tile path for specified indices and date
select tile_pathname, 
  round((%(point_x)s - %(point_x)s::integer) * tile_type.x_pixels)::integer as x_ordinate,
  round((1.0 - (%(point_y)s - %(point_y)s::integer)) * tile_type.y_pixels)::integer as y_ordinate -- Offset from Top
from acquisition
  inner join satellite using(satellite_id)
  inner join dataset using(acquisition_id)
  inner join processing_level using(level_id)
  inner join tile using(dataset_id)
  inner join tile_type using(tile_type_id)
where tile_type_id = %(tile_type_id)s
  and tile_class_id = 1 -- Non-empty tiles
  and (%(satellite)s is null or upper(satellite_tag) = upper(%(satellite)s))
  and upper(level_name) = upper(%(processing_level)s)
  and end_datetime > %(point_date)s and end_datetime < (%(point_date)s + 1)
  and x_index = cast((%(point_x)s - x_origin) / x_size as integer) 
  and y_index = cast((%(point_y)s - y_origin) / y_size as integer)
  order by x_ref, y_ref desc limit 1; -- Return Southernmost tile
"""        
        params = {'point_x': point_x, 
                  'point_y': point_y, 
                  'point_date': point_date, 
                  'processing_level': processing_level,
                  'satellite': satellite, 
                  'tile_type_id': tile_type_id
                  }
        
        log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
        
        db_cursor2.execute(sql, params)
        result = db_cursor2.fetchone()
        if result: # Tile exists
            
            return result[0], (result[1], result[2])
        else:
            return None
   
    def __init__(self, config=None):
        self.agdc_root = os.path.dirname(__file__)

        self.db_connection = None

        self.host = None
        self.dbname = None
        self.user = None
        self.password = None

        # Default schemas: can be overridden in config file.
        self.schemas = 'agdc, public, gis, topology'
        
        self.process_id = os.getenv('PBS_O_HOST', socket.gethostname()) + ':' + os.getenv('PBS_JOBID', str(os.getpid()))

        def open_config(config_file):
            assert os.path.exists(config_file), config_file + " does not exist"

            logger.debug('  Opening conf file %s', repr(config_file))
            _config_parser = ConfigParser.SafeConfigParser(allow_no_value=True)
            _config_parser.read(config_file)

            assert _config_parser.has_section(DataCube.SECTION_NAME), 'No %s section defined in conf file' % DataCube.SECTION_NAME

            return _config_parser

        def string_to_boolean(bool_string):
            return bool_string[0].lower() in ['t', '1']
        
        args = self.parse_args()
        
        self.debug = args.debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
            logger.debug('datacube module logging level set to DEBUG')
            
        log_multiline(logger.debug, args.__dict__, 'args.__dict__',  '\t')

        # Default conf file is agdc_default.conf - show absolute pathname in error messages
        config_file = config or os.path.abspath(args.config_file or
                                      os.path.join(self.agdc_root, 'agdc_default.conf'))
        
        config_parser = open_config(config_file)
    
        # Set instance attributes for every value in config file
        for attribute_name in config_parser.options(DataCube.SECTION_NAME):
            attribute_value = config_parser.get(DataCube.SECTION_NAME, attribute_name)
            self.__setattr__(attribute_name, attribute_value)
            
        # Set instance attributes for every value in command line arguments file
        for attribute_name in args.__dict__.keys():
            attribute_value = args.__dict__[attribute_name]
            if attribute_value:
                self.__setattr__(attribute_name, attribute_value)
            
        self.create_directory(self.temp_dir)
        
        self.port = int(self.port)    
        
        self.db_connection = self.create_connection()
        
        
        # Store tile type info in dict stucture
        db_cursor = self.db_connection.cursor()
        sql = """-- Retrieve all tile_type information
select
  tile_type_id,
  tile_type_name,
  crs,
  x_origin,
  y_origin,
  x_size,
  y_size,
  x_pixels,
  y_pixels,
  unit,
  file_format,
  file_extension,
  format_options,
  tile_directory,
  x_size / x_pixels as x_pixel_size,
  y_size / y_pixels as y_pixel_size
from tile_type
""" 
        log_multiline(logger.debug, sql, 'SQL', '\t')
        db_cursor.execute(sql)
        
        self.tile_type_dict = {}
        for record in db_cursor:
            tile_type_info = {
                'tile_type_id': record[0],
                'tile_type_name': record[1],
                'crs': record[2],
                'x_origin': record[3],
                'y_origin': record[4],
                'x_size': record[5],
                'y_size': record[6],
                'x_pixels': record[7],
                'y_pixels': record[8],
                'unit': record[9],
                'file_format': record[10],
                'file_extension': record[11],
                'format_options': record[12],
                'tile_directory': record[13],
                'x_pixel_size': record[14],
                'y_pixel_size': record[15]
                }
            self.tile_type_dict[record[0]] = tile_type_info
                        
        # Store bands in nested dict stucture
        self.bands = {}
        db_cursor = self.db_connection.cursor()
        sql = """-- Retrieve all band information (including derived bands)
select tile_type_id,
  coalesce(satellite_tag, 'DERIVED') as satellite_tag,
  coalesce(sensor_name, level_name) as sensor_name,
  band_id,
  sensor_id,
  band_name,
  band_type_name,
  file_number,
  resolution,
  min_wavelength,
  max_wavelength,
  file_pattern,
  level_name,
  tile_layer,
  band_tag,
  resampling_method,
  nodata_value

from band ba
inner join band_type bt using(band_type_id)
inner join band_source bs using (band_id)
inner join processing_level pl using(level_id)
left join sensor se using(satellite_id, sensor_id)
left join satellite sa using(satellite_id)
order by tile_type_id,satellite_name, sensor_name, level_name, tile_layer
""" 
        log_multiline(logger.debug, sql, 'SQL', '\t')
        db_cursor.execute(sql)
        
        for record in db_cursor:
            # self.bands is keyed by tile_type_id
            band_dict = self.bands.get(record[0], {})
            if not band_dict: # New dict needed              
                self.bands[record[0]] = band_dict 
                
            # sensor_dict is keyed by (satellite_tag, sensor_name)
            sensor_dict = band_dict.get((record[1], record[2]), {})
            if not sensor_dict: # New dict needed
                band_dict[(record[1], record[2])] = sensor_dict 
                
            band_info = {}
            band_info['band_id'] = record[3]
            band_info['band_name'] = record[5]
            band_info['band_type'] = record[6]
            band_info['file_number'] = record[7]
            band_info['resolution'] = record[8]
            band_info['min_wavelength'] = record[9]
            band_info['max_wavelength'] = record[10]
            band_info['file_pattern'] = record[11]
            band_info['level_name'] = record[12]
            band_info['tile_layer'] = record[13]
            band_info['band_tag'] = record[14]
            band_info['resampling_method'] = record[15]
            band_info['nodata_value'] = record[16]
            
            sensor_dict[record[7]] = band_info # file_number - must be unique for a given satellite/sensor or derived level
            
        log_multiline(logger.debug, self.bands, 'self.bands', '\t')    
         
    def __del__(self):
        if self.db_connection:
            self.db_connection.close()        
    
            
    def lock_object(self, lock_object, lock_type_id=1, lock_status_id=None, lock_detail=None):
        # Need separate non-persistent connection for lock mechanism to allow independent transaction commits
        lock_connection = self.create_connection()
        
        lock_cursor = lock_connection.cursor()
        result = None
        sql = """-- Insert lock record if doesn't already exist
insert into lock(
  lock_type_id,
  lock_object,
  lock_owner,
  lock_status_id)
select
  %(lock_type_id)s,
  %(lock_object)s,
  %(lock_owner)s,
  %(lock_status_id)s
where not exists
  (select
    lock_type_id,
    lock_object
  from lock
  where lock_type_id = %(lock_type_id)s
    and lock_object = %(lock_object)s);
    
-- Update lock record if it is not owned or owned by this process
update lock
set lock_owner = %(lock_owner)s,
  lock_status_id = %(lock_status_id)s,
  lock_detail = %(lock_detail)s
  where lock_type_id = %(lock_type_id)s
    and lock_object = %(lock_object)s
    and (lock_owner is null or lock_owner = %(lock_owner)s);
""" 
        params = {'lock_type_id': lock_type_id,
                  'lock_object': lock_object,
                  'lock_owner': self.process_id,
                  'lock_status_id': lock_status_id,
                  'lock_detail': lock_detail
                  }
        
        log_multiline(logger.debug, lock_cursor.mogrify(sql, params), 'SQL', '\t')
        
        # Need to specifically check object lock record for this process and specified status
        try:
            lock_cursor.execute(sql, params)
            result = self.check_object_locked(lock_object=lock_object, 
                                              lock_type_id=lock_type_id, 
                                              lock_status_id=lock_status_id, 
                                              lock_owner=self.process_id,
                                              lock_connection=lock_connection)
        finally:
            lock_connection.close() 
            
        if result:
            logger.debug('Locked object %s', lock_object)
        else:
            logger.debug('Unable to lock object %s', lock_object)
            
        return result
            

        
    def unlock_object(self, lock_object, lock_type_id=1):
        # Need separate non-persistent connection for lock mechanism to allow independent transaction commits
        lock_connection = self.create_connection()
        
        lock_cursor = lock_connection.cursor()
        result = False
        sql = """-- Delete lock object if it is owned by this process
delete from lock     
where lock_type_id = %(lock_type_id)s
  and lock_object = %(lock_object)s
  and lock_owner = %(lock_owner)s;
""" 
        params = {'lock_type_id': lock_type_id,
                  'lock_object': lock_object,
                  'lock_owner': self.process_id
                  }
        
        log_multiline(logger.debug, lock_cursor.mogrify(sql, params), 'SQL', '\t')
        try:
            lock_cursor.execute(sql, params)
            result = not self.check_object_locked(lock_object, 
                                                  lock_type_id)   
        finally:
            lock_connection.close()
            
        if result:
            logger.debug('Unlocked object %s', lock_object)
        else:
            logger.debug('Unable to unlock object %s', lock_object)
            
        return result
    
    def check_object_locked(self, lock_object, lock_type_id=1, lock_status_id=None, lock_owner=None, lock_connection=None):
        # Check whether we need to create a new connection and do it if required
        create_connection = not lock_connection
        # Need separate non-persistent connection for lock mechanism to allow independent transaction commits
        lock_connection = lock_connection or self.create_connection()
        
        lock_cursor = lock_connection.cursor()
        result = None
        sql = """-- Select lock record if it exists
select     
  lock_object,
  lock_owner,
  lock_status_id,
  lock_detail
  from lock
  where lock_type_id = %(lock_type_id)s
    and lock_object = %(lock_object)s
    and (%(lock_status_id)s is null or lock_status_id = %(lock_status_id)s)
    and (%(lock_owner)s is null or lock_owner = %(lock_owner)s);
""" 
        params = {'lock_type_id': lock_type_id,
                  'lock_object': lock_object,
                  'lock_owner': lock_owner,
                  'lock_status_id': lock_status_id
                  }
        
        log_multiline(logger.debug, lock_cursor.mogrify(sql, params), 'SQL', '\t')
        try:
            lock_cursor.execute(sql, params)
            record = lock_cursor.fetchone()
            if record:
                result = {'lock_type_id': lock_type_id,
                  'lock_object': record[0],
                  'lock_owner': record[1],
                  'lock_status_id': record[2],
                  'lock_detail': record[3]
                  }       
        finally:
            # Only close connection if it was created in this function
            if create_connection:
                lock_connection.close()
        
        return result
        
    def clear_all_locks(self, lock_object=None, lock_type_id=1, lock_owner=None):
        """ 
        USE WITH CAUTION - This will affect all processes using specified lock type
        """
        # Need separate non-persistent connection for lock mechanism to allow independent transaction commits
        lock_connection = self.create_connection()
        
        lock_cursor = lock_connection.cursor()
        sql = """-- Delete ALL lock objects matching any supplied parameters
delete from lock     
where (%(lock_type_id)s is null or lock_type_id = %(lock_type_id)s)
  and (%(lock_object)s is null or lock_object = %(lock_object)s)
  and (%(lock_owner)s is null or lock_owner = %(lock_owner)s);
""" 
        params = {'lock_type_id': lock_type_id,
                  'lock_object': lock_object,
                  'lock_owner': lock_owner
                  }
        
        log_multiline(logger.debug, lock_cursor.mogrify(sql, params), 'SQL', '\t')
        try:
            lock_cursor.execute(sql, params)
        finally:
            lock_connection.close()
    
    def check_files_ready(self, filename_list):
        logger.debug('Checking files %s', filename_list)
        for filename in filename_list:
            if not os.path.exists(filename) or self.check_object_locked(filename) or not os.path.getsize(filename):
                logger.debug('File %s is not ready', filename)
                return False # At least one file is not ready
        return True # All files are ready
    
    def touch(self, filename, output=None):
        new_file = open(filename, 'a', 0)
        if output:
            new_file.write(output + '\n')
        new_file.close()
                
    def cell_has_data(self, x_index, y_index, start_datetime=None, end_datetime=None, tile_type_id=None):
        db_cursor = self.db_connection.cursor()
        sql = """-- count of acquisitions which have tiles covering the matching indices
select count(distinct acquisition_id) as acquisition_count
from tile_footprint
  inner join tile using(x_index, y_index, tile_type_id)
  inner join dataset using (dataset_id)
  inner join acquisition using (acquisition_id)
where tile_type_id = %(tile_type_id)s
  and x_index = %(x_index)s and y_index = %(y_index)s and tile_type_id = %(tile_type_id)s
  and (%(start_datetime)s is null or start_datetime >= %(start_datetime)s)
  and (%(end_datetime)s is null or end_datetime <= %(end_datetime)s);      
"""        
        tile_type_id = tile_type_id or self.default_tile_type_id
        params = {'x_index': x_index,
                  'y_index': y_index,
                  'start_datetime': start_datetime,
                  'end_datetime': end_datetime,
                  'tile_type_id': tile_type_id
                  }
        
        log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')
        db_cursor.execute(sql, params)
        
        record = db_cursor.fetchone()
        if record:
            return record[0]
        else:
            return 0
        
                               
if __name__ == '__main__':

    # Set top level standard output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    datacube = DataCube()
    
    log_multiline(logger.info, datacube.__dict__, 'Datacube contents', '\t')
    
    # Test locking mechanism
    datacube.clear_all_locks(lock_object='***lock_test***')
    logger.info('clear_all_locks test passed: %s', not datacube.check_object_locked('***lock_test***'))
    
    datacube.lock_object('***lock_test***')
    logger.info('lock_object test passed: %s', bool(datacube.check_object_locked('***lock_test***')))
    
    datacube.unlock_object('***lock_test***')
    logger.info('unlock_object test passed: %s', not datacube.check_object_locked('***lock_test***'))


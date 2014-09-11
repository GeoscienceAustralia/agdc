#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

'''
Created on 05/10/2012

@author: Alex Ip
'''
import os
import sys
import argparse
import logging
import re
import psycopg2
import numpy
import shutil
from osgeo import gdal,osr
from math import floor, ceil
from datetime import datetime
from copy import copy
import time
import numexpr
from scipy import ndimage

from EOtools.bodies.vincenty import vinc_dist
from EOtools.blrb import interpolate_grid
    
from EOtools.utils import log_multiline
from EOtools.execute import execute

from agdc import DataCube


# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
CONTIGUITY_BIT_INDEX = 8
nan = numpy.float32(numpy.NaN) # Smoothed DSM should have all valid data values - need this for edges only
RADIANS_PER_DEGREE = 0.01745329251994329576923690768489
    
class earth(object):

    # Mean radius
    RADIUS = 6371009.0  # (metres)

    # WGS-84
    #RADIUS = 6378135.0  # equatorial (metres)
    #RADIUS = 6356752.0  # polar (metres)

    # Length of Earth ellipsoid semi-major axis (metres)
    SEMI_MAJOR_AXIS = 6378137.0

    # WGS-84
    A = 6378137.0           # equatorial radius (metres)
    B = 6356752.3142        # polar radius (metres)
    F = (A - B) / A         # flattening
    ECC2 = 1.0 - B**2/A**2  # squared eccentricity

    MEAN_RADIUS = (A*2 + B) / 3

    # Earth ellipsoid eccentricity (dimensionless)
    #ECCENTRICITY = 0.00669438
    #ECC2 = math.pow(ECCENTRICITY, 2)

    # Earth rotational angular velocity (radians/sec)
    OMEGA = 0.000072722052

class DEMTiler(DataCube):

    def getFileSizekB(self, path):
        """Gets the size of a file (megabytes).
    
        Arguments:
            path: file path
     
        Returns:
            File size (MB)
    
        Raises:
            OSError [Errno=2] if file does not exist
        """     
        return os.path.getsize(path) / 1024

    def getFileSizeMB(self, path):
        """Gets the size of a file (megabytes).
    
        Arguments:
            path: file path
     
        Returns:
            File size (MB)
    
        Raises:
            OSError [Errno=2] if file does not exist
        """     
        return self.getFileSizekB(path) / 1024
    
    def parse_args(self):
        """Overrides Datacube function to parse the command line arguments.
    
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
        _arg_parser.add_argument('--refresh', dest='refresh',
            default=True, action='store_const', const=True,
            help='Refresh mode flag to force updating of existing records')
        _arg_parser.add_argument('-t', '--tile_type', dest='default_tile_type_id',
            required=False, default=None,
            help='Tile type ID of tile to be stacked')
        _arg_parser.add_argument('-f', '--file', dest='filename',
            default='/g/data/v27/dem/dsm1sv1_0_Clean.img',
            help='Input DTM/DSM file to tile')
        _arg_parser.add_argument('-l', '--level', dest='level_name',
            default='DSM',
            help='Processing level of file')
    
        return _arg_parser.parse_args()
    
    def get_pixel_size(self, dataset, (x, y)):
        """
        Returns X & Y sizes in metres of specified pixel as a tuple.
        N.B: Pixel ordinates are zero-based from top left
        """
        logger.debug('(x, y) = (%f, %f)', x, y)
        
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromWkt(dataset.GetProjection())
        
        geotransform = dataset.GetGeoTransform()
        logger.debug('geotransform = %s', geotransform)
        
        latlong_spatial_reference = spatial_reference.CloneGeogCS()
        coord_transform_to_latlong = osr.CoordinateTransformation(spatial_reference, latlong_spatial_reference)

        # Determine pixel centre and edges in georeferenced coordinates
        xw = geotransform[0] + x * geotransform[1]
        yn = geotransform[3] + y * geotransform[5] 
        xc = geotransform[0] + (x + 0.5) * geotransform[1]
        yc = geotransform[3] + (y + 0.5) * geotransform[5] 
        xe = geotransform[0] + (x + 1.0) * geotransform[1]
        ys = geotransform[3] + (y + 1.0) * geotransform[5] 
        
        logger.debug('xw = %f, yn = %f, xc = %f, yc = %f, xe = %f, ys = %f', xw, yn, xc, yc, xe, ys)
        
        # Convert georeferenced coordinates to lat/lon for Vincenty
        lon1, lat1, _z = coord_transform_to_latlong.TransformPoint(xw, yc, 0)
        lon2, lat2, _z = coord_transform_to_latlong.TransformPoint(xe, yc, 0)
        logger.debug('For X size: (lon1, lat1) = (%f, %f), (lon2, lat2) = (%f, %f)', lon1, lat1, lon2, lat2)
        x_size, _az_to, _az_from = vinc_dist(earth.F, earth.A, 
                                             lat1 * RADIANS_PER_DEGREE, lon1 * RADIANS_PER_DEGREE, 
                                             lat2 * RADIANS_PER_DEGREE, lon2 * RADIANS_PER_DEGREE)
        
        lon1, lat1, _z = coord_transform_to_latlong.TransformPoint(xc, yn, 0)
        lon2, lat2, _z = coord_transform_to_latlong.TransformPoint(xc, ys, 0)
        logger.debug('For Y size: (lon1, lat1) = (%f, %f), (lon2, lat2) = (%f, %f)', lon1, lat1, lon2, lat2)
        y_size, _az_to, _az_from = vinc_dist(earth.F, earth.A, 
                                             lat1 * RADIANS_PER_DEGREE, lon1 * RADIANS_PER_DEGREE, 
                                             lat2 * RADIANS_PER_DEGREE, lon2 * RADIANS_PER_DEGREE)
        
        logger.debug('(x_size, y_size) = (%f, %f)', x_size, y_size)
        return (x_size, y_size)

    def get_pixel_size_grids(self, dataset):
        """ Returns two grids with interpolated X and Y pixel sizes for given datasets"""
        
        def get_pixel_x_size(x, y):
            return self.get_pixel_size(dataset, (x, y))[0]
        
        def get_pixel_y_size(x, y):
            return self.get_pixel_size(dataset, (x, y))[1]
        
        x_size_grid = numpy.zeros((dataset.RasterXSize, dataset.RasterYSize)).astype(numpy.float32)
        interpolate_grid(depth=1, shape=x_size_grid.shape, eval_func=get_pixel_x_size, grid=x_size_grid)

        y_size_grid = numpy.zeros((dataset.RasterXSize, dataset.RasterYSize)).astype(numpy.float32)
        interpolate_grid(depth=1, shape=y_size_grid.shape, eval_func=get_pixel_y_size, grid=y_size_grid)
        
        return (x_size_grid, y_size_grid)

    def __init__(self, source_datacube=None, default_tile_type_id=1):
        """Constructor
        Arguments:
            source_datacube: Optional DataCube object whose connection and data will be shared
            tile_type_id: Optional tile_type_id value (defaults to config file value = 1)
        """
        
        if source_datacube:
            # Copy values from source_datacube and then override command line args
            self.__dict__ = copy(source_datacube.__dict__)
            
            args = self.parse_args()
            # Set instance attributes for every value in command line arguments file
            for attribute_name in args.__dict__.keys():
                attribute_value = args.__dict__[attribute_name]
                self.__setattr__(attribute_name, attribute_value)

        else:
            DataCube.__init__(self); # Call inherited constructor
            
        if self.debug:
            console_handler.setLevel(logging.DEBUG)

        # Turn autocommit OFF so that transaction can cover all queries for each dataset
        self.db_connection.autocommit = False
        self.db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        # Attempt to parse dates from command line arguments or config file
        try:
            self.default_tile_type_id = int(self.default_tile_type_id) 
        except:
            self.default_tile_type_id = default_tile_type_id
        try:
            self.start_date = datetime.strptime(self.start_date, '%d/%m/%Y').date()
        except:
            self.start_date = None
        try:
            self.end_date = datetime.strptime(self.end_date, '%d/%m/%Y').date()
        except:
            self.end_date = None
        try:
            self.min_path = int(self.min_path) 
        except:
            self.min_path = None
        try:
            self.max_path = int(self.max_path) 
        except:
            self.max_path = None
        try:
            self.min_row = int(self.min_row) 
        except:
            self.min_row = None
        try:
            self.max_row = int(self.max_row) 
        except:
            self.max_row = None
            
    def create_tiles(self, filename=None, level_name=None, tile_type_id=None):
        # Set default values to instance values
        filename = filename or self.filename
        level_name = level_name or self.level_name
        tile_type_id = tile_type_id or self.default_tile_type_id
        nodata_value = None
        
        tile_type_info = self.tile_type_dict[tile_type_id]
        
        dem_band_info = self.bands[tile_type_id].get(('DERIVED', level_name))
        assert dem_band_info, 'No band level information defined for level %s' % level_name
        
        def find_tiles(x_index = None, y_index = None):
            """Find any tile records for current dataset
            returns dict of tile information keyed by tile_id
            """
            db_cursor2 = self.db_connection.cursor()
            
            sql = """-- Check for any existing tiles
select
tile_id,
x_index,
y_index,
tile_type_id,
tile_pathname,
dataset_id,
tile_class_id,
tile_size
from tile_footprint
inner join tile using(x_index, y_index, tile_type_id)
inner join dataset using(dataset_id)
inner join processing_level using(level_id)
where tile_type_id = %(tile_type_id)s
and (%(x_index)s is null or x_index = %(x_index)s)
and (%(y_index)s is null or y_index = %(y_index)s)
and level_name = %(level_name)s
and ctime is not null
;
"""
            params = {'x_index': x_index,
                  'y_index': y_index,
                  'tile_type_id': tile_type_info['tile_type_id'],
                  'level_name': level_name}
                          
            log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
            db_cursor2.execute(sql, params)
            tile_info = {}
            for record in db_cursor2:
                tile_info_dict = {
                    'x_index': record[1],
                    'y_index': record[2],
                    'tile_type_id': record[3],
                    'tile_pathname': record[4],
                    'dataset_id': record[5],
                    'tile_class_id': record[6],
                    'tile_size': record[7]
                    }
                tile_info[record[0]] = tile_info_dict # Keyed by tile_id
                
            log_multiline(logger.debug, tile_info, 'tile_info', '\t')
            return tile_info

        
        # Function create_tiles starts here
        db_cursor = self.db_connection.cursor()
        
        dataset = gdal.Open(filename)
        assert dataset, 'Unable to open dataset %s' % filename
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromWkt(dataset.GetProjection())
        geotransform = dataset.GetGeoTransform()
        logger.debug('geotransform = %s', geotransform)
            
        latlong_spatial_reference = spatial_reference.CloneGeogCS()
        coord_transform_to_latlong = osr.CoordinateTransformation(spatial_reference, latlong_spatial_reference)
        
        tile_spatial_reference = osr.SpatialReference()
        s = re.match('EPSG:(\d+)', tile_type_info['crs'])
        if s:
            epsg_code = int(s.group(1))
            logger.debug('epsg_code = %d', epsg_code)
            assert tile_spatial_reference.ImportFromEPSG(epsg_code) == 0, 'Invalid EPSG code for tile projection'
        else:
            assert tile_spatial_reference.ImportFromWkt(tile_type_info['crs']), 'Invalid WKT for tile projection'
        
        logger.debug('Tile WKT = %s', tile_spatial_reference.ExportToWkt())
            
        coord_transform_to_tile = osr.CoordinateTransformation(spatial_reference, tile_spatial_reference)
        
        # Need to keep tile and lat/long references separate even though they may be equivalent
        # Upper Left
        ul_x, ul_y = geotransform[0], geotransform[3]
        ul_lon, ul_lat, _z = coord_transform_to_latlong.TransformPoint(ul_x, ul_y, 0)
        tile_ul_x, tile_ul_y, _z = coord_transform_to_tile.TransformPoint(ul_x, ul_y, 0)
        # Upper Right
        ur_x, ur_y = geotransform[0] + geotransform[1] * dataset.RasterXSize, geotransform[3]
        ur_lon, ur_lat, _z = coord_transform_to_latlong.TransformPoint(ur_x, ur_y, 0)
        tile_ur_x, tile_ur_y, _z = coord_transform_to_tile.TransformPoint(ur_x, ur_y, 0)
        # Lower Right
        lr_x, lr_y = geotransform[0] + geotransform[1] * dataset.RasterXSize, geotransform[3] + geotransform[5] * dataset.RasterYSize
        lr_lon, lr_lat, _z = coord_transform_to_latlong.TransformPoint(lr_x, lr_y, 0)
        tile_lr_x, tile_lr_y, _z = coord_transform_to_tile.TransformPoint(lr_x, lr_y, 0)
        # Lower Left
        ll_x, ll_y = geotransform[0], geotransform[3] + geotransform[5] * dataset.RasterYSize
        ll_lon, ll_lat, _z = coord_transform_to_latlong.TransformPoint(ll_x, ll_y, 0)
        tile_ll_x, tile_ll_y, _z = coord_transform_to_tile.TransformPoint(ll_x, ll_y, 0)
        
        tile_min_x = min(tile_ul_x, tile_ll_x)
        tile_max_x = max(tile_ur_x, tile_lr_x)
        tile_min_y = min(tile_ll_y, tile_lr_y)
        tile_max_y = max(tile_ul_y, tile_ur_y)
        
        tile_index_range = (int(floor((tile_min_x - tile_type_info['x_origin']) / tile_type_info['x_size'])), 
                    int(floor((tile_min_y - tile_type_info['y_origin']) / tile_type_info['y_size'])), 
                    int(ceil((tile_max_x - tile_type_info['x_origin']) / tile_type_info['x_size'])), 
                    int(ceil((tile_max_y - tile_type_info['y_origin']) / tile_type_info['y_size'])))
        
        sql = """-- Find dataset_id for given path
select dataset_id
from dataset 
where dataset_path like '%%' || %(basename)s
"""
        params = {'basename': os.path.basename(filename)}
        log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')
        db_cursor.execute(sql, params)
        result = db_cursor.fetchone()
        if result: # Record already exists
            dataset_id = result[0]
            if self.refresh:
                logger.info('Updating existing record for %s', filename)
                
                sql = """
update dataset 
  set level_id = (select level_id from processing_level where upper(level_name) = upper(%(processing_level)s)),
  datetime_processed = %(datetime_processed)s,
  dataset_size = %(dataset_size)s,
  crs = %(crs)s,
  ll_x = %(ll_x)s,
  ll_y = %(ll_y)s,
  lr_x = %(lr_x)s,
  lr_y = %(lr_y)s,
  ul_x = %(ul_x)s,
  ul_y = %(ul_y)s,
  ur_x = %(ur_x)s,
  ur_y = %(ur_y)s,
  x_pixels = %(x_pixels)s,
  y_pixels = %(y_pixels)s
where dataset_id = %(dataset_id)s;

select %(dataset_id)s
"""
            else:
                logger.info('Skipping existing record for %s', filename)
                return
        else: # Record doesn't already exist
            logger.info('Creating new record for %s', filename)
            dataset_id = None       
                    
            sql = """-- Create new dataset record
insert into dataset(
  dataset_id, 
  acquisition_id, 
  dataset_path, 
  level_id,
  datetime_processed,
  dataset_size,
  crs,
  ll_x,
  ll_y,
  lr_x,
  lr_y,
  ul_x,
  ul_y,
  ur_x,
  ur_y,
  x_pixels,
  y_pixels
  )
select
  nextval('dataset_id_seq') as dataset_id,
  null as acquisition_id,
  %(dataset_path)s,
  (select level_id from processing_level where upper(level_name) = upper(%(processing_level)s)),
  %(datetime_processed)s,
  %(dataset_size)s,
  %(crs)s,
  %(ll_x)s,
  %(ll_y)s,
  %(lr_x)s,
  %(lr_y)s,
  %(ul_x)s,
  %(ul_y)s,
  %(ur_x)s,
  %(ur_y)s,
  %(x_pixels)s,
  %(y_pixels)s
where not exists
  (select dataset_id
  from dataset
  where dataset_path = %(dataset_path)s
  );

select dataset_id 
from dataset
where dataset_path = %(dataset_path)s
;
"""
        dataset_size = self.getFileSizekB(filename) # Need size in kB to match other datasets 
        
        # same params for insert or update
        params = {'dataset_id': dataset_id,
            'dataset_path': filename,
            'processing_level': level_name,
            'datetime_processed': None,
            'dataset_size': dataset_size,
            'll_lon': ll_lon,
            'll_lat': ll_lat,
            'lr_lon': lr_lon,
            'lr_lat': lr_lat,
            'ul_lon': ul_lon,
            'ul_lat': ul_lat,
            'ur_lon': ur_lon,
            'ur_lat': ur_lat,
            'crs': dataset.GetProjection(),
            'll_x': ll_x,
            'll_y': ll_y,
            'lr_x': lr_x,
            'lr_y': lr_y,
            'ul_x': ul_x,
            'ul_y': ul_y,
            'ur_x': ur_x,
            'ur_y': ur_y,
            'x_pixels': dataset.RasterXSize,
            'y_pixels': dataset.RasterYSize,
            'gcp_count': None,
            'mtl_text': None,
            'cloud_cover': None
            }
        
        log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')    
        db_cursor.execute(sql, params)
        result = db_cursor.fetchone() # Retrieve new dataset_id if required
        dataset_id = dataset_id or result[0]

        tile_output_root = os.path.join(self.tile_root, 
                                        tile_type_info['tile_directory'],
                                        level_name, 
                                        os.path.basename(filename)
                                        )
        logger.debug('tile_output_root = %s', tile_output_root)
        self.create_directory(tile_output_root)

        work_directory = os.path.join(self.temp_dir,
                                      os.path.basename(filename)
                                      )
        logger.debug('work_directory = %s', work_directory)
        self.create_directory(work_directory)
                
        for x_index in range(tile_index_range[0], tile_index_range[2]):
            for y_index in range(tile_index_range[1], tile_index_range[3]): 
                
                tile_info = find_tiles(x_index, y_index)
                
                if tile_info:
                    logger.info('Skipping existing tile (%d, %d)', x_index, y_index)
                    continue

                tile_basename = '_'.join([level_name,
                                          re.sub('\+', '', '%+04d_%+04d' % (x_index, y_index))]) + tile_type_info['file_extension']
                
                tile_output_path = os.path.join(tile_output_root, tile_basename)
                                                                   
                # Check whether this tile has already been processed
                if not self.lock_object(tile_output_path):
                    logger.warning('Tile  %s already being processed - skipping.', tile_output_path)
                    continue
                
                try:                 
                    self.remove(tile_output_path)
                    
                    temp_tile_path = os.path.join(self.temp_dir, tile_basename)
                                                                       
                    tile_extents = (tile_type_info['x_origin'] + x_index * tile_type_info['x_size'], 
                                tile_type_info['y_origin'] + y_index * tile_type_info['y_size'], 
                                tile_type_info['x_origin'] + (x_index + 1) * tile_type_info['x_size'], 
                                tile_type_info['y_origin'] + (y_index + 1) * tile_type_info['y_size']) 
                    logger.debug('tile_extents = %s', tile_extents)
                    
                    command_string = 'gdalwarp'
                    if not self.debug:
                        command_string += ' -q'
                    command_string += ' -t_srs %s -te %f %f %f %f -tr %f %f -tap -tap -r %s' % (
                        tile_type_info['crs'],
                        tile_extents[0], tile_extents[1], tile_extents[2], tile_extents[3], 
                        tile_type_info['x_pixel_size'], tile_type_info['y_pixel_size'],
                        dem_band_info[10]['resampling_method']
                        )
                    
                    if nodata_value is not None:
                        command_string += ' -srcnodata %d -dstnodata %d' % (nodata_value, nodata_value)
                                                                          
                    command_string += ' -of %s' % tile_type_info['file_format']
                    
                    if tile_type_info['format_options']:
                        for format_option in tile_type_info['format_options'].split(','):
                            command_string += ' -co %s' % format_option
                        
                    command_string += ' -overwrite %s %s' % (
                        filename,
                        temp_tile_path
                        )
                    
                    logger.debug('command_string = %s', command_string)
                    
                    result = execute(command_string=command_string)
                    
                    if result['stdout']:
                        log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 
                    
                    if result['returncode']:
                        log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')
                        raise Exception('%s failed', command_string) 
                    
                    temp_dataset = gdal.Open(temp_tile_path)
                                    
                    gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                    #output_dataset = gdal_driver.Create(output_tile_path, 
                    #                                    nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                    #                                    1, nbar_dataset.GetRasterBand(1).DataType,
                    #                                    tile_type_info['format_options'].split(','))
                    output_dataset = gdal_driver.Create(tile_output_path, 
                                                        temp_dataset.RasterXSize, temp_dataset.RasterYSize,
                                                        len(dem_band_info), 
                                                        temp_dataset.GetRasterBand(1).DataType,
                                                        tile_type_info['format_options'].split(','))
                    assert output_dataset, 'Unable to open output dataset %s'% output_dataset  
                    output_geotransform = temp_dataset.GetGeoTransform()                                 
                    output_dataset.SetGeoTransform(output_geotransform)
                    output_dataset.SetProjection(temp_dataset.GetProjection()) 
                    
                    elevation_array = temp_dataset.GetRasterBand(1).ReadAsArray()
                    del temp_dataset
                    self.remove(temp_tile_path)
        
                    pixel_x_size = abs(output_geotransform[1])
                    pixel_y_size = abs(output_geotransform[5])
                    x_m_array, y_m_array = self.get_pixel_size_grids(output_dataset)
                    
                    dzdx_array = ndimage.sobel(elevation_array, axis=1)/(8. * abs(output_geotransform[1]))
                    dzdx_array = numexpr.evaluate("dzdx_array * pixel_x_size / x_m_array")
                    del x_m_array
                    
                    dzdy_array = ndimage.sobel(elevation_array, axis=0)/(8. * abs(output_geotransform[5]))
                    dzdy_array = numexpr.evaluate("dzdy_array * pixel_y_size / y_m_array")
                    del y_m_array

                    for band_file_number in sorted(dem_band_info.keys()):
                        output_band_number = dem_band_info[band_file_number]['tile_layer']
                        output_band = output_dataset.GetRasterBand(output_band_number)
                        
                        if band_file_number == 10: # Elevation
                            output_band.WriteArray(elevation_array)
                            del elevation_array
                            
                        elif band_file_number == 20: # Slope    
                            hypotenuse_array = numpy.hypot(dzdx_array, dzdy_array)
                            slope_array = numexpr.evaluate("arctan(hypotenuse_array) / RADIANS_PER_DEGREE")
                            del hypotenuse_array
                            output_band.WriteArray(slope_array)
                            del slope_array
                            
                        elif band_file_number == 30: # Aspect
                            # Convert angles from conventional radians to compass heading 0-360
                            aspect_array = numexpr.evaluate("(450 - arctan2(dzdy_array, -dzdx_array) / RADIANS_PER_DEGREE) % 360")
                            output_band.WriteArray(aspect_array)
                            del aspect_array

                        if nodata_value is not None:
                            output_band.SetNoDataValue(nodata_value)
                        output_band.FlushCache()
                    
                    #===========================================================
                    # # This is not strictly necessary - copy metadata to output dataset
                    # output_dataset_metadata = temp_dataset.GetMetadata()
                    # if output_dataset_metadata:
                    #    output_dataset.SetMetadata(output_dataset_metadata) 
                    #    log_multiline(logger.debug, output_dataset_metadata, 'output_dataset_metadata', '\t')    
                    #===========================================================
                    
                    output_dataset.FlushCache()
                    del output_dataset
                    logger.info('Finished writing dataset %s', tile_output_path)
                    
                    tile_size = self.getFileSizeMB(tile_output_path)
    
                    sql = """-- Insert new tile_footprint record if necessary
    insert into tile_footprint (
      x_index, 
      y_index, 
      tile_type_id, 
      x_min, 
      y_min, 
      x_max, 
      y_max
      )
    select
      %(x_index)s, 
      %(y_index)s, 
      %(tile_type_id)s, 
      %(x_min)s, 
      %(y_min)s, 
      %(x_max)s, 
      %(y_max)s
    where not exists
      (select 
        x_index, 
        y_index, 
        tile_type_id
      from tile_footprint
      where x_index = %(x_index)s 
        and y_index = %(y_index)s 
        and tile_type_id = %(tile_type_id)s);
    
    -- Update any existing tile record
    update tile
    set 
      tile_pathname = %(tile_pathname)s,
      tile_class_id = %(tile_class_id)s,
      tile_size = %(tile_size)s,
      ctime = now()
    where 
      x_index = %(x_index)s
      and y_index = %(y_index)s
      and tile_type_id = %(tile_type_id)s
      and dataset_id = %(dataset_id)s;
    
    -- Insert new tile record if necessary
    insert into tile (
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
      nextval('tile_id_seq'::regclass),
      %(x_index)s,
      %(y_index)s,
      %(tile_type_id)s,
      %(dataset_id)s,
      %(tile_pathname)s,
      %(tile_class_id)s,
      %(tile_size)s,
      now()
    where not exists
      (select tile_id
      from tile
      where 
        x_index = %(x_index)s
        and y_index = %(y_index)s
        and tile_type_id = %(tile_type_id)s
        and dataset_id = %(dataset_id)s
      );
    """  
                    params = {'x_index': x_index,
                              'y_index': y_index,
                              'tile_type_id': tile_type_info['tile_type_id'],
                              'x_min': tile_extents[0], 
                              'y_min': tile_extents[1], 
                              'x_max': tile_extents[2], 
                              'y_max': tile_extents[3],
                              'dataset_id': dataset_id, 
                              'tile_pathname': tile_output_path,
                              'tile_class_id': 1,
                              'tile_size': tile_size
                              }
                    
                    log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')
                    db_cursor.execute(sql, params)
                            
                    self.db_connection.commit()  
                finally:
                    self.unlock_object(tile_output_path)
                
        logger.info('Finished creating all tiles')
                
                       
if __name__ == '__main__':
    dem_tiler = DEMTiler()
       
    dem_tiler.create_tiles()

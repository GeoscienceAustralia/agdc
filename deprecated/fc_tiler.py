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
from math import floor,ceil
from datetime import datetime
from copy import copy
import time
import string
    
from EOtools.utils import log_multiline
from EOtools.execute import execute

from agdc import DataCube

TILE_OWNER = 'axi547:rs0' # Owner of file files

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class LandsatTiler(DataCube):

    CONTIGUITY_BIT_INDEX = 8
    
    def getFileSizeMB(self, path):
        """Gets the size of a file (megabytes).
    
        Arguments:
            path: file path
     
        Returns:
            File size (MB)
    
        Raises:
            OSError [Errno=2] if file does not exist
        """     
        return os.path.getsize(path) / (1024*1024)

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
    
        return _arg_parser.parse_args()
    
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
            
    def create_tiles(self, start_date=None, end_date=None, min_path=None, max_path=None, min_row=None, max_row=None, tile_type_id=None):
        # Set default values to instance values
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        min_path = min_path or self.min_path
        max_path = max_path or self.max_path
        min_row = min_row or self.min_row
        max_row = max_row or self.max_row
        tile_type_id = tile_type_id or self.default_tile_type_id
        
        tile_type_info = self.tile_type_dict[tile_type_id]
        
        def process_dataset(dataset_info):
            log_multiline(logger.debug, dataset_info, 'Dataset values', '\t')
            
            def find_file(dataset_dir, file_pattern):
#                logger.debug('find_file(%s, %s) called', dataset_dir, file_pattern)
                assert os.path.isdir(dataset_dir), '%s is not a valid directory' % dataset_dir
                filelist = [filename for filename in os.listdir(dataset_dir) if re.match(file_pattern, filename)]
#                logger.debug('filelist = %s', filelist)
                assert len(filelist) == 1, 'Unable to find unique match for file pattern %s' % file_pattern
                return os.path.join(dataset_dir, filelist[0])
            
            def get_tile_index_range(dataset_filename):
                """Returns integer (xmin, ymin, xmax, ymax) tuple for input GDAL dataset filename"""
                dataset = gdal.Open(dataset_filename)
                assert dataset, 'Unable to open dataset %s' % dataset_filename
                spatial_reference = osr.SpatialReference()
                spatial_reference.ImportFromWkt(dataset.GetProjection())
                geotransform = dataset.GetGeoTransform()
                logger.debug('geotransform = %s', geotransform)
#                latlong_spatial_reference = spatial_reference.CloneGeogCS()
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
                # Upper Left
                xmin, ymax, _z = coord_transform_to_tile.TransformPoint(geotransform[0], geotransform[3], 0)
                # Lower Right
                xmax, ymin, _z = coord_transform_to_tile.TransformPoint(geotransform[0] + geotransform[1] * dataset.RasterXSize, 
                                                                       geotransform[3] + geotransform[5] * dataset.RasterYSize, 
                                                                       0)
                
                logger.debug('Coordinates: xmin = %f, ymin = %f, xmax = %f, ymax = %f', xmin, ymin, xmax, ymax)

                return (int(floor((xmin - tile_type_info['x_origin']) / tile_type_info['x_size'])), 
                        int(floor((ymin - tile_type_info['y_origin']) / tile_type_info['y_size'])), 
                        int(ceil((xmax - tile_type_info['x_origin']) / tile_type_info['x_size'])), 
                        int(ceil((ymax - tile_type_info['y_origin']) / tile_type_info['y_size'])))
                
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
where (%(x_index)s is null or x_index = %(x_index)s)
  and (%(y_index)s is null or y_index = %(y_index)s)
  and tile_type_id = %(tile_type_id)s
  and dataset_id = %(fc_dataset_id)s

  and ctime is not null -- TODO: Remove this after reload
;
"""
                params = {'x_index': x_index,
                      'y_index': y_index,
                      'tile_type_id': tile_type_info['tile_type_id'],
                      'fc_dataset_id': dataset_info['fc_dataset_id']}
                              
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
                    
                
            def get_vrt_band_list():
                """Returns list of band information to create tiles
                """
                logger.debug('get_vrt_band_list() called')
                vrt_band_list = []
#===============================================================================
#                 sensor_dict = self.bands[tile_type_id][(dataset_info['satellite_tag'], dataset_info['sensor_name'])]
# #                log_multiline(logger.debug, sensor, 'Sensor', '\t')
#                 for file_number in sorted(sensor_dict.keys()):
#                     band_info = sensor_dict[file_number]
#                     if band_info['level_name'] == 'NBAR':
#                         dataset_dir = dataset_info['nbar_dataset_path']
#                         dataset_id = dataset_info['nbar_dataset_id']
#                         processing_level = dataset_info['nbar_level_name']
#                         nodata_value = dataset_info['nbar_nodata_value']
#                         resampling_method = dataset_info['nbar_resampling_method']
#                     elif band_info['level_name'] == 'ORTHO':
#                         dataset_dir = dataset_info['l1t_dataset_path']
#                         dataset_id = dataset_info['l1t_dataset_id']
#                         processing_level = dataset_info['l1t_level_name']
#                         nodata_value = dataset_info['l1t_nodata_value']
#                         resampling_method = dataset_info['l1t_resampling_method']
#                     else:
#                         continue # Ignore any pan-chromatic and derived bands
#                     
#                     dataset_dir = os.path.join(dataset_dir, 'scene01')
#                     filename = find_file(dataset_dir, band_info['file_pattern'])
#                     vrt_band_list.append({'file_number': band_info['file_number'], 
#                                           'filename': filename, 
#                                           'name': band_info['band_name'],
#                                           'dataset_id': dataset_id,
#                                           'band_id': band_info['band_id'],
#                                           'processing_level': processing_level,
#                                           'nodata_value': nodata_value,
#                                           'resampling_method': resampling_method,
#                                           'tile_layer': band_info['tile_layer']})
#===============================================================================
                    
                #TODO: Make this able to handle multiple derived layers
                for band_level in ['FC']:
                    derived_bands = self.bands[tile_type_id][('DERIVED', band_level)]
                    for file_number in sorted(derived_bands.keys()):
                        band_info = derived_bands[file_number]
                        file_pattern = band_info['file_pattern']
                        dataset_dir = os.path.join(dataset_info['fc_dataset_path'], 'scene01')
                        dataset_id = dataset_info['fc_dataset_id']
                        filename = find_file(dataset_dir, file_pattern) 
                        processing_level = dataset_info['fc_level_name']
                        nodata_value = dataset_info['fc_nodata_value'] # Should be None for FC
                        resampling_method = dataset_info['fc_resampling_method']
                        vrt_band_list.append({'file_number': None, 
                                      'filename': filename, 
                                      'name': band_info['band_name'],
                                      'dataset_id': dataset_id,
                                      'band_id': band_info['band_id'],
                                      'processing_level': processing_level,
                                      'nodata_value': nodata_value,
                                      'resampling_method': resampling_method,
                                      'tile_layer': 1})
                
                log_multiline(logger.debug, vrt_band_list, 'vrt_band_list = %s', '\t')
                return vrt_band_list
            
            def get_tile_has_data(tile_index_range):
                tile_has_data = {}
                db_cursor2 = self.db_connection.cursor()
                sql = """-- Find all PQA tiles which exist for the dataset
select
  x_index,
  y_index
from dataset
  inner join tile using(dataset_id)
where tile_type_id = %(tile_type_id)s
  and level_id = 3 -- PQA
  and tile_class_id = 1 -- Tile containing live data
  and acquisition_id = %(acquisition_id)s             
                """
                params = {'tile_type_id': tile_type_info['tile_type_id'],
                      'acquisition_id': dataset_info['acquisition_id']}
                              
                log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
                db_cursor2.execute(sql, params)
                      
                for x_index in range(tile_index_range[0], tile_index_range[2]):
                    for y_index in range(tile_index_range[1], tile_index_range[3]):  
                        tile_has_data[(x_index, y_index)] = False
                
                # Set tile_has_data element to True if PQA tile exists
                for record in db_cursor2:
                    tile_has_data[(record[0], record[1])] = True
                
                return tile_has_data
            
            
            # process_dataset function starts here
            result = False
            db_cursor1 = self.db_connection.cursor()
            
            logger.info('Processing dataset %s', dataset_info['fc_dataset_path'])
            
            vrt_band_stack_basename = '_'.join([dataset_info['satellite_tag'], 
                    re.sub('\W', '', dataset_info['sensor_name']), 
                    dataset_info['start_datetime'].date().strftime('%Y%m%d'), 
                    '%03d' % dataset_info['x_ref'], 
                    '%03d' % dataset_info['y_ref']]
                    ) + '.vrt'
            logger.debug('vrt_band_stack_basename = %s', vrt_band_stack_basename)
            
            tile_output_root = os.path.join(self.tile_root, tile_type_info['tile_directory'],
                                                 dataset_info['satellite_tag'] + '_' + re.sub('\W', '', dataset_info['sensor_name'])) 
            logger.debug('tile_output_root = %s', tile_output_root)

            vrt_band_list = get_vrt_band_list()
            tile_index_range = get_tile_index_range(vrt_band_list[0]['filename']) # Find extents of first band dataset
            tile_count = abs(tile_index_range[2] - tile_index_range[0]) * (tile_index_range[3] - tile_index_range[1])
            
            # Check whether tiles exist for every band
            tile_record_count = len(find_tiles())
            logger.info('Found %d tile records in database for %d tiles', tile_record_count, tile_count) # Count FC only
            if tile_record_count == tile_count:
                logger.info('All tiles already exist in database - skipping tile creation for %s', dataset_info['fc_dataset_path'])
                return result
            
            try:
                
                #TODO: Create all new acquisition records and commit the transaction here                
                
                # Use NBAR dataset name for dataset lock (could have been any other level)
                work_directory = os.path.join(self.temp_dir,
                                         os.path.basename(dataset_info['fc_dataset_path'])
                                         )
                
                tile_has_data = get_tile_has_data(tile_index_range)             

                any_tile_has_data = False
                for value in tile_has_data.values():
                    any_tile_has_data |= value

                if not any_tile_has_data:
                    logger.info('No valid PQ tiles found - skipping tile creation for %s', dataset_info['fc_dataset_path'])
                    return result
                
                #TODO: Apply lock on path/row instead of on dataset to try to force the same node to process the full depth
                if not self.lock_object(work_directory):
                    logger.info('Already processing %s - skipping', dataset_info['fc_dataset_path'])
                    return result
                
                if self.refresh and os.path.exists(work_directory):
                    shutil.rmtree(work_directory)
                
                self.create_directory(work_directory)
                
                for processing_level in ['FC']:
                    vrt_band_info_list = [vrt_band_info for vrt_band_info in vrt_band_list if vrt_band_info['processing_level'] == processing_level]
                    nodata_value = vrt_band_info_list[0]['nodata_value'] # All the same for a given processing_level
                    resampling_method = vrt_band_info_list[0]['resampling_method'] # All the same for a given processing_level
                    
                    vrt_band_stack_filename = os.path.join(work_directory,
                                                           processing_level + '_' + vrt_band_stack_basename)
                    
                    if not os.path.exists(vrt_band_stack_filename) or self.check_object_locked(vrt_band_stack_filename):
    
                        # Check whether this dataset is already been processed
                        if not self.lock_object(vrt_band_stack_filename):
                            logger.warning('Band stack %s already being processed - skipping.', vrt_band_stack_filename)
                            continue
        
                        logger.info('Creating %s band stack file %s', processing_level, vrt_band_stack_filename)
                        command_string = 'gdalbuildvrt -separate'
                        if not self.debug:
                            command_string += ' -q'
                        if nodata_value is not None:
                            command_string += ' -srcnodata %d -vrtnodata %d' % (
                            nodata_value,                                                                                      
                            nodata_value)                                                                                 
                        command_string += ' -overwrite %s %s' % (
                            vrt_band_stack_filename,
                            ' '.join([vrt_band_info['filename'] for vrt_band_info in vrt_band_info_list])
                            )
                        logger.debug('command_string = %s', command_string)
                    
                        result = execute(command_string=command_string)
                    
                        if result['stdout']:
                            log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 
                
                        if result['returncode']:
                            log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')
                            raise Exception('%s failed', command_string) 
                    
                        band_stack_dataset = gdal.Open(vrt_band_stack_filename)
                        assert band_stack_dataset, 'Unable to open VRT %s' % vrt_band_stack_filename
                        band_stack_dataset.SetMetadata(
                            {'satellite': dataset_info['satellite_tag'], 
                             'sensor': dataset_info['sensor_name'], 
                             'start_datetime': dataset_info['start_datetime'].isoformat(),
                             'end_datetime': dataset_info['end_datetime'].isoformat(),
                             'path': '%03d' % dataset_info['x_ref'],
                             'row': '%03d' % dataset_info['y_ref']}
                            )
                    
                        for band_index in range(len(vrt_band_info_list)):
                            band = band_stack_dataset.GetRasterBand(band_index + 1)
                            band.SetMetadata({'name': vrt_band_info_list[band_index]['name'], 
                                              'filename': vrt_band_info_list[band_index]['filename']})
                            
                            # Need to set nodata values for each band - can't seem to do it in gdalbuildvrt
                            nodata_value = vrt_band_info_list[band_index]['nodata_value']
                            if nodata_value is not None:
                                band.SetNoDataValue(nodata_value)
                            
                        band_stack_dataset.FlushCache()
                        self.unlock_object(vrt_band_stack_filename)
                    else:
                        logger.info('Band stack %s already exists', vrt_band_stack_filename)
                        band_stack_dataset = gdal.Open(vrt_band_stack_filename)
        
                    logger.info('Processing %d %s Tiles', tile_count, processing_level)
                    for x_index in range(tile_index_range[0], tile_index_range[2]):
                        for y_index in range(tile_index_range[1], tile_index_range[3]):                       
                            tile_extents = (tile_type_info['x_origin'] + x_index * tile_type_info['x_size'], 
                            tile_type_info['y_origin'] + y_index * tile_type_info['y_size'], 
                            tile_type_info['x_origin'] + (x_index + 1) * tile_type_info['x_size'], 
                            tile_type_info['y_origin'] + (y_index + 1) * tile_type_info['y_size']) 
                            logger.debug('tile_extents = %s', tile_extents)
                                                
                            tile_output_dir = os.path.join(tile_output_root, 
                                                           re.sub('\+', '', '%+04d_%+04d' % (x_index, y_index)),
                                                                  '%04d' % dataset_info['start_datetime'].year
                                                           ) 
                                                   
                            self.create_directory(os.path.join(tile_output_dir, 'mosaic_cache'))
                            
                            tile_output_path = os.path.join(tile_output_dir,
                                '_'.join([dataset_info['satellite_tag'], 
                                    re.sub('\W', '', dataset_info['sensor_name']),
                                    processing_level,
                                    re.sub('\+', '', '%+04d_%+04d' % (x_index, y_index)),
                                    re.sub(':', '-', dataset_info['start_datetime'].isoformat())
                                    ]) + tile_type_info['file_extension']
                                )
                                     
                            # Check whether this tile has already been processed
                            if not self.lock_object(tile_output_path):
                                logger.warning('Tile  %s already being processed - skipping.', tile_output_path)
                                continue
                            
                            # Only generate tile file if PQA tile or tile contains data
                            if tile_has_data.get((x_index, y_index)) is None or tile_has_data[(x_index, y_index)]:                               
                                command_string = 'gdalwarp'
                                if not self.debug:
                                    command_string += ' -q'
                                command_string += ' -t_srs %s -te %f %f %f %f -tr %f %f -tap -tap -r %s' % (
                                    tile_type_info['crs'],
                                    tile_extents[0], tile_extents[1], tile_extents[2], tile_extents[3], 
                                    tile_type_info['x_pixel_size'], tile_type_info['y_pixel_size'],
                                    resampling_method
                                    )
                                
                                if nodata_value is not None:
                                    command_string += ' -srcnodata %d -dstnodata %d' % (nodata_value, nodata_value)
                                                                                      
                                command_string += ' -of %s' % tile_type_info['file_format']
                                
                                if tile_type_info['format_options']:
                                    for format_option in tile_type_info['format_options'].split(','):
                                        command_string += ' -co %s' % format_option
                                    
                                command_string += ' -overwrite %s %s' % (
                                    vrt_band_stack_filename,
                                    tile_output_path
                                    )
             
                                logger.debug('command_string = %s', command_string)
                                
                                retry=True
                                while retry:
                                    result = execute(command_string=command_string)

                                    if result['stdout']:
                                        log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t')

                                    if result['returncode']: # Return code is non-zero
                                        log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')

                                        # Work-around for gdalwarp error writing LZW-compressed GeoTIFFs 
                                        if (string.find(result['stderr'], 'LZW') > -1 # LZW-related error
                                            and tile_type_info['file_format'] == 'GTiff' # Output format is GeoTIFF
                                            and string.find(tile_type_info['format_options'], 'COMPRESS=LZW') > -1): # LZW compression requested
                                            
                                            temp_tile_path = os.path.join(os.path.dirname(vrt_band_stack_filename), 
                                                                          os.path.basename(tile_output_path))

                                            # Write uncompressed tile to a temporary path
                                            command_string = string.replace(command_string, 'COMPRESS=LZW', 'COMPRESS=NONE')
                                            command_string = string.replace(command_string, tile_output_path, temp_tile_path)
                                            
                                            # Translate temporary uncompressed tile to final compressed tile
                                            command_string += '; gdal_translate -of GTiff'
                                            if tile_type_info['format_options']:
                                                for format_option in tile_type_info['format_options'].split(','):
                                                    command_string += ' -co %s' % format_option
                                            command_string += ' %s %s' % (
                                                                          temp_tile_path,
                                                                          tile_output_path
                                                                          )
                                        else:
                                            raise Exception('%s failed', command_string)
                                    else:
                                        retry = False # No retry on success
                                
                                # Set tile metadata
                                tile_dataset = gdal.Open(tile_output_path)
                                assert tile_dataset, 'Unable to open tile dataset %s' % tile_output_path
                                
                                # Check whether PQA tile contains any  contiguous data
                                if tile_has_data.get((x_index, y_index)) is None and processing_level == 'PQA':
                                    tile_has_data[(x_index, y_index)] = ((numpy.bitwise_and(tile_dataset.GetRasterBand(1).ReadAsArray(), 
                                                                                          1 << LandsatTiler.CONTIGUITY_BIT_INDEX)) > 0).any()
                                    logger.debug('%s tile (%d, %d) has data = %s', processing_level, x_index, y_index, tile_has_data[(x_index, y_index)])
                                
                                # Only bother setting metadata if tile has valid data
                                if tile_has_data[(x_index, y_index)]:    
                                    metadata = band_stack_dataset.GetMetadata()
                                    metadata['x_index'] = str(x_index)
                                    metadata['y_index'] = str(y_index)
                                    tile_dataset.SetMetadata(metadata)
                                    
                                    # Set tile band metadata
                                    for band_index in range(len(vrt_band_info_list)):
                                        scene_band = band_stack_dataset.GetRasterBand(band_index + 1)
                                        tile_band = tile_dataset.GetRasterBand(band_index + 1)
                                        tile_band.SetMetadata(scene_band.GetMetadata())
                                        
                                        # Need to set nodata values for each band - gdalwarp doesn't copy it across
                                        nodata_value = vrt_band_info_list[band_index]['nodata_value']
                                        if nodata_value is not None:
                                            tile_band.SetNoDataValue(nodata_value)
    
                                              
                                    logger.info('Processed %s Tile (%d, %d)', processing_level, x_index, y_index)
                                else:
                                    logger.info('Skipped empty %s Tile (%d, %d)', processing_level, x_index, y_index)
                            else:
                                logger.info('Skipped empty %s Tile (%d, %d)', processing_level, x_index, y_index)
    
                            
                            # Change permissions on any recently created files
                            command_string = 'chmod -R a-wxs,u+rwX,g+rsX %s; chown -R %s %s' % (tile_output_dir,
                                                                                                TILE_OWNER,
                                                                                                tile_output_dir)
                            
                            result = execute(command_string=command_string)
                            
                            if result['stdout']:
                                log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 
                        
                            # N.B: command may return errors for files not owned by user
                            if result['returncode']:
                                log_multiline(logger.warning, result['stderr'], 'stderr from ' + command_string, '\t')
#                                raise Exception('%s failed', command_string) 
                            
                            self.unlock_object(tile_output_path)
                               
                            # Check whether tile contains any data    
                            if tile_has_data[(x_index, y_index)]:   
                                tile_class_id = 1 # Valid tile
                                tile_size = self.getFileSizeMB(tile_output_path)
                            else: # PQA tile contains no data 
                                # Remove empty PQA tile file
                                tile_class_id = 2 # Dummy tile record with no file
                                self.remove(tile_output_path)
                                tile_size = 0  
                                                       
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
                                      'dataset_id': vrt_band_info_list[0]['dataset_id'], # All the same
                                      'tile_pathname': tile_output_path,
                                      'tile_class_id': tile_class_id,
                                      'tile_size': tile_size
                                      }
                            
                            log_multiline(logger.debug, db_cursor1.mogrify(sql, params), 'SQL', '\t')
                            db_cursor1.execute(sql, params)
                                  
                self.unlock_object(work_directory)
    
                if not self.debug:
                    shutil.rmtree(work_directory)
                    
                result = True
                self.db_connection.commit()  
                logger.info('Dataset tiling completed - Transaction committed')
                return result
            except Exception, e:
                logger.error('Tiling operation failed: %s', e.message) # Keep on processing
                self.db_connection.rollback()
                if self.debug:
                    raise
            
            
        def process_scenes():                           
            db_cursor = self.db_connection.cursor()
            
            sql = """-- Find all scenes with L1T, fc and PQA level datasets with missing tiles
select * from (
    select distinct
      acquisition_id,
      fc.dataset_id as fc_dataset_id,
      fc.dataset_path as fc_dataset_path,
      fc.level_name as fc_level_name,
      fc.nodata_value as fc_nodata_value,
      fc.resampling_method as fc_resampling_method,
      fc.tile_count as fc_tile_count,
      satellite_tag,
      sensor_name,
      x_ref,
      y_ref,
      start_datetime,
      end_datetime,
      ll_lon,
      ll_lat,
      lr_lon,
      lr_lat,
      ul_lon,
      ul_lat,
      ur_lon,
      ur_lat,
      fc.crs,
      fc.ll_x,
      fc.ll_y,
      fc.lr_x,
      fc.lr_y,
      fc.ul_x,
      fc.ul_y,
      fc.ur_x,
      fc.ur_y,
      fc.x_pixels,
      fc.y_pixels,
      -- TODO: Use dataset_footprint table so that this will not break for projected tile types
      (
        ceil(greatest((lr_lon + 360.0)::numeric %% 360.0::numeric,
          (ur_lon + 360.0)::numeric %% 360.0::numeric) / 1.0)
        -
        floor(least((ll_lon + 360.0)::numeric %% 360.0::numeric,
          (ul_lon + 360.0)::numeric %% 360.0::numeric) / 1.0)
      )
      *
      (
        ceil(greatest(ul_lat, ur_lat) / 1.0)
        -
        floor(least(ll_lat, lr_lat) / 1.0)
      ) as tiles_required
    from acquisition
    inner join (
      select
        acquisition_id,
        d.dataset_id,
        level_name,
        dataset_path,
        nodata_value,
        resampling_method,
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
        y_pixels,
        count(tile_id) as tile_count
      from dataset d
      inner join processing_level using(level_id)
      left join tile t on t.dataset_id = d.dataset_id and tile_type_id = 1
        and ctime is not null -- *** TODO: Remove this line after reload ***
      where level_name = 'FC'
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
      ) fc using(acquisition_id)
    inner join satellite sa using(satellite_id)
    inner join sensor se using(satellite_id, sensor_id)
    where (%(start_date)s is null or start_datetime >= %(start_date)s)
      and (%(end_date)s is null or end_datetime < cast(%(end_date)s as date) + 1)
      and (%(min_path)s is null or x_ref >= %(min_path)s)
      and (%(max_path)s is null or x_ref <= %(max_path)s)
      and (%(min_row)s is null or y_ref >= %(min_row)s)
      and (%(max_row)s is null or y_ref <= %(max_row)s)
) datasets
where fc_tile_count < tiles_required
order by -- Order by path, row then descending date-times
  fc_tile_count,
  x_ref,
  y_ref,
  start_datetime desc,
  end_datetime desc,
  satellite_tag,
  sensor_name;
""" 
            params = {'tile_type_id': tile_type_id,
                      'start_date': start_date,
                      'end_date': end_date,
                      'min_path': min_path,
                      'max_path': max_path,
                      'min_row': min_row,
                      'max_row': max_row,
                      'tile_x_size': tile_type_info['x_size'],
                      'tile_y_size': tile_type_info['y_size']
                      }
            log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')
            
            # This mother of all queries creates a logjam at the DB server, so we only allow one instance a query at a time to submit it
            #TODO: Find a nicer way of dealing with this
            while not self.lock_object(os.path.basename(__file__) +' dataset query'):
                time.sleep(10)            
            try:
                db_cursor.execute(sql, params)
            finally:
                self.unlock_object(os.path.basename(__file__) +' dataset query')
            
            column_list = ['acquisition_id',
                          'fc_dataset_id',
                          'fc_dataset_path',
                          'fc_level_name',
                          'fc_nodata_value', 
                          'fc_resampling_method',               
                          'fc_tile_count',            
                          'satellite_tag', 
                          'sensor_name', 
                          'x_ref', 
                          'y_ref', 
                          'start_datetime', 
                          'end_datetime', 
                          'll_lon',
                          'll_lat',
                          'lr_lon',
                          'lr_lat',
                          'ul_lon',
                          'ul_lat',
                          'ur_lon',
                          'ur_lat',
                          'crs',
                          'll_x',
                          'll_y',
                          'lr_x',
                          'lr_y',
                          'ul_x',
                          'ul_y',
                          'ur_x',
                          'ur_y',
                          'x_pixels',
                          'y_pixels']
            
            for record in db_cursor:
                dataset_info = {}
                for column_index in range(len(column_list)):
                    dataset_info[column_list[column_index]] = record[column_index]

                # Ignore bad dataset and proceed to next one if not debugging
                if self.debug:                
                    process_dataset(dataset_info)
                else:
                    try:
                        process_dataset(dataset_info)
                    except Exception, e:
                        logger.warning(e.message)

        
        # Start of create_tiles function
        process_scenes()
#        create_composites()
                       
if __name__ == '__main__':
    landsat_tiler = LandsatTiler()
    
    #===========================================================================
    # # Sleep for a random number of seconds to avoid potential database lock-up with many instances starting up at the same time
    # # TODO: Find something better than this nasty work-around
    # if not landsat_tiler.debug:
    #    time.sleep(random.randint(0, 30)) 
    #===========================================================================
    
    landsat_tiler.create_tiles()

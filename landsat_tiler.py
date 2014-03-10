#!/usr/bin/env python
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
    
from ULA3.utils import log_multiline
from ULA3.utils import execute

from datacube import DataCube


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
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
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
  and (dataset_id = %(l1t_dataset_id)s
    or dataset_id = %(nbar_dataset_id)s
    or dataset_id = %(pqa_dataset_id)s)

  and ctime is not null -- TODO: Remove this after reload
;
"""
                params = {'x_index': x_index,
                      'y_index': y_index,
                      'tile_type_id': tile_type_info['tile_type_id'],
                      'l1t_dataset_id': dataset_info['l1t_dataset_id'],
                      'nbar_dataset_id': dataset_info['nbar_dataset_id'],
                      'pqa_dataset_id': dataset_info['pqa_dataset_id']}
                              
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
                sensor_dict = self.bands[tile_type_id][(dataset_info['satellite_tag'], dataset_info['sensor_name'])]
#                log_multiline(logger.debug, sensor, 'Sensor', '\t')
                for file_number in sorted(sensor_dict.keys()):
                    band_info = sensor_dict[file_number]
                    if band_info['level_name'] == 'NBAR':
                        dataset_dir = dataset_info['nbar_dataset_path']
                        dataset_id = dataset_info['nbar_dataset_id']
                        processing_level = dataset_info['nbar_level_name']
                        nodata_value = dataset_info['nbar_nodata_value']
                        resampling_method = dataset_info['nbar_resampling_method']
                    elif band_info['level_name'] == 'ORTHO':
                        dataset_dir = dataset_info['l1t_dataset_path']
                        dataset_id = dataset_info['l1t_dataset_id']
                        processing_level = dataset_info['l1t_level_name']
                        nodata_value = dataset_info['l1t_nodata_value']
                        resampling_method = dataset_info['l1t_resampling_method']
                    else:
                        continue # Ignore any pan-chromatic and derived bands
                    
                    dataset_dir = os.path.join(dataset_dir, 'scene01')
                    filename = find_file(dataset_dir, band_info['file_pattern'])
                    vrt_band_list.append({'file_number': band_info['file_number'], 
                                          'filename': filename, 
                                          'name': band_info['band_name'],
                                          'dataset_id': dataset_id,
                                          'band_id': band_info['band_id'],
                                          'processing_level': processing_level,
                                          'nodata_value': nodata_value,
                                          'resampling_method': resampling_method,
                                          'tile_layer': band_info['tile_layer']})
                    
                # Add Derived bands (only PQA at this stage)
                for band_level in ['PQA']:
                    derived_bands = self.bands[tile_type_id][('DERIVED', band_level)]
    #                log_multiline(logger.debug, derived_bands, 'derived_bands', '\t')
                    #TODO: Make this able to handle multiple layers
                    band_info = [band_info for band_info in derived_bands.values() 
                                 if band_info['level_name'] == band_level][0]
                    file_pattern = band_info['file_pattern']
                    dataset_dir = os.path.join(dataset_info['pqa_dataset_path'], 'scene01')
                    dataset_id = dataset_info['pqa_dataset_id']
                    filename = find_file(dataset_dir, file_pattern) 
                    processing_level = dataset_info['pqa_level_name']
                    nodata_value = dataset_info['pqa_nodata_value'] # Should be None for PQA
                    resampling_method = dataset_info['pqa_resampling_method']
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
                      
            # process_dataset function starts here
            result = False
            db_cursor1 = self.db_connection.cursor()
            
            logger.info('Processing dataset %s', dataset_info['nbar_dataset_path'])
            
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
            logger.info('Found %d tile records in database for %d tiles', tile_record_count, tile_count * 3) # Count ORTHO, NBAR & PQA
            if tile_record_count == tile_count * 3:
                logger.info('All tiles already exist in database - skipping tile creation for %s', dataset_info['nbar_dataset_path'])
                return result
            
            try:
                
                #TODO: Create all new acquisition records and commit the transaction here                
                
                # Use NBAR dataset name for dataset lock (could have been any other level)
                work_directory = os.path.join(self.temp_dir,
                                         os.path.basename(dataset_info['nbar_dataset_path'])
                                         )
                
                #TODO: Apply lock on path/row instead of on dataset to try to force the same node to process the full depth
                if not self.lock_object(work_directory):
                    logger.info('Already processing %s - skipping', dataset_info['nbar_dataset_path'])
                    return result
                
                if self.refresh and os.path.exists(work_directory):
                    shutil.rmtree(work_directory)
                
                self.create_directory(work_directory)
                
                tile_has_data = {}
                for processing_level in ['PQA', 'ORTHO', 'NBAR']: # N.B: PQA must be first
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
                                
                                result = execute(command_string=command_string)
                                
                                if result['stdout']:
                                    log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 
                            
                                if result['returncode']:
                                    log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')
                                    raise Exception('%s failed', command_string) 
                                
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
                            command_string = 'chmod -R 775 %s; chmod -R 777 %s' % (tile_output_dir, 
                                                                  os.path.join(tile_output_dir, 'mosaic_cache')
                                                                  )
                            
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
      y_max,
      bbox
      )
    select
      %(x_index)s, 
      %(y_index)s, 
      %(tile_type_id)s, 
      %(x_min)s, 
      %(y_min)s, 
      %(x_max)s, 
      %(y_max)s,
      ST_GeomFromText('POLYGON(%(x_min)s %(y_min)s,%(x_max)s %(y_min)s,%(x_max)s %(y_max)s,%(x_min)s %(y_max)s,%(x_min)s %(y_min)s)', 4326)
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
                if not self.debug:
                    raise
            
            
        def process_scenes():                           
            db_cursor = self.db_connection.cursor()
            
            sql = """-- Find all scenes with L1T, NBAR and PQA level datasets with missing tiles
select * from (
    select distinct
      acquisition_id,
      l1t.dataset_id as l1t_dataset_id,
      l1t.dataset_path as l1t_dataset_path,
      l1t.level_name as l1t_level_name,
      l1t.nodata_value as l1t_nodata_value,
      l1t.resampling_method as l1t_resampling_method,
      l1t.tile_count as l1t_tile_count,
      nbar.dataset_id as nbar_dataset_id,
      nbar.dataset_path as nbar_dataset_path,
      nbar.level_name as nbar_level_name,
      nbar.nodata_value as nbar_nodata_value,
      nbar.resampling_method as nbar_resampling_method,
      nbar.tile_count as nbar_tile_count,
      pqa.dataset_id as pqa_dataset_id,
      pqa.dataset_path as pqa_dataset_path,
      pqa.level_name as pqa_level_name,
      pqa.nodata_value as pqa_nodata_value,
      pqa.resampling_method as pqa_resampling_method,
      pqa.tile_count as pqa_tile_count,
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
      nbar.crs,
      nbar.ll_x,
      nbar.ll_y,
      nbar.lr_x,
      nbar.lr_y,
      nbar.ul_x,
      nbar.ul_y,
      nbar.ur_x,
      nbar.ur_y,
      nbar.x_pixels,
      nbar.y_pixels,
      -- TODO: Use dataset_footprint table so that this will not break for projected tile types
      (
        ceil(greatest((lr_lon + 360.0)::numeric %% 360.0::numeric, 
          (ur_lon + 360.0)::numeric %% 360.0::numeric) / %(tile_x_size)s)
        -
        floor(least((ll_lon + 360.0)::numeric %% 360.0::numeric,
          (ul_lon + 360.0)::numeric %% 360.0::numeric) / %(tile_x_size)s)
      )
      *
      (
        ceil(greatest(ul_lat, ur_lat) / %(tile_y_size)s) 
        -
        floor(least(ll_lat, lr_lat) / %(tile_y_size)s)
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
        count(tile_id) as tile_count
      from dataset d
      inner join processing_level using(level_id)
      left join tile t on t.dataset_id = d.dataset_id and tile_type_id = 1
        and ctime is not null -- *** TODO: Remove this line after reload ***
      where level_name = 'ORTHO'
      group by 1,2,3,4,5,6
      ) l1t using(acquisition_id)
    inner join (
      select
        acquisition_id,
        d.dataset_id,
        level_name,
        dataset_path,
        nodata_value,
        resampling_method,
        -- Grab extra info from NBAR dataset - should be the same as in L1T & PQA datasets
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
      where level_name = 'NBAR'
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
      ) nbar using(acquisition_id)
    inner join (
      select
        acquisition_id,
        d.dataset_id,
        level_name,
        dataset_path,
        nodata_value,
        resampling_method,
        count(tile_id) as tile_count
      from dataset d
      inner join processing_level using(level_id)
      left join tile t on t.dataset_id = d.dataset_id and tile_type_id = 1
        and ctime is not null -- *** TODO: Remove this line after reload ***
      where level_name = 'PQA'
      group by 1,2,3,4,5,6
      ) pqa using(acquisition_id)
    inner join satellite sa using(satellite_id)
    inner join sensor se using(satellite_id, sensor_id)
    where (%(start_date)s is null or start_datetime >= %(start_date)s)
      and (%(end_date)s is null or end_datetime < cast(%(end_date)s as date) + 1)
      and (%(min_path)s is null or x_ref >= %(min_path)s)
      and (%(max_path)s is null or x_ref <= %(max_path)s)
      and (%(min_row)s is null or y_ref >= %(min_row)s)
      and (%(max_row)s is null or y_ref <= %(max_row)s)
      and (cloud_cover is null or cloud_cover < 98) -- Arbitrary threshold above which scene should be ignored
) datasets
where l1t_tile_count < tiles_required
  or nbar_tile_count < tiles_required
  or pqa_tile_count < tiles_required
order by -- Order by path, row then descending date-times
  l1t_tile_count + nbar_tile_count + pqa_tile_count,
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
            while not self.lock_object(os.path.basename(__file__) + ' dataset query'):
                time.sleep(10)            
            try:
                db_cursor.execute(sql, params)
            finally:
                self.unlock_object(os.path.basename(__file__) +' dataset query')
            
            column_list = ['acquisition_id',
                          'l1t_dataset_id', 
                          'l1t_dataset_path',
                          'l1t_level_name',
                          'l1t_nodata_value',   
                          'l1t_resampling_method', 
                          'l1t_tile_count',            
                          'nbar_dataset_id',
                          'nbar_dataset_path',
                          'nbar_level_name',
                          'nbar_nodata_value', 
                          'nbar_resampling_method',               
                          'nbar_tile_count',            
                          'pqa_dataset_id',
                          'pqa_dataset_path',
                          'pqa_level_name',
                          'pqa_nodata_value', 
                          'pqa_resampling_method',               
                          'pqa_tile_count',            
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

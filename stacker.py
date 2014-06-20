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
from osgeo import gdal, osr, gdalconst
from copy import copy
from datetime import datetime, time, timedelta
from scipy import ndimage
import numpy
import numpy.ma as ma
import shutil
from time import sleep

from ULA3.utils import execute
from ULA3.utils import log_multiline

from datacube import DataCube

PQA_NO_DATA_VALUE = 16127 # All ones except for contiguity (bit 8)

PQA_NO_DATA_BITMASK = 0x01FF
# Contiguity and band saturation bits all one, others zero.

PQA_NO_DATA_CHECK_VALUE = 0x00FF
# Contiguity bit zero, band saturation bits all one.
# For use with PQA data masked with the PQA_NO_DATA_BITMASK

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class Stacker(DataCube):

    def parse_args(self):
        """Parse the command line arguments.
    
        Returns:
            argparse namespace object
        """
        logger.debug('  Calling parse_args()')
    
        _arg_parser = argparse.ArgumentParser('stacker')
        
        # N.B: modtran_root is a direct overrides of config entries
        # and its variable name must be prefixed with "_" to allow lookup in conf file
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
            help='Stacker configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('-x', '--x_index', dest='x_index',
            required=False, default=None,
            help='x-index of tile to be stacked')
        _arg_parser.add_argument('-y', '--y_index', dest='y_index',
            required=False, default=None,
            help='y-index of tile to be stacked')
        _arg_parser.add_argument('-o', '--output', dest='output_dir',
            required=False, default=1,
            help='Output directory path')
        _arg_parser.add_argument('-s', '--start_date', dest='start_date',
            required=False, default=None,
            help='Start Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-e', '--end_date', dest='end_date',
            required=False, default=None,
            help='End Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-a', '--satellite', dest='satellite',
            required=False, default=None,
            help='Short Satellite name (e.g. LS5, LS7)')
        _arg_parser.add_argument('-n', '--sensor', dest='sensor',
            required=False, default=None,
            help='Sensor Name (e.g. TM, ETM+)')
        _arg_parser.add_argument('-t', '--tile_type', dest='default_tile_type_id',
            required=False, default=1,
            help='Tile type ID of tiles to be stacked')
        _arg_parser.add_argument('-p', '--path', dest='path',
            required=False, default=None,
            help='WRS path of tiles to be stacked')
        _arg_parser.add_argument('-r', '--row', dest='row',
            required=False, default=None,
            help='WRS row of tiles to be stacked')
        _arg_parser.add_argument('--refresh', dest='refresh',
           default=False, action='store_const', const=True,
           help='Refresh mode flag to force updating of existing files')
        _arg_parser.add_argument('-of', '--out_format', dest='out_format',
            required=False, default=None,
            help='Specify a GDAL complient output format for the file to be physically generated. If unset, then only the VRT will be generated. Example use -of ENVI')
        _arg_parser.add_argument('-sfx', '--suffix', dest='suffix',
            required=False, default=None,
            help='Specify an output suffix for the physically generated file. Is only applied when -of <FORMAT> is set.')
    
        return _arg_parser.parse_args()
        
    def __init__(self, source_datacube=None, default_tile_type_id=1):
        """Constructor
        Arguments:
            source_datacube: Optional DataCube object whose connection and data will be shared
            tile_type_id: Optional tile_type_id value (defaults to 1)
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
            DataCube.__init__(self) # Call inherited constructor
            
        if self.debug:
            console_handler.setLevel(logging.DEBUG)
            
        # Attempt to parse dates from command line arguments or config file
        try:
            self.default_tile_type_id = int(self.default_tile_type_id) 
        except:
            self.default_tile_type_id = default_tile_type_id # Use function argument value if not in command-line args
        try:
            self.start_date = datetime.strptime(self.start_date, '%Y%m%d').date()
        except:
            try:
                self.start_date = datetime.strptime(self.start_date, '%d/%m/%Y').date()
            except:
                self.start_date = None            
        try:
            self.end_date = datetime.strptime(self.end_date, '%Y%m%d').date()
        except:
            try:
                self.end_date = datetime.strptime(self.end_date, '%d/%m/%Y').date()
            except:
                self.end_date= None            
        try:
            self.x_index = int(self.x_index) 
        except:
            self.x_index = None
        try:
            self.y_index = int(self.y_index) 
        except:
            self.y_index = None

        # Path/Row values to permit single-scene stacking
        try:
            self.path = int(self.path) 
        except:
            self.path = None
        try:
            self.row = int(self.row) 
        except:
            self.row = None
            
        # Other variables set from config file only - not used
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

            
    def stack_files(self, timeslice_info_list, stack_dataset_path, band1_vrt_path=None, overwrite=False):
        if os.path.exists(stack_dataset_path) and not overwrite:
            logger.debug('Stack VRT file %s already exists', stack_dataset_path)
            return
        
        band_no = timeslice_info_list[0]['tile_layer'] # Should all be the same
        build_vrt = True
        if band_no == 1: # First band
            intermediate_path = stack_dataset_path # No post-processing required
        elif band1_vrt_path: # band1_vrt_path provided - use this as source for new VRT
            intermediate_path = band1_vrt_path
            build_vrt = False
        else: # No band1_vrt_path provided
            intermediate_path = re.sub('\.vrt$', '.tmp', stack_dataset_path)
        
        file_list_path = re.sub('\.vrt$', '.txt', stack_dataset_path)
        if build_vrt:
            logger.info('Creating %d layer stack VRT file %s', len(timeslice_info_list), stack_dataset_path)
            list_file = open(file_list_path, 'w')
            list_file.write('\n'.join([timeslice_info['tile_pathname'] for timeslice_info in timeslice_info_list]))
            list_file.close()
            del list_file

            command_string = 'gdalbuildvrt'
            if not self.debug:
                command_string += ' -q'
#            command_string += ' -separate -overwrite %s \\\n%s' % (
#                intermediate_path,
#                ' \\\n'.join([timeslice_info['tile_pathname'] for timeslice_info in timeslice_info_list])
#                )
            command_string += ' -separate -input_file_list %s -overwrite %s' % (
                file_list_path,
                intermediate_path
                )
            if not self.debug:
                command_string += '\nrm %s' % file_list_path
        else:
            command_string = ''
        
        if band_no > 1: # Need to post process intermediate VRT file
            if command_string:
                command_string += '\n'
            command_string += 'cat %s | sed s/\<SourceBand\>1\<\\\\/SourceBand\>/\<SourceBand\>%d\<\\\\/SourceBand\>/g > %s' % (intermediate_path, band_no, stack_dataset_path)
#                command_string += '\nchmod 777 %s' % stack_dataset_path
            if build_vrt: # Intermediate file created for band > 1
                if not self.debug: # Remove temporary intermediate file just created
                    command_string += '\nrm %s' % intermediate_path
            else:
                logger.info('Creating %d layer stack VRT file %s from %s', len(timeslice_info_list), stack_dataset_path, intermediate_path)

        
        logger.debug('command_string = %s', command_string)
    
        result = execute(command_string=command_string)
    
        if result['stdout']:
            log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 

        if result['stderr']:
            log_multiline(logger.debug, result['stderr'], 'stderr from ' + command_string, '\t')
            
        if result['returncode']:
            raise Exception('%s failed', command_string) 
                  
        temporal_stack_dataset = gdal.Open(stack_dataset_path)
        assert temporal_stack_dataset, 'Unable to open VRT %s' % stack_dataset_path

        for band_index in range(len(timeslice_info_list)):
            band = temporal_stack_dataset.GetRasterBand(band_index + 1)
            
            # Copy dict and convert to strings for metadata
            metadata_dict = dict(timeslice_info_list[band_index])
            for key in metadata_dict.keys():
                metadata_dict[key] = str(metadata_dict[key])
                
            band.SetMetadata(metadata_dict)
            log_multiline(logger.debug, band.GetMetadata(), 'band.GetMetadata()', '\t')
            
            # Need to set nodata values for each band - can't seem to do it in gdalbuildvrt
            nodata_value = timeslice_info_list[band_index]['nodata_value']
            if nodata_value is not None:
                logger.debug('nodata_value = %s', nodata_value)
                band.SetNoDataValue(nodata_value)
            
        temporal_stack_dataset.FlushCache()

 
    def stack_tile(self, x_index, y_index, stack_output_dir=None, 
                   start_datetime=None, end_datetime=None, 
                   satellite=None, sensor=None, 
                   tile_type_id=None, 
                   path=None, 
                   row=None, 
                   create_band_stacks=True,
                   disregard_incomplete_data=False):
        """
        Function which returns a data structure and optionally creates band-wise VRT dataset stacks
        
        Arguments:
            x_index, y_index: Integer indices of tile to stack
            stack_output_dir: String defining output directory for band stacks 
                (not used if create_band_stacks == False)
            start_datetime, end_datetime: Optional datetime objects delineating temporal range
            satellite, sensor: Optional satellite and sensor string parameters to filter result set
            tile_type_id: Integer value of tile_type_id to search 
            path: WRS path of source scenes
            row: WRS row of source scenes
            create_band_stacks: Boolean flag indicating whether band stack VRT files should be produced
            disregard_incomplete_data: Boolean flag indicating whether to constrain results to tiles with
                complete L1T, NBAR and PQA data. This ensures identical numbers of stack layers but
                introduces a hard-coded constraint around processing levels.
        """
        
        assert stack_output_dir or not create_band_stacks, 'Output directory must be supplied for temporal stack generation'
        tile_type_id = tile_type_id or self.default_tile_type_id

        #
        # stack_tile local functions
        #

        def cache_mosaic_files(mosaic_file_list, mosaic_dataset_path, overwrite=False, pqa_data=False):
            logger.debug('cache_mosaic_files(mosaic_file_list=%s, mosaic_dataset_path=%s, overwrite=%s, pqa_data=%s) called', mosaic_file_list, mosaic_dataset_path, overwrite, pqa_data)
            
            if pqa_data: # Need to handle PQA datasets manually and produce a real output file
                # Change the output file extension to match the source (This is a bit ugly)
                mosaic_dataset_path = re.sub('\.\w+$', 
                                             re.search('\.\w+$', mosaic_file_list[0]).group(0), 
                                             mosaic_dataset_path)
                
                if os.path.exists(mosaic_dataset_path) and not overwrite:
                    logger.debug('Mosaic file %s already exists', mosaic_dataset_path)
                    return mosaic_dataset_path
            
                logger.info('Creating PQA mosaic file %s', mosaic_dataset_path)
                assert self.lock_object(mosaic_dataset_path), 'Unable to acquire lock for %s' % mosaic_dataset_path
            
                # Copy first source file to destination - will be same size and datatype
                shutil.copy(mosaic_file_list[0], mosaic_dataset_path)
                mosaic_dataset = gdal.Open(mosaic_dataset_path, gdalconst.GA_Update)
                assert mosaic_dataset, 'Unable to copy %s to %s' % (mosaic_file_list[0], mosaic_dataset_path)
                output_band = mosaic_dataset.GetRasterBand(1)
                data_array = output_band.ReadAsArray()
                data_array[...] = -1 # Set all background values to FFFF (i.e. all ones)
                
                overall_data_mask = numpy.zeros((mosaic_dataset.RasterXSize, 
                                               mosaic_dataset.RasterYSize), 
                                              dtype=numpy.bool)
                
                # Populate data_array with -masked PQA data
                for pqa_dataset_index in range(len(mosaic_file_list)):
                    pqa_dataset_path = mosaic_file_list[pqa_dataset_index]
                    pqa_dataset = gdal.Open(pqa_dataset_path)
                    assert pqa_dataset, 'Unable to open %s' % pqa_dataset_path
                    pqa_array = pqa_dataset.ReadAsArray()
                    del pqa_dataset
                    logger.debug('Opened %s', pqa_dataset_path)
                    
                    # Set all data-containing pixels to true in data_mask
                    pqa_bitmasked = pqa_array & PQA_NO_DATA_BITMASK
                    pqa_data_mask = ((pqa_bitmasked != PQA_NO_DATA_CHECK_VALUE) &
                                     (pqa_bitmasked != 0))
                    # pqa_data_mask = (pqa_array != PQA_NO_DATA_VALUE) & (pqa_array != 0)
                    overall_data_mask = overall_data_mask | pqa_data_mask # Update overall_data_mask to true for all data-containing pixels
                    data_array[pqa_data_mask] = numpy.bitwise_and(data_array[pqa_data_mask], (pqa_array[pqa_data_mask])) # Set bits which are true in all source arrays
                    
                    #===========================================================
                    # log_multiline(logger.debug, pqa_array, 'pqa_array', '\t')
                    # log_multiline(logger.debug, pqa_data_mask, 'pqa_data_mask', '\t')
                    # log_multiline(logger.debug, overall_data_mask, 'overall_data_mask', '\t')
                    # log_multiline(logger.debug, data_array, 'data_array', '\t')
                    #===========================================================

                data_array[~overall_data_mask] = PQA_NO_DATA_VALUE # Set all pixels which don't contain data to PQA_NO_DATA_VALUE
#                log_multiline(logger.debug, data_array, 'data_array', '\t')
                
                output_band.WriteArray(data_array)  
                mosaic_dataset.FlushCache()  
            
            else: # Anything other than PQA
                if os.path.exists(mosaic_dataset_path) and not overwrite:
                    logger.debug('Mosaic VRT file %s already exists', mosaic_dataset_path)
                    return mosaic_dataset_path
            
                logger.info('Creating mosaic VRT file %s', mosaic_dataset_path)
                assert self.lock_object(mosaic_dataset_path), 'Unable to acquire lock for %s' % mosaic_dataset_path

                command_string = 'gdalbuildvrt'
                if not self.debug:
                    command_string += ' -q'
                command_string += ' -overwrite %s \\\n%s' % (
                    mosaic_dataset_path,
                    ' \\\n'.join(mosaic_file_list)
                    )
                command_string += '\nchmod 777 %s' % mosaic_dataset_path
                
                logger.debug('command_string = %s', command_string)
            
                result = execute(command_string=command_string)
            
                if result['stdout']:
                    log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 
        
                if result['stderr']:
                    log_multiline(logger.debug, result['stderr'], 'stderr from ' + command_string, '\t')
                    
                if result['returncode']:
                    raise Exception('%s failed', command_string) 
            
            # Check for corrupted file and remove it
            try:
                assert gdal.Open(mosaic_dataset_path), 'Unable to open mosaic dataset %s. Attempting to remove it.' % mosaic_dataset_path
            except:
                self.remove(mosaic_dataset_path) 
                raise
                    
            self.unlock_object(mosaic_dataset_path)    
              
            return mosaic_dataset_path # Return potentially modified filename 
        
        def create_mosaic_dir(mosaic_dir):
            command_string = 'mkdir -p %s' % mosaic_dir
            command_string += '\nchmod 777 %s' % mosaic_dir

            logger.debug('command_string = %s', command_string)

            result = execute(command_string=command_string)

            if result['stdout']:
                log_multiline(logger.debug, result['stdout'], 'stdout from ' + command_string, '\t') 

            if result['returncode']:
                log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')
                raise Exception('%s failed', command_string) 

        def record_timeslice_information(timeslice_info, mosaic_file_list, stack_dict):

            if len(mosaic_file_list) > 1: # Mosaic required - cache under tile directory
                mosaic_dir = os.path.join(os.path.dirname(timeslice_info['tile_pathname']),
                                                          'mosaic_cache')
                if not os.path.isdir(mosaic_dir):
                    create_mosaic_dir(mosaic_dir)

                timeslice_info['tile_pathname'] = os.path.join(
                    mosaic_dir,
                    re.sub(r'\.\w+$', '.vrt', os.path.basename(timeslice_info['tile_pathname']))
                    )

                # N.B: cache_mosaic_files function may modify filename
                timeslice_info['tile_pathname'] = \
                    cache_mosaic_files(mosaic_file_list, timeslice_info['tile_pathname'],
                                       overwrite=self.refresh, pqa_data=(timeslice_info['level_name'] == 'PQA'))

            stack_dict[timeslice_info['start_datetime']] = timeslice_info
        
        #
        # stack_tile method body         
        #

        db_cursor2 = self.db_connection.cursor()
        
        sql = """-- Retrieve all tile and band details for specified tile range
select distinct
  band_tag, 
  band_name, 
  start_datetime, 
  end_datetime, 
  satellite_tag, 
  sensor_name, 
  t.tile_pathname, 
  tile_layer,
  x_ref as path,
  y_ref as row,
  level_name,
  nodata_value,
  gcp_count,
  cloud_cover
from tile_footprint tf
inner join tile t using (x_index, y_index, tile_type_id)
inner join dataset using(dataset_id)
inner join acquisition using(acquisition_id)
inner join satellite using(satellite_id)
inner join sensor using(satellite_id, sensor_id)
inner join processing_level using(level_id)
inner join band_source using(tile_type_id, level_id)
inner join band using(band_id) -- Don't exclude bands with null satellite/sensor"""
        if disregard_incomplete_data:
            sql += """
inner join dataset l1t_dataset using(acquisition_id)
inner join dataset nbar_dataset using(acquisition_id)
inner join dataset pqa_dataset using(acquisition_id)
inner join tile l1t_tile using (x_index, y_index, tile_type_id)
inner join tile nbar_tile using (x_index, y_index, tile_type_id)
inner join tile pqa_tile using (x_index, y_index, tile_type_id)"""
        sql += """
where tile_type_id = %(tile_type_id)s
  and t.tile_class_id = 1 -- Select only valid tiles"""
        if disregard_incomplete_data:
            sql += """
  and l1t_dataset.level_id = 1
  and nbar_dataset.level_id = 2
  and pqa_dataset.level_id = 3
  and l1t_tile.dataset_id = l1t_dataset.dataset_id
  and nbar_tile.dataset_id = nbar_dataset.dataset_id
  and pqa_tile.dataset_id = pqa_dataset.dataset_id"""
        sql += """
  and (band.satellite_id is null or band.satellite_id = sensor.satellite_id)
  and (band.sensor_id is null or band.sensor_id = sensor.sensor_id)
  and x_index = %(x_index)s
  and y_index = %(y_index)s
  and (%(start_datetime)s is null or start_datetime >= %(start_datetime)s)
  and (%(end_datetime)s is null or end_datetime < %(end_datetime)s)
  and (%(satellite)s is null or satellite_tag = %(satellite)s)
  and (%(sensor)s is null or sensor_name = %(sensor)s)
  and (%(x_ref)s is null or x_ref = %(x_ref)s)
  and (%(y_ref)s is null or y_ref = %(y_ref)s)
order by
  band_tag, 
  start_datetime, 
  end_datetime, 
  satellite_tag, 
  sensor_name;
"""
        params = {'x_index': x_index,
              'y_index': y_index,
              'tile_type_id': tile_type_id,
              'start_datetime': start_datetime,
              'end_datetime': end_datetime,
              'satellite': satellite,
              'sensor': sensor,
              'x_ref': path,
              'y_ref': row
              }
                      
        log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
        db_cursor2.execute(sql, params)
        
        band_stack_dict = {}
        stack_dict = {}
        mosaic_file_list = []
        last_band_tile_info = None
        timeslice_info = None
        start_datetime = None
        end_datetime = None
        
        for record in db_cursor2:   
            assert record, 'No data found for this tile and temporal range'      
            band_tile_info = {'x_index': x_index,
                'y_index': y_index,            
                'band_tag': record[0], 
                'band_name': record[1], 
                'start_datetime': record[2], 
                'end_datetime': record[3], 
                'satellite_tag': record[4],
                'sensor_name': record[5], 
                'tile_pathname': record[6],
                'tile_layer': record[7], 
                'path': record[8],
                'start_row': record[9], 
                'end_row': record[9], # Copy of row field
                'level_name': record[10],
                'nodata_value': record[11],
                'gcp_count': record[12],
                'cloud_cover': record[13] 
                }
#            log_multiline(logger.debug, band_tile_info, 'band_tile_info', '\t')
            
            assert os.path.exists(band_tile_info['tile_pathname']), 'File for tile %s does not exist' % band_tile_info['tile_pathname']
            
            # If this tile is NOT a continuation of the last one
            if (not last_band_tile_info # First tile
                or (band_tile_info['band_tag'] != last_band_tile_info['band_tag'])
                or (band_tile_info['satellite_tag'] != last_band_tile_info['satellite_tag'])
                or (band_tile_info['sensor_name'] != last_band_tile_info['sensor_name'])
                or (band_tile_info['path'] != last_band_tile_info['path'])
                or ((band_tile_info['start_datetime'] - last_band_tile_info['end_datetime']) > timedelta(0, 3600)) # time difference > 1hr
                ):
                # Record timeslice information for previous timeslice if it exists
                if timeslice_info:
                    record_timeslice_information(timeslice_info, mosaic_file_list, stack_dict)
                
                # Start recording a new band if necessary
                if (not last_band_tile_info or (band_tile_info['band_tag'] != last_band_tile_info['band_tag'])):                    
                    stack_dict = {}
                    level_dict = band_stack_dict.get(band_tile_info['level_name']) or {}
                    if not level_dict:
                        band_stack_dict[band_tile_info['level_name']] = level_dict
                        
                    level_dict[band_tile_info['band_tag']] = stack_dict
                
                # Start a new timeslice
                mosaic_file_list = [band_tile_info['tile_pathname']]
                timeslice_info = band_tile_info
            else: # Tile IS a continuation of the last one - same timeslice
                mosaic_file_list.append(band_tile_info['tile_pathname'])
                timeslice_info['end_datetime'] = band_tile_info['end_datetime']
                timeslice_info['end_row'] = band_tile_info['end_row']
                            
            last_band_tile_info = band_tile_info
            
        # Check for no results, otherwise record the last timeslice
        if not timeslice_info:
            return {}
        else:
            record_timeslice_information(timeslice_info, mosaic_file_list, stack_dict)

        log_multiline(logger.debug, band_stack_dict, 'band_stack_dict', '\t')
        
        if (stack_output_dir):
            self.create_directory(stack_output_dir)
        
        # Create stack files for each band
        stack_info_dict = {}
        if create_band_stacks:
            for level_name in sorted(band_stack_dict.keys()):
                band1_stack_filename = None
                for band_tag in sorted(band_stack_dict[level_name].keys()):
                    stack_dict = band_stack_dict[level_name][band_tag]
                    timeslice_info_list = []
                    stack_filename = None
                    for start_time in sorted(stack_dict.keys()):
                        timeslice_info_list.append(stack_dict[start_time])
                   
                    stack_filename = stack_filename or os.path.join(stack_output_dir,
                                                                    '_'.join([timeslice_info_list[0]['level_name'],
                                                                              re.sub('\+', '', '%+04d_%+04d' % (x_index, y_index)),
                                                                              timeslice_info_list[0]['band_tag']]) + '.vrt')
        
                    self.stack_files(timeslice_info_list, stack_filename, band1_stack_filename, overwrite=True)
                    
                    # Convert the virtual file to a physical file, ie VRT to ENVI
                    if self.out_format:
                        #if (len(self.suffix) > 0):
                        if self.suffix:
                            outfname = re.sub('.vrt', '.%s'%self.suffix, stack_filename)
                        else:
                            # Strip the suffix of the existing file name and output to disk
                            outfname = os.path.splitext(stack_filename)[0]

                        # Base commandline string
                        command_string = 'gdal_translate -of %s %s %s' %(self.out_format, stack_filename, outfname)
                    
                        logger.debug('command_string = %s', command_string)

                        print 'Creating Physical File'
                        print 'File Format: %s' %self.out_format
                        print 'Filename: %s' %outfname

                        result = execute(command_string=command_string)

                        if result['stdout']:
                            log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t')

                        if result['stderr']:
                            log_multiline(logger.debug, result['stderr'], 'stderr from ' + command_string, '\t')

                        if result['returncode']:
                            raise Exception('%s failed', command_string)
                        if timeslice_info_list[0]['tile_layer'] == 1:
                            band1_stack_filename = stack_filename
                    
        #            log_multiline(logger.info, timeslice_info_list, 'stack_info_dict[%s]' % stack_filename, '\t')
                    stack_info_dict[stack_filename] = timeslice_info_list
                    
        else: # Don't create stacks - just return a dict of tiles for each level   
            for level_name in sorted(band_stack_dict.keys()):
                for band_tag in sorted(band_stack_dict[level_name].keys()):
                    # Only look at bands at layer 1 - all other bands share the same file
                    if band_stack_dict[level_name][band_tag].values()[0]['tile_layer'] == 1: # N.B: All the same layer
                        stack_dict = stack_info_dict.get(level_name, {})
                        if not stack_dict: # New dict keyed by start_datetime
                            stack_info_dict[level_name] = stack_dict
                        stack_dict.update(band_stack_dict[level_name][band_tag]) # Merge band 1 dict into layer dict

                                           
        return stack_info_dict
    
    def get_pqa_mask(self, pqa_dataset_path, good_pixel_masks=[32767,16383,2457], dilation=3):
        pqa_gdal_dataset = gdal.Open(pqa_dataset_path)
        assert pqa_gdal_dataset, 'Unable to open PQA GeoTIFF file %s' % pqa_dataset_path
        pqa_array = pqa_gdal_dataset.GetRasterBand(1).ReadAsArray()
        del pqa_gdal_dataset
        
        # Ignore bit 8 - always 0 for Landsat 5
        pqa_array = pqa_array | 64
        
    #    logger.debug('pqa_array = %s', pqa_array)
                    
        # Dilating both the cloud and cloud shadow masks 
        s = [[1,1,1],[1,1,1],[1,1,1]]
        acca = (pqa_array & 1024) >> 10
        erode = ndimage.binary_erosion(acca, s, iterations=dilation, border_value=1)
        dif = erode - acca
        dif[dif < 0] = 1
        pqa_array += (dif << 10)
        del acca
        fmask = (pqa_array & 2048) >> 11
        erode = ndimage.binary_erosion(fmask, s, iterations=dilation, border_value=1)
        dif = erode - fmask
        dif[dif < 0] = 1
        pqa_array += (dif << 11)
        del fmask
        acca_shad = (pqa_array & 4096) >> 12
        erode = ndimage.binary_erosion(acca_shad, s, iterations=dilation, border_value=1)
        dif = erode - acca_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 12)
        del acca_shad
        fmask_shad = (pqa_array & 8192) >> 13
        erode = ndimage.binary_erosion(fmask_shad, s, iterations=dilation, border_value=1)
        dif = erode - fmask_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 13)
        
        #=======================================================================
        # pqa_mask = ma.getmask(ma.masked_equal(pqa_array, int(good_pixel_masks[0])))
        # for good_pixel_mask in good_pixel_masks[1:]:
        #    pqa_mask = ma.mask_or(pqa_mask, ma.getmask(ma.masked_equal(pqa_array, int(good_pixel_mask))))
        #=======================================================================
        pqa_mask = numpy.zeros(pqa_array.shape, dtype=numpy.bool)
        for good_pixel_mask in good_pixel_masks:
            pqa_mask[pqa_array == good_pixel_mask] = True
            
        return pqa_mask
        
    def apply_pqa_mask(self, data_array, pqa_mask, no_data_value):
        assert len(data_array.shape) == 2, 'apply_pqa_mask can only be applied to 2D arrays'
        assert data_array.shape == pqa_mask.shape, 'Mis-matched data_array and pqa_mask'        
        data_array[~pqa_mask] = no_data_value
        
        
    def get_static_info(self, level_name=None, x_index=None, y_index=None, tile_type_id=None):
        """Retrieve static (i.e. not time varying) data for specified processing level(s) (e.g. 'DSM')""" 
        x_index = x_index or self.x_index
        y_index = y_index or self.y_index
        tile_type_id = tile_type_id or self.default_tile_type_id
        
        db_cursor2 = self.db_connection.cursor()
                
        sql = """-- Retrieve all tile details for static data
select distinct
  level_name,
  dataset_path,
  tile_pathname
from dataset
inner join processing_level using(level_id)
inner join tile t using (dataset_id)
inner join tile_footprint tf using (x_index, y_index, tile_type_id)
where tile_type_id = %(tile_type_id)s
  and tile_class_id = 1 -- Select only valid tiles
  and (%(level_name)s is null or level_name = %(level_name)s)
  and x_index = %(x_index)s
  and y_index = %(y_index)s
  and acquisition_id is null -- No acquisition for static data
order by
  level_name, 
  dataset_path;
"""
        params = {'level_name': level_name,
                  'x_index': x_index,
                  'y_index': y_index,
                  'tile_type_id': tile_type_id,
                  }
                      
        log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
        db_cursor2.execute(sql, params)
        
        static_info_dict = {}
        last_level_name = ''
        for record in db_cursor2:   
            static_data = {'level_name': record[0],
                      'dataset_path': record[1],
                      'tile_pathname': record[2]
                      }
            
            assert static_data['level_name'] != last_level_name, 'Duplicate data source found for level %s' % static_data['level_name']
            
            band_info = self.bands[tile_type_id].get(('DERIVED', static_data['level_name']))
                        
            static_info_dict[static_data['level_name']] = {
                                                           'level_name': static_data['level_name'],
                                                           'nodata_value': band_info.values()[0]['nodata_value'], # All values the same for the one level
                                                           'tile_pathname': static_data['tile_pathname'],
                                                           'x_index': x_index,
                                                           'y_index': y_index
                                                           #====================
                                                           # 'band_name': None,
                                                           # 'band_tag': None,
                                                           # 'end_datetime': None,
                                                           # 'end_row': None,
                                                           # 'path': None,
                                                           # 'satellite_tag': None,
                                                           # 'sensor_name': None,
                                                           # 'start_datetime': None,
                                                           # 'start_row': None,
                                                           # 'tile_layer': 1,
                                                           # 'gcp_count': None,
                                                           # 'cloud_cover': None
                                                           #====================
                                                           }        
            
        return static_info_dict   
        
        
    def stack_derived(self, x_index, y_index, stack_output_dir,
                      start_datetime=None, end_datetime=None, 
                      satellite=None, sensor=None,
                      tile_type_id=None,
                      create_stacks=True):
        
        tile_type_id = tile_type_id or self.default_tile_type_id
        tile_type_info = self.tile_type_dict[tile_type_id]
        
        stack_output_info = {'x_index': x_index, 
                      'y_index': y_index,
                      'stack_output_dir': stack_output_dir,
                      'start_datetime': start_datetime, 
                      'end_datetime': end_datetime, 
                      'satellite': satellite, 
                      'sensor': sensor}
        
        # Create intermediate mosaics and return dict with stack info
        stack_info_dict = self.stack_tile(x_index=x_index, 
                                         y_index=y_index, 
                                         stack_output_dir=stack_output_dir, 
                                         start_datetime=start_datetime, 
                                         end_datetime=end_datetime, 
                                         satellite=satellite, 
                                         sensor=sensor, 
                                         tile_type_id=None,
                                         create_band_stacks=False,
                                         disregard_incomplete_data=True)
        
        # Create intermediate mosaics and return dict with stack info
        logger.debug('self.stack_tile(x_index=%s, y_index=%s, stack_output_dir=%s, start_datetime=%s, end_datetime=%s, satellite=%s, sensor=%s, tile_type_id=%s, create_band_stacks=%s, disregard_incomplete_data=%s) called', 
                                         x_index, y_index, 
                                         stack_output_dir, 
                                         start_datetime, 
                                         end_datetime, 
                                         satellite, 
                                         sensor, 
                                         None,
                                         False,
                                         True)
    
        log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
        
        static_info_dict = self.get_static_info(level_name='DSM')
        log_multiline(logger.debug, static_info_dict, 'static_info_dict', '\t')
        
        processing_levels = sorted(stack_info_dict.keys())
        
        # Find all datetimes
        start_datetime_set = set()
        for processing_level in processing_levels:
            for start_datetime in stack_info_dict[processing_level].keys():
                start_datetime_set.add(start_datetime)
        start_datetimes = sorted(start_datetime_set)
        del start_datetime_set

        # Iterate through sorted start_datetimes
        derived_stack_dict = {}
        for start_datetime in start_datetimes:
            # Create input_dataset_dict dict for deriver_function
            input_dataset_dict = {}
            for processing_level in processing_levels:
                input_dataset_dict[processing_level] = stack_info_dict[processing_level].get(start_datetime)
                
            input_dataset_dict.update(static_info_dict) # Add static data to dict passed to function
            
            # Create derived datasets and receive name(s) of timeslice file(s) keyed by stack file name(s)
            output_dataset_info = self.derive_datasets(input_dataset_dict, stack_output_info, tile_type_info) 
            
            if output_dataset_info is not None:
                for output_stack_path in output_dataset_info:
                    # Create a new list for each stack if it doesn't already exist
                    stack_list = derived_stack_dict.get(output_stack_path, [])
                    if not stack_list:
                        derived_stack_dict[output_stack_path] = stack_list
                        
                    stack_list.append(output_dataset_info[output_stack_path])
               
        log_multiline(logger.debug, derived_stack_dict, 'derived_stack_dict', '\t')
        
        # Individual tile processing is finished. now build stack(s)
        if create_stacks:
            for output_stack_path in sorted(derived_stack_dict.keys()):
                if os.path.exists(output_stack_path) and not self.refresh:
                    logger.info('Skipped existing stack file %s', output_stack_path)
                    continue
                
                if (self.lock_object(output_stack_path)):
                    logger.debug('Creating temporal stack %s', output_stack_path)
                    self.stack_files(timeslice_info_list=derived_stack_dict[output_stack_path], 
                                 stack_dataset_path=output_stack_path, 
                                 band1_vrt_path=None, overwrite=True)
                    self.unlock_object(output_stack_path)
                    logger.info('VRT stack file %s created', output_stack_path)
            
        return derived_stack_dict        
        
              
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
        """ Abstract function for calling in stack_derived() function. Should be overridden
        in a descendant class.
        
        Arguments:
            input_dataset_dict: Dict keyed by processing level (e.g. ORTHO, NBAR, PQA, DEM)
                containing all tile info which can be used within the function
                A sample is shown below (including superfluous band-specific information):
                
{
'NBAR': {'band_name': 'Visible Blue',
    'band_tag': 'B10',
    'end_datetime': datetime.datetime(2000, 2, 9, 23, 46, 36, 722217),
    'end_row': 77,
    'level_name': 'NBAR',
    'nodata_value': -999L,
    'path': 91,
    'satellite_tag': 'LS7',
    'sensor_name': 'ETM+',
    'start_datetime': datetime.datetime(2000, 2, 9, 23, 46, 12, 722217),
    'start_row': 77,
    'tile_layer': 1,
    'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_NBAR_150_-025_2000-02-09T23-46-12.722217.tif',
    'x_index': 150,
    'y_index': -25},
'ORTHO': {'band_name': 'Thermal Infrared (Low Gain)',
     'band_tag': 'B61',
     'end_datetime': datetime.datetime(2000, 2, 9, 23, 46, 36, 722217),
     'end_row': 77,
     'level_name': 'ORTHO',
     'nodata_value': 0L,
     'path': 91,
     'satellite_tag': 'LS7',
     'sensor_name': 'ETM+',
     'start_datetime': datetime.datetime(2000, 2, 9, 23, 46, 12, 722217),
     'start_row': 77,
     'tile_layer': 1,
     'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_ORTHO_150_-025_2000-02-09T23-46-12.722217.tif',
     'x_index': 150,
     'y_index': -25},
'PQA': {'band_name': 'Pixel Quality Assurance',
    'band_tag': 'PQA',
    'end_datetime': datetime.datetime(2000, 2, 9, 23, 46, 36, 722217),
    'end_row': 77,
    'level_name': 'PQA',
    'nodata_value': None,
    'path': 91,
    'satellite_tag': 'LS7',
    'sensor_name': 'ETM+',
    'start_datetime': datetime.datetime(2000, 2, 9, 23, 46, 12, 722217),
    'start_row': 77,
    'tile_layer': 1,
    'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_PQA_150_-025_2000-02-09T23-46-12.722217.tif,
    'x_index': 150,
    'y_index': -25}
}                
                
        Arguments (Cont'd):
            stack_output_info: dict containing stack output information. 
                Obtained from stacker object. 
                A sample is shown below
                
stack_output_info = {'x_index': 144, 
                      'y_index': -36,
                      'stack_output_dir': '/g/data/v10/tmp/ndvi',
                      'start_datetime': None, # Datetime object or None
                      'end_datetime': None, # Datetime object or None 
                      'satellite': None, # String or None 
                      'sensor': None} # String or None 
                      
        Arguments (Cont'd):
            tile_type_info: dict containing tile type information. 
                Obtained from stacker object (e.g: stacker.tile_type_dict[tile_type_id]).
                A sample is shown below
                
    {'crs': 'EPSG:4326',
    'file_extension': '.tif',
    'file_format': 'GTiff',
    'format_options': 'COMPRESS=LZW,BIGTIFF=YES',
    'tile_directory': 'EPSG4326_1deg_0.00025pixel',
    'tile_type_id': 1L,
    'tile_type_name': 'Unprojected WGS84 1-degree at 4000 pixels/degree',
    'unit': 'degree',
    'x_origin': 0.0,
    'x_pixel_size': Decimal('0.00025000000000000000'),
    'x_pixels': 4000L,
    'x_size': 1.0,
    'y_origin': 0.0,
    'y_pixel_size': Decimal('0.00025000000000000000'),
    'y_pixels': 4000L,
    'y_size': 1.0}
                            
        Function must create one or more GDAL-supported output datasets. Useful functions in the
        Stacker class include Stacker.get_pqa_mask(), but it is left to the coder to produce exactly
        what is required for a single slice of the temporal stack of derived quantities.
            
        Returns:
            output_dataset_info: Dict keyed by stack filename
                containing metadata info for GDAL-supported output datasets created by this function.
                Note that the key(s) will be used as the output filename for the VRT temporal stack
                and each dataset created must contain only a single band. An example is as follows:
{'/g/data/v10/tmp/ndvi/NDVI_stack_150_-025.vrt': 
    {'band_name': 'Normalised Differential Vegetation Index with PQA applied',
    'band_tag': 'NDVI',
    'end_datetime': datetime.datetime(2000, 2, 9, 23, 46, 36, 722217),
    'end_row': 77,
    'level_name': 'NDVI',
    'nodata_value': None,
    'path': 91,
    'satellite_tag': 'LS7',
    'sensor_name': 'ETM+',
    'start_datetime': datetime.datetime(2000, 2, 9, 23, 46, 12, 722217),
    'start_row': 77,
    'tile_layer': 1,
    'tile_pathname': '/g/data/v10/tmp/ndvi/LS7_ETM_NDVI_150_-025_2000-02-09T23-46-12.722217.tif',
    'x_index': 150,
    'y_index': -25}
}
                
                
        """
        assert type(input_dataset_dict) == dict, 'input_dataset_dict must be a dict'
        
        log_multiline(logger.debug, input_dataset_dict, 'input_dataset_dict', '\t')    
       
        # Test function to copy ORTHO & NBAR band datasets with pixel quality mask applied
        # to an output directory for stacking

        output_dataset_dict = {}
        for input_level in ['NBAR', 'ORTHO']:
            input_dataset_info = input_dataset_dict[input_level]
            input_path = input_dataset_info['tile_pathname']
            
            # Generate sorted list of band info for this tile type, satellite and sensor
            band_dict = self.bands[tile_type_info['tile_type_id']][(input_dataset_info['satellite_tag'], input_dataset_info['sensor_name'])]
            band_info_list = [band_dict[tile_layer] for tile_layer in sorted(band_dict.keys()) if band_dict[tile_layer]['level_name'] == input_level]

            # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
            pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
            
            input_dataset = gdal.Open(input_path)
            assert input_dataset, 'Unable to open dataset %s' % input_dataset
            
            no_data_value = input_dataset_info['nodata_value']
                                
            # Create single-band output dataset for each band
            for band_index in range(input_dataset.RasterCount):               
                # Define the output stack name (used as dict key)
                output_stack_path = os.path.join(self.output_dir, '%s_%s_pqa_masked.vrt' % (input_level, 
                                                                                            band_info_list[band_index]['band_tag']
                                                                                            )
                                                     )
                    
                output_tile_path = os.path.join(self.output_dir, re.sub('\.\w+$', 
                                                                   '_%s%s' % (band_info_list[band_index]['band_tag'],
                                                                              tile_type_info['file_extension']),
                                                                   os.path.basename(input_path)
                                                                   )
                                           )
                
                # Copy metadata for eventual inclusion in stack file output
                # This could also be written to the output tile if required
                output_dataset_info = dict(input_dataset_info)
                output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find 
                output_dataset_info['band_name'] = '%s with PQA mask applied' % band_info_list[band_index]['band_name']
                output_dataset_info['band_tag'] = '%s-PQA' % band_info_list[band_index]['band_tag']
                output_dataset_info['tile_layer'] = 1

                # Check for existing, valid file
                if self.refresh or not os.path.exists(output_tile_path) or not gdal.Open(output_tile_path):
                    
                    if self.lock_object(output_tile_path):
                        
                        input_band = input_dataset.GetRasterBand(band_index + 1)
                    
                        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                        output_dataset = gdal_driver.Create(output_tile_path, 
                                                            input_dataset.RasterXSize, input_dataset.RasterYSize,
                                                            1, input_band.DataType,
                                                            tile_type_info['format_options'].split(','))
                        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
                        output_dataset.SetGeoTransform(input_dataset.GetGeoTransform())
                        output_dataset.SetProjection(input_dataset.GetProjection()) 
        
                        output_band = output_dataset.GetRasterBand(1)
        
                        data_array = input_band.ReadAsArray()
                        
                        self.apply_pqa_mask(data_array, pqa_mask, no_data_value)
                        
                        output_band.WriteArray(data_array)
                        output_band.SetNoDataValue(no_data_value)
                        output_band.FlushCache()
                        
                        # This is not strictly necessary - copy metadata to output dataset
                        output_dataset_metadata = input_dataset.GetMetadata()
                        output_dataset_metadata.update(input_band.GetMetadata())
                        if output_dataset_metadata:
                            output_dataset.SetMetadata(output_dataset_metadata) 
                            log_multiline(logger.debug, output_dataset_metadata, 'output_dataset_metadata', '\t')    
                        
                        output_dataset.FlushCache()
                        self.unlock_object(output_tile_path)
                        logger.info('Finished writing dataset %s', output_tile_path)
                    else:
                        logger.info('Skipped locked dataset %s', output_tile_path)
                        sleep(5) #TODO: Find a nicer way of dealing with contention for the same output tile
                else:
                    logger.info('Skipped existing, valid dataset %s', output_tile_path)
                
                output_dataset_dict[output_stack_path] = output_dataset_info
#                log_multiline(logger.debug, output_dataset_info, 'output_dataset_info', '\t')    
        
        log_multiline(logger.debug, output_dataset_dict, 'output_dataset_dict', '\t')    
        # Both NBAR & ORTHO datasets processed - return info for both
        return output_dataset_dict
    
if __name__ == '__main__':
    def date2datetime(input_date, time_offset=time.min):
        if not input_date:
            return None
        return datetime.combine(input_date, time_offset)
    
    stacker = Stacker()
    
    # Check for required command line parameters
    assert stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert stacker.output_dir, 'Output directory not specified (-o or --output)'
    assert os.path.isdir(stacker.output_dir), 'Invalid output directory specified (-o or --output)'
    stacker.output_dir = os.path.abspath(stacker.output_dir)
    
    log_multiline(logger.debug, stacker.__dict__, 'stacker.__dict__', '\t')
    
    # Stacker object already has command line parameters
    # Note that disregard_incomplete_data is set to True for command line invokation
    stack_info_dict = stacker.stack_tile(x_index=stacker.x_index, 
                                         y_index=stacker.y_index, 
                                         stack_output_dir=stacker.output_dir, 
                                         start_datetime=date2datetime(stacker.start_date, time.min), 
                                         end_datetime=date2datetime(stacker.end_date, time.max), 
                                         satellite=stacker.satellite, 
                                         sensor=stacker.sensor, 
                                         path=stacker.path, 
                                         row=stacker.row, 
                                         tile_type_id=None,
                                         create_band_stacks=True,
                                         disregard_incomplete_data=True)
    
    log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), stacker.output_dir)

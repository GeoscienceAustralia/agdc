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
Stacker subclass implementation to create PQ-masked temporal stack of multiple 
indices and bands (similar to index_stacker.py) for a given annual period aggregated 
across multiple years.
Originally developed to deliver seasonal statistics for MDBA researchers.

Created on 21/02/2013

@author: u76345
'''
import os
import sys
import logging
import re
import numpy
from datetime import datetime, date, time
from osgeo import gdal, gdalconst
from time import sleep
import numexpr
import gc
import argparse
from copy import copy

from agdc import Stacker
from eotools.stats.temporal_stats import create_envi_hdr
from eotools.utils import log_multiline
from eotools.stats import temporal_stats

SCALE_FACTOR = 10000
NaN = numpy.float32(numpy.NaN)

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class SeasonStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def __init__(self, source_datacube=None, default_tile_type_id=1):
        if source_datacube:
            # Copy values from source_datacube and then override command line args
            self.__dict__ = copy(source_datacube.__dict__)
            
            args = self.parse_args()
            # Set instance attributes for every value in command line arguments file
            for attribute_name in args.__dict__.keys():
                attribute_value = args.__dict__[attribute_name]
                self.__setattr__(attribute_name, attribute_value)

        else:
            Stacker.__init__(self, source_datacube, default_tile_type_id) # Call inherited constructor
            
        if self.debug:
            console_handler.setLevel(logging.DEBUG)
            
        try:
            self.years = int(self.years) 
        except:
            self.years = 0
            
        
        
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
            default=os.path.join(self.agdc_root, 'agdc_default.conf'),
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
            help='Tile type ID of tile to be stacked')
        _arg_parser.add_argument('--refresh', dest='refresh',
           default=False, action='store_const', const=True,
           help='Refresh mode flag to force updating of existing files')
        _arg_parser.add_argument('-Y', '--years', dest='years',
            required=False, default=0,
            help='Number of years to repeat') 
    
        return _arg_parser.parse_args()
        
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
        """ Overrides abstract function in stacker class. Called in Stacker.stack_derived() function. 
        Creates PQA-masked NDVI stack
        
        Arguments:
            nbar_dataset_dict: Dict keyed by processing level (e.g. ORTHO, NBAR, PQA, DEM)
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
        assert type(input_dataset_dict) == dict, 'nbar_dataset_dict must be a dict'
                
        dtype = {'B10' : gdalconst.GDT_Float32,
                 'B20' : gdalconst.GDT_Float32,
                 'B30' : gdalconst.GDT_Float32,
                 'B40' : gdalconst.GDT_Float32,
                 'B50' : gdalconst.GDT_Float32,
                 'B70' : gdalconst.GDT_Float32,
                 'NDVI' : gdalconst.GDT_Float32,
                 'EVI' : gdalconst.GDT_Float32,
                 'NDSI' : gdalconst.GDT_Float32,
                 'NDMI' : gdalconst.GDT_Float32,
                 'SLAVI' : gdalconst.GDT_Float32,
                 'SATVI' : gdalconst.GDT_Float32,
                 'WATER' : gdalconst.GDT_Int16}

        no_data_value = {'B10' : numpy.nan,
                 'B20' : numpy.nan,
                 'B30' : numpy.nan,
                 'B40' : numpy.nan,
                 'B50' : numpy.nan,
                 'B70' : numpy.nan,
                 'NDVI' : numpy.nan,
                 'EVI' : numpy.nan,
                 'NDSI' : numpy.nan,
                 'NDMI' : numpy.nan,
                 'SLAVI' : numpy.nan,
                 'SATVI' : numpy.nan,
                 'WATER' : -1}
    
        log_multiline(logger.debug, input_dataset_dict, 'nbar_dataset_dict', '\t')    
       
        # Test function to copy ORTHO & NBAR band datasets with pixel quality mask applied
        # to an output directory for stacking

        output_dataset_dict = {}
        nbar_dataset_info = input_dataset_dict['NBAR'] # Only need NBAR data for NDVI
        #thermal_dataset_info = input_dataset_dict['ORTHO'] # Could have one or two thermal bands
        
        nbar_dataset_path = nbar_dataset_info['tile_pathname']
        
        # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
        pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
        
        nbar_dataset = gdal.Open(nbar_dataset_path)
        assert nbar_dataset, 'Unable to open NBAR dataset %s' % nbar_dataset
        
        band_array = None;
        # List of outputs to generate from each file
        output_tag_list = ['B10', 'B20', 'B30', 'B40', 'B50', 'B70', 
                           'NDVI', 'EVI', 'NDSI', 'NDMI', 'SLAVI', 'SATVI']
        for output_tag in sorted(output_tag_list): 
        # List of outputs to generate from each file
            # TODO: Make the stack file name reflect the date range                    
            output_stack_path = os.path.join(self.output_dir, 
                                             re.sub(r'\+', '', '%s_%+04d_%+04d' % (output_tag,
                                                                                   stack_output_info['x_index'],
                                                                                    stack_output_info['y_index'])))
                                                                                    
            if stack_output_info['start_datetime']:
                output_stack_path += '_%s' % stack_output_info['start_datetime'].strftime('%m%d')
            if stack_output_info['end_datetime']:
                output_stack_path += '_%s' % stack_output_info['end_datetime'].strftime('%m%d')
                
            output_stack_path += '_pqa_stack.vrt'
            
            output_tile_path = os.path.join(self.output_dir, re.sub(r'\.\w+$', tile_type_info['file_extension'],
                                                                    re.sub('NBAR', 
                                                                           output_tag,
                                                                           os.path.basename(nbar_dataset_path)
                                                                           )
                                                                   )
                                           )
                
            # Copy metadata for eventual inclusion in stack file output
            # This could also be written to the output tile if required
            output_dataset_info = dict(nbar_dataset_info)
            output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find tiles to stack
            output_dataset_info['band_name'] = '%s with PQA mask applied' % output_tag
            output_dataset_info['band_tag'] = '%s-PQA' % output_tag
            output_dataset_info['tile_layer'] = 1
            output_dataset_info['nodata_value'] = no_data_value[output_tag]

            # Check for existing, valid file
            if self.refresh or not os.path.exists(output_tile_path):

                if self.lock_object(output_tile_path): # Test for concurrent writes to the same file
                    try:
                        # Read whole nbar_dataset into one array. 
                        # 62MB for float32 data should be OK for memory depending on what else happens downstream
                        if band_array is None:
                            # Convert to float32 for arithmetic and scale back to 0~1 reflectance
                            band_array = (nbar_dataset.ReadAsArray().astype(numpy.float32)) / SCALE_FACTOR

                            # Re-project issues with PQ. REDO the contiguity layer.
                            non_contiguous = (band_array < 0).any(0)
                            pqa_mask[non_contiguous] = False
                    
                        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                        #output_dataset = gdal_driver.Create(output_tile_path, 
                        #                                    nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                        #                                    1, nbar_dataset.GetRasterBand(1).DataType,
                        #                                    tile_type_info['format_options'].split(','))
                        output_dataset = gdal_driver.Create(output_tile_path, 
                                                            nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                                            1, dtype[output_tag],
                                                            tile_type_info['format_options'].split(','))
                        logger.debug('gdal_driver.Create(%s, %s, %s, %s, %s, %s',
                                                            output_tile_path, 
                                                            nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                                            1, dtype[output_tag],
                                                            tile_type_info['format_options'].split(','))
                        assert output_dataset, 'Unable to open output dataset %s' % output_tile_path                                   
                        output_dataset.SetGeoTransform(nbar_dataset.GetGeoTransform())
                        output_dataset.SetProjection(nbar_dataset.GetProjection()) 
            
                        output_band = output_dataset.GetRasterBand(1)
            
                        # Calculate each output here
                        # Remember band_array indices are zero-based

                        if output_tag[0] == 'B': # One of the band tags
                            band_file_no = int(output_tag[1:])
                            # Look up tile_layer (i.e. band number) for specified spectral band in tile dataset
                            tile_layer = self.bands[tile_type_info['tile_type_id']][(nbar_dataset_info['satellite_tag'], nbar_dataset_info['sensor_name'])][band_file_no]['tile_layer']
                            # Copy values 
                            data_array = band_array[tile_layer - 1].copy()
                        elif output_tag == 'NDVI':
                            data_array = numexpr.evaluate("((b4 - b3) / (b4 + b3)) + 1", {'b4':band_array[3], 'b3':band_array[2]})
                        elif output_tag == 'EVI':
                            data_array = numexpr.evaluate("(2.5 * ((b4 - b3) / (b4 + (6 * b3) - (7.5 * b1) + 1))) + 1", {'b4':band_array[3], 'b3':band_array[2], 'b1':band_array[0]})
                        elif output_tag == 'NDSI':   
                            data_array = numexpr.evaluate("((b3 - b5) / (b3 + b5)) + 1", {'b5':band_array[4], 'b3':band_array[2]})
                        elif output_tag == 'NDMI':
                            data_array = numexpr.evaluate("((b4 - b5) / (b4 + b5)) + 1", {'b5':band_array[4], 'b4':band_array[3]})
                        elif output_tag == 'SLAVI':
                            data_array = numexpr.evaluate("b4 / (b3 + b5)", {'b5':band_array[4], 'b4':band_array[3], 'b3':band_array[2]})
                        elif output_tag == 'SATVI':
                            data_array = numexpr.evaluate("(((b5 - b3) / (b5 + b3 + 0.5)) * 1.5 - (b7 / 2)) + 1", {'b5':band_array[4], 'b7':band_array[5], 'b3':band_array[2]})
                        elif output_tag == 'WATER':
                            data_array = numpy.zeros(band_array[0].shape, dtype=numpy.int16)
                            #TODO: Call water analysis code here
                        else:
                            raise Exception('Invalid operation')
                                            
                        if no_data_value[output_tag]:
                            self.apply_pqa_mask(data_array=data_array, pqa_mask=pqa_mask, no_data_value=no_data_value[output_tag])
                        
                        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                        #output_dataset = gdal_driver.Create(output_tile_path, 
                        #                                    nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                        #                                    1, nbar_dataset.GetRasterBand(1).DataType,
                        #                                    tile_type_info['format_options'].split(','))
                        output_dataset = gdal_driver.Create(output_tile_path, 
                                                            nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                                            1, dtype[output_tag],
                                                            tile_type_info['format_options'].split(','))
                        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
                        output_dataset.SetGeoTransform(nbar_dataset.GetGeoTransform())
                        output_dataset.SetProjection(nbar_dataset.GetProjection()) 
            
                        output_band = output_dataset.GetRasterBand(1)
            
                        output_band.WriteArray(data_array)
                        output_band.SetNoDataValue(output_dataset_info['nodata_value'])
                        output_band.FlushCache()
                        
                        # This is not strictly necessary - copy metadata to output dataset
                        output_dataset_metadata = nbar_dataset.GetMetadata()
                        if output_dataset_metadata:
                            output_dataset.SetMetadata(output_dataset_metadata) 
                            log_multiline(logger.debug, output_dataset_metadata, 'output_dataset_metadata', '\t')    
                        
                        output_dataset.FlushCache()
                        logger.info('Finished writing dataset %s', output_tile_path)
                    finally:
                        self.unlock_object(output_tile_path)
                else:
                    logger.info('Skipped locked dataset %s', output_tile_path)
                    sleep(5) #TODO: Find a nicer way of dealing with contention for the same output tile
                    
            else:
                logger.info('Skipped existing dataset %s', output_tile_path)
        
            output_dataset_dict[output_stack_path] = output_dataset_info
#                    log_multiline(logger.debug, output_dataset_info, 'output_dataset_info', '\t')    

        log_multiline(logger.debug, output_dataset_dict, 'output_dataset_dict', '\t')    
        # NDVI dataset processed - return info
        return output_dataset_dict
    

if __name__ == '__main__':
    def assemble_stack(season_stacker, years=0):    
        """
        returns stack_info_dict - a dict keyed by stack file name containing a list of tile_info dicts
        """
        def date2datetime(input_date, time_offset=time.min):
            if not input_date:
                return None
            return datetime.combine(input_date, time_offset)
        
        derived_stack_dict = {}
        start_date = season_stacker.start_date
        end_date = season_stacker.end_date
        end_year = end_date.year + years
        
        while end_date.year <= end_year:            
            season_info_dict = season_stacker.stack_derived(x_index=season_stacker.x_index, 
                             y_index=season_stacker.y_index, 
                             stack_output_dir=season_stacker.output_dir, 
                             start_datetime=date2datetime(start_date, time.min), 
                             end_datetime=date2datetime(end_date, time.max), 
                             satellite=season_stacker.satellite, 
                             sensor=season_stacker.sensor,
                             create_stacks=False)
            
            for output_stack_path in season_info_dict:
                # Create a new list for each stack if it doesn't already exist
                stack_list = derived_stack_dict.get(output_stack_path, [])
                if not stack_list:
                    derived_stack_dict[output_stack_path] = stack_list
                    
                stack_list.extend(season_info_dict[output_stack_path])
        
            start_date = date(start_date.year + 1, start_date.month, start_date.day) 
            end_date = date(end_date.year + 1, end_date.month, end_date.day)
            
        log_multiline(logger.debug, derived_stack_dict, 'derived_stack_dict', '\t')
        
        for output_stack_path in sorted(derived_stack_dict.keys()):
            if os.path.exists(output_stack_path) and not season_stacker.refresh:
                logger.info('Skipped existing stack file %s', output_stack_path)
                continue
            
            if (season_stacker.lock_object(output_stack_path)):
                logger.debug('Creating temporal stack %s', output_stack_path)
                season_stacker.stack_files(timeslice_info_list=derived_stack_dict[output_stack_path], 
                             stack_dataset_path=output_stack_path, 
                             band1_vrt_path=None, overwrite=True)
                season_stacker.unlock_object(output_stack_path)
#                logger.info('VRT stack file %s created', output_stack_path)

        logger.info('Finished creating %d temporal stack files in %s.', len(derived_stack_dict), season_stacker.output_dir)
        return derived_stack_dict
    
    def translate_ndvi_to_envi(season_stacker, stack_info_dict):
        
        def vrt2envi(ndvi_vrt_stack_path, ndvi_envi_stack_path):
            ndvi_vrt_stack_dataset = gdal.Open(ndvi_vrt_stack_path)
            logger.info('opened %d layer datset %s', ndvi_vrt_stack_dataset.RasterCount, ndvi_vrt_stack_path)
            
            gdal_driver = gdal.GetDriverByName('ENVI')
            ndvi_envi_stack_dataset = gdal_driver.Create(ndvi_envi_stack_path, 
                                                         ndvi_vrt_stack_dataset.RasterXSize, 
                                                         ndvi_vrt_stack_dataset.RasterYSize, 
                                                         ndvi_vrt_stack_dataset.RasterCount, 
                                                         gdal.GDT_Int16)
            ndvi_envi_stack_dataset.SetGeoTransform(ndvi_vrt_stack_dataset.GetGeoTransform())
            ndvi_envi_stack_dataset.SetProjection(ndvi_vrt_stack_dataset.GetProjection())
            logger.info('Created %s', ndvi_envi_stack_path)
            
            for layer_index in range(ndvi_vrt_stack_dataset.RasterCount):
                input_band = ndvi_vrt_stack_dataset.GetRasterBand(layer_index + 1)
                ndvi_vrt_stack_dataset.FlushCache() # Get rid of cached layer after read
                layer_array = input_band.ReadAsArray() * SCALE_FACTOR
                layer_array[~numpy.isfinite(layer_array)] = -32768                                                  
                output_band = ndvi_envi_stack_dataset.GetRasterBand(layer_index + 1)
                output_band.WriteArray(layer_array)
                output_band.SetNoDataValue(-32768)
                output_band.SetMetadata(input_band.GetMetadata())
                output_band.FlushCache()
                
            ndvi_envi_stack_dataset.FlushCache()
            del layer_array
            del output_band
            gc.collect()
                
    #        ndvi_envi_stack_dataset.SetMetadata(ndvi_vrt_stack_dataset.GetMetadata())
            del ndvi_envi_stack_dataset
            # ndvi_envi_stack_dataset = gdal.Open(ndvi_envi_stack_path)
            gc.collect()
        
        ndvi_vrt_stack_path = [vrt_stack_path for vrt_stack_path in stack_info_dict.keys() if vrt_stack_path.find('NDVI') > -1][0]
            
        stack_list = stack_info_dict[ndvi_vrt_stack_path]
        layer_name_list = ['Band_%d %s-%s %s' % (tile_index + 1, 
                                                 stack_list[tile_index]['satellite_tag'],
                                                 stack_list[tile_index]['sensor_name'],
                                                 stack_list[tile_index]['start_datetime'].isoformat()
                                                 ) for tile_index in range(len(stack_list))]
        
        ndvi_envi_stack_path = re.sub(r'\.vrt$', '_envi', ndvi_vrt_stack_path)
        
        if os.path.exists(ndvi_envi_stack_path) and not season_stacker.refresh:
            logger.info('Skipping existing NDVI Envi file %s', ndvi_envi_stack_path)
            return ndvi_envi_stack_path
        
        vrt2envi(ndvi_vrt_stack_path, ndvi_envi_stack_path)
        create_envi_hdr(envi_file=ndvi_envi_stack_path, noData=-32768, band_names=layer_name_list)
               
        logger.info('Finished writing NDVI Envi file %s', ndvi_envi_stack_path)
        
        return ndvi_envi_stack_path
            
    def calc_stats(season_stacker, stack_info_dict):
        stats_dataset_path_dict = {}
        for vrt_stack_path in sorted(stack_info_dict.keys()):
            stack_list = stack_info_dict[vrt_stack_path]
            
            if vrt_stack_path.find('WATER') > -1: # Don't run stats on water analysis
                continue
            
            stats_dataset_path = vrt_stack_path.replace('.vrt', '_stats_envi')
            stats_dataset_path_dict[vrt_stack_path] = stats_dataset_path
            
            if os.path.exists(stats_dataset_path) and not season_stacker.refresh:
                logger.info('Skipping existing stats file %s', stats_dataset_path)
                continue
            
            temporal_stats.main(vrt_stack_path, stats_dataset_path, 
                                               noData=stack_list[0]['nodata_value'], 
                                               #================================
                                               # xtile=season_stacker.tile_type_dict[season_stacker.default_tile_type_id]['x_pixels'], # Full tile width
                                               # ytile=season_stacker.tile_type_dict[season_stacker.default_tile_type_id]['y_pixels'], # Full tile height
                                               #================================
                                               ytile=200, # Full width x 200 rows
                                               provenance=True # Create two extra bands for datetime and satellite provenance
                                               )
            logger.info('Finished creating stats file %s', stats_dataset_path)
            
        logger.info('Finished calculating %d temporal summary stats files in %s.', len(stats_dataset_path_dict), season_stacker.output_dir)
        return stats_dataset_path_dict
        
    def update_stats_metadata(season_stacker, stack_info_dict, stats_dataset_path_dict):
        for vrt_stack_path in sorted(stack_info_dict.keys()):
            stats_dataset_path = stats_dataset_path_dict.get(vrt_stack_path)
            if not stats_dataset_path: # Don't proceed if no stats file (e.g. WATER)
                continue
            
            stack_list = stack_info_dict[vrt_stack_path]
            start_datetime = stack_list[0]['start_datetime']
            end_datetime = stack_list[-1]['end_datetime']
            description = 'Statistical summary for %s' % stack_list[0]['band_name']

            # Reopen output file and write source dataset to metadata
            stats_dataset = gdal.Open(stats_dataset_path, gdalconst.GA_Update)
            metadata = stats_dataset.GetMetadata()
            metadata['source_dataset'] = vrt_stack_path # Should already be set
            metadata['start_datetime'] = start_datetime.isoformat()
            metadata['end_datetime'] = end_datetime.isoformat()
            stats_dataset.SetMetadata(metadata)
            stats_dataset.SetDescription(description)
            stats_dataset.FlushCache()
            logger.info('Finished updating metadata in stats file %s', stats_dataset_path)
            
        logger.info('Finished updating metadata in %d temporal summary stats files in %s.', len(stats_dataset_path_dict), season_stacker.output_dir)
            
    def remove_intermediate_files(season_stacker, stack_info_dict):
        """ Function to remove intermediate band files after stats have been computed
        """
        removed_file_count = 0
        for vrt_stack_path in sorted(stack_info_dict.keys()):           
            stack_list = stack_info_dict[vrt_stack_path]
            
            # Remove all derived quantity tiles (referenced by VRT)
            for tif_dataset_path in [tile_info['tile_pathname'] for tile_info in stack_list]:
                season_stacker.remove(tif_dataset_path)
                logger.debug('Removed file %s', tif_dataset_path)
                season_stacker.remove(tif_dataset_path + '.aux.xml')
                logger.debug('Removed file %s', tif_dataset_path + '.aux.xml')
               
            # Remove stack VRT file
            season_stacker.remove(vrt_stack_path)
            logger.debug('Removed file %s', vrt_stack_path)
            season_stacker.remove(vrt_stack_path + '.aux.xml')
            logger.debug('Removed file %s', vrt_stack_path + '.aux.xml')
            
            removed_file_count += len(stack_list) + 1
             
            #===================================================================
            # # Remove all Envi stack files except for NDVI   
            # if not re.match('NDVI', stack_list[0]['band_tag']): 
            #    season_stacker.remove(envi_dataset_path_dict[vrt_stack_path])
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path])
            #    season_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    season_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    removed_file_count += 1
            #===================================================================
                
        logger.info('Finished removing %d intermediate files in %s.', 
                    removed_file_count, 
                    season_stacker.output_dir)
        
                     
    # Main function starts here
    # Stacker class takes care of command line parameters
    season_stacker = SeasonStacker()

    # Check for required command line parameters
    assert season_stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert season_stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert season_stacker.output_dir, 'Output directory not specified (-o or --output)'
    assert not os.path.exists(season_stacker.output_dir) or os.path.isdir(season_stacker.output_dir), 'Invalid output directory specified (-o or --output)'
    season_stacker.output_dir = os.path.abspath(season_stacker.output_dir)
    
    stack_info_dict = assemble_stack(season_stacker, season_stacker.years)
    
    if not stack_info_dict:
        logger.info('No tiles to stack. Exiting.')
        exit(0)
    
    stats_dataset_path_dict = calc_stats(season_stacker, stack_info_dict)
    update_stats_metadata(season_stacker, stack_info_dict, stats_dataset_path_dict)
    ndvi_envi_dataset_path = translate_ndvi_to_envi(season_stacker, stack_info_dict)

# Do NOT delete intermediate files because MDBA seasons overlap and concurrent jobs may fail
# Need to do a manual "find <output_root> -name '*.tif' -delete" and "find <output_root> -name '*.vrt' -delete" when all jobs are finished
#    if not season_stacker.debug:
#        remove_intermediate_files(season_stacker, stack_info_dict)
    



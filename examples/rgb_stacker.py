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
Stacker subclass implementation to create PQ-masked RGB (Bands 5-4-2) files. 
NB: GDAL cannot temporally stack multi-band files, so separate files are generated.

Created on 21/02/2013

@author: u76345
'''
import os
import sys
import logging
import re
import numpy
from datetime import datetime, time
from osgeo import gdal, gdalconst
from time import sleep
import gc

from agdc import Stacker
from eotools.utils import log_multiline
from agdc import BandLookup

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
                
class IndexStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
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
        
        log_multiline(logger.debug, input_dataset_dict, 'input_dataset_dict', '\t')

        # Definitions for mapping NBAR values to RGB
        rgb_bands=('SWIR1','NIR','G')
        rgb_minmax=((780,5100),(200,4500),(100,2300)) # Min/Max scaled values to map to 1-255
        
        nbar_dataset_info = input_dataset_dict.get('NBAR') # Only need NBAR data for NDVI
        #thermal_dataset_info = input_dataset_dict['ORTHO'] # Could have one or two thermal bands
        
        if not nbar_dataset_info:
            log_multiline(logger.warning, input_dataset_dict, 'NBAR dict does not exist', '\t')
            return None

        # Instantiate band lookup object with all required lookup parameters
        lookup = BandLookup(data_cube=self,
                            lookup_scheme_name='LANDSAT-LS5/7',
                            tile_type_id=tile_type_info['tile_type_id'],
                            satellite_tag=nbar_dataset_info['satellite_tag'],
                            sensor_name=nbar_dataset_info['sensor_name'],
                            level_name=nbar_dataset_info['level_name']
                            )

        nbar_dataset_path = nbar_dataset_info['tile_pathname']
        output_tile_path = os.path.join(self.output_dir, re.sub(r'\.\w+$',
                                                           '_RGB.tif',
                                                           os.path.basename(nbar_dataset_path)
                                                           )
                                   )
        
        if os.path.exists(output_tile_path):
            logger.info('Skipping existing file %s', output_tile_path)
            return None
        
        if not self.lock_object(output_tile_path):
            logger.info('Skipping locked file %s', output_tile_path)
            return None
        
        input_dataset = gdal.Open(nbar_dataset_path)
        assert input_dataset, 'Unable to open dataset %s' % nbar_dataset_path

        # Nasty work-around for bad PQA due to missing thermal bands for LS8-OLI
        if nbar_dataset_info['satellite_tag'] == 'LS8' and nbar_dataset_info['sensor_name'] == 'OLI':
            pqa_mask = numpy.ones(shape=(input_dataset.RasterYSize, input_dataset.RasterXSize), dtype=numpy.bool)
            logger.debug('Work-around for LS8-OLI PQA issue applied: EVERYTHING PASSED')
        else:
            if input_dataset_dict.get('PQA') is None: # No PQA tile available
                return

            # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
            pqa_mask = self.get_pqa_mask(pqa_dataset_path=input_dataset_dict['PQA']['tile_pathname']) 

        log_multiline(logger.debug, pqa_mask, 'pqa_mask', '\t')

        gdal_driver = gdal.GetDriverByName('GTiff')
        output_dataset = gdal_driver.Create(output_tile_path, 
                                            input_dataset.RasterXSize, input_dataset.RasterYSize,
                                            3, gdal.GDT_Byte,
                                            ['INTERLEAVE=PIXEL','COMPRESS=LZW'] #,'BIGTIFF=YES']
                                            )
        
        assert output_dataset, 'Unable to open output dataset %s'% output_dataset   
                                        
        output_dataset.SetGeoTransform(input_dataset.GetGeoTransform())
        output_dataset.SetProjection(input_dataset.GetProjection()) 

        for band_index in range(3):
            logger.debug('Processing %s band in layer %s as band %s', rgb_bands[band_index], lookup.band_no[rgb_bands[band_index]], band_index + 1)

            # Offset byte values by 1 to avoid transparency bug
            scale = (rgb_minmax[band_index][1] - rgb_minmax[band_index][0]) / 254.0
            offset = 1.0 - rgb_minmax[band_index][0] / scale
            
            input_array = input_dataset.GetRasterBand(lookup.band_no[rgb_bands[band_index]]).ReadAsArray()
            log_multiline(logger.debug, input_array, 'input_array', '\t')

            output_array = (input_array / scale + offset).astype(numpy.byte)
            
            # Set out-of-range values to minimum or maximum as required
            output_array[input_array < rgb_minmax[band_index][0]] = 1
            output_array[input_array > rgb_minmax[band_index][1]] = 255
            
            output_array[~pqa_mask] = 0 # Apply PQA Mask
            log_multiline(logger.debug, output_array, 'output_array', '\t')
            
            output_band = output_dataset.GetRasterBand(band_index + 1)
            output_band.WriteArray(output_array)
            output_band.SetNoDataValue(0)
            output_band.FlushCache()
        output_dataset.FlushCache()
        self.unlock_object(output_tile_path)
        logger.info('Finished writing RGB file %s', output_tile_path)
        
        return None # Don't build a stack file

if __name__ == '__main__':
    def assemble_stack(index_stacker):    
        """
        returns stack_info_dict - a dict keyed by stack file name containing a list of tile_info dicts
        """
        def date2datetime(input_date, time_offset=time.min):
            if not input_date:
                return None
            return datetime.combine(input_date, time_offset)
            
        stack_info_dict = index_stacker.stack_derived(x_index=index_stacker.x_index, 
                             y_index=index_stacker.y_index, 
                             stack_output_dir=index_stacker.output_dir, 
                             start_datetime=date2datetime(index_stacker.start_date, time.min), 
                             end_datetime=date2datetime(index_stacker.end_date, time.max), 
                             satellite=index_stacker.satellite, 
                             sensor=index_stacker.sensor)
        
        log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
        
        logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), index_stacker.output_dir)
        return stack_info_dict
    
    def remove_intermediate_files(index_stacker, stack_info_dict):
        """ Function to remove intermediate band files after stats have been computed
        """
        removed_file_count = 0
        for vrt_stack_path in sorted(stack_info_dict.keys()):           
            stack_list = stack_info_dict[vrt_stack_path]
            
            # Remove all derived quantity tiles (referenced by VRT)
            for tif_dataset_path in [tile_info['tile_pathname'] for tile_info in stack_list]:
                index_stacker.remove(tif_dataset_path)
                logger.debug('Removed file %s', tif_dataset_path)
                index_stacker.remove(tif_dataset_path + '.aux.xml')
                logger.debug('Removed file %s', tif_dataset_path + '.aux.xml')
               
            # Remove stack VRT file
            index_stacker.remove(vrt_stack_path)
            logger.debug('Removed file %s', vrt_stack_path)
            index_stacker.remove(vrt_stack_path + '.aux.xml')
            logger.debug('Removed file %s', vrt_stack_path + '.aux.xml')
            
            removed_file_count += len(stack_list) + 1
             
            #===================================================================
            # # Remove all Envi stack files except for NDVI   
            # if not re.match('NDVI', stack_list[0]['band_tag']): 
            #    index_stacker.remove(envi_dataset_path_dict[vrt_stack_path])
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path])
            #    index_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    index_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    removed_file_count += 1
            #===================================================================
                
        logger.info('Finished removing %d intermediate files in %s.', 
                    removed_file_count, 
                    index_stacker.output_dir)
        
                     
    # Main function starts here
    # Stacker class takes care of command line parameters
    index_stacker = IndexStacker()
    
    if index_stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert index_stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert index_stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert index_stacker.output_dir, 'Output directory not specified (-o or --output)'
    assert not os.path.exists(index_stacker.output_dir) or os.path.isdir(index_stacker.output_dir), 'Invalid output directory specified (-o or --output)'
    index_stacker.output_dir = os.path.abspath(index_stacker.output_dir)
    
    stack_info_dict = assemble_stack(index_stacker)
    
    if not stack_info_dict:
        logger.info('No tiles to stack. Exiting.')
        exit(0)
    
#    if not index_stacker.debug:
#        remove_intermediate_files(index_stacker, stack_info_dict)
    



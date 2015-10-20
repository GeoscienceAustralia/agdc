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
Created on 21/02/2013

@author: u76345
'''
import os
import sys
import logging
import re
import numpy
from datetime import datetime, time
from osgeo import gdal

from agdc import Stacker
from eotools.utils import log_multiline
from agdc import BandLookup

SCALE_FACTOR = 10000

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class NDVIStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
        """ Overrides abstract function in stacker class. Called in Stacker.stack_derived() function. 
        Creates PQA-masked NDVI stack
        
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
        nbar_dataset_info = input_dataset_dict.get('NBAR') # Only need NBAR data for NDVI
        if nbar_dataset_info is None:
            return

        #thermal_dataset_info = input_dataset_dict['ORTHO'] # Could have one or two thermal bands
        
        # Instantiate band lookup object with all required lookup parameters
        lookup = BandLookup(data_cube=self,
                            lookup_scheme_name='LANDSAT-LS5/7',
                            tile_type_id=tile_type_info['tile_type_id'],
                            satellite_tag=nbar_dataset_info['satellite_tag'],
                            sensor_name=nbar_dataset_info['sensor_name'],
                            level_name=nbar_dataset_info['level_name']
                            )

        nbar_dataset_path = nbar_dataset_info['tile_pathname']
        
        #=======================================================================
        # # Generate sorted list of band info for this tile type, satellite and sensor
        # band_dict = self.bands[tile_type_info['tile_type_id']][(nbar_dataset_info['satellite_tag'], nbar_dataset_info['sensor_name'])]
        # band_info_list = [band_dict[tile_layer] for tile_layer in sorted(band_dict.keys()) if band_dict[tile_layer]['level_name'] == 'NBAR']
        #=======================================================================

        # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
        pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
        
        nbar_dataset = gdal.Open(nbar_dataset_path)
        assert nbar_dataset, 'Unable to open dataset %s' % nbar_dataset
        logger.debug('Opened NBAR dataset %s', nbar_dataset_path)
        
        #no_data_value = nbar_dataset_info['nodata_value']
        no_data_value = -32767 # Need a value outside the scaled range -10000 - +10000
        
        for output_tag in ['NDVI']: # List of outputs to generate from each file - just NDVI at this stage.
                                
            output_stack_path = os.path.join(self.output_dir, '%s_pqa_masked.vrt' % output_tag)
                    
            output_tile_path = os.path.join(self.output_dir, re.sub(r'\.\w+$',
                                                                   '_%s%s' % (output_tag,
                                                                                tile_type_info['file_extension']),
                                                                   os.path.basename(nbar_dataset_path)
                                                                   )
                                           )
                
            # Copy metadata for eventual inclusion in stack file output
            # This could also be written to the output tile if required
            output_dataset_info = dict(nbar_dataset_info)
            output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find 
            output_dataset_info['band_name'] = '%s with PQA mask applied' % output_tag
            output_dataset_info['band_tag'] = '%s-PQA' % output_tag
            output_dataset_info['tile_layer'] = 1
    
            # Check for existing, valid file
            if self.refresh or not os.path.exists(output_tile_path) or not gdal.Open(output_tile_path):
                gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                output_dataset = gdal_driver.Create(output_tile_path, 
                                                    nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                                    1, nbar_dataset.GetRasterBand(1).DataType,
                                                    tile_type_info['format_options'].split(','))
                assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
                output_dataset.SetGeoTransform(nbar_dataset.GetGeoTransform())
                output_dataset.SetProjection(nbar_dataset.GetProjection()) 
    
                output_band = output_dataset.GetRasterBand(1)
    
                # Calculate NDVI here
                # Remember band indices are one-based
                try:
                    # Read and adjust arrays for NIR and R
                    NIR_array = nbar_dataset.GetRasterBand(lookup.band_no['NIR']).ReadAsArray() * lookup.adjustment_multiplier['NIR'] + lookup.adjustment_offset['NIR'] * SCALE_FACTOR
                    R_array = nbar_dataset.GetRasterBand(lookup.band_no['R']).ReadAsArray() * lookup.adjustment_multiplier['R'] + lookup.adjustment_offset['R'] * SCALE_FACTOR
                except TypeError:   
                    return
                  
                data_array = numpy.true_divide(NIR_array - R_array, NIR_array + R_array) * SCALE_FACTOR
                
                self.apply_pqa_mask(data_array, pqa_mask, no_data_value)
                
                output_band.WriteArray(data_array)
                output_band.SetNoDataValue(no_data_value)
                output_band.FlushCache()
                
                # This is not strictly necessary - copy metadata to output dataset
                output_dataset_metadata = nbar_dataset.GetMetadata()
                if output_dataset_metadata:
                    output_dataset.SetMetadata(output_dataset_metadata) 
                    log_multiline(logger.debug, output_dataset_metadata, 'output_dataset_metadata', '\t')    
                
                output_dataset.FlushCache()
                logger.info('Finished writing %s', output_tile_path)
            else:
                logger.info('Skipped existing, valid dataset %s', output_tile_path)
            
            output_dataset_dict[output_stack_path] = output_dataset_info
#                    log_multiline(logger.debug, output_dataset_info, 'output_dataset_info', '\t')    

        log_multiline(logger.debug, output_dataset_dict, 'output_dataset_dict', '\t')    
        # NDVI dataset processed - return info
        return output_dataset_dict
    

if __name__ == '__main__':
    def date2datetime(input_date, time_offset=time.min):
        if not input_date:
            return None
        return datetime.combine(input_date, time_offset)
    # Stacker class takes care of command line parameters
    ndvi_stacker = NDVIStacker()
    
    
    if ndvi_stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert ndvi_stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert ndvi_stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert ndvi_stacker.output_dir, 'Output directory not specified (-o or --output)'
    
    
    stack_info_dict = ndvi_stacker.stack_derived(x_index=ndvi_stacker.x_index, 
                         y_index=ndvi_stacker.y_index, 
                         stack_output_dir=ndvi_stacker.output_dir, 
                         start_datetime=date2datetime(ndvi_stacker.start_date, time.min), 
                         end_datetime=date2datetime(ndvi_stacker.end_date, time.max), 
                         satellite=ndvi_stacker.satellite, 
                         sensor=ndvi_stacker.sensor)
    
    log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), ndvi_stacker.output_dir)

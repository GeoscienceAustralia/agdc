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
from datetime import datetime, time

from agdc import Stacker
from eotools.utils import log_multiline

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class PQAStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def derive_datasets(self, input_dataset_info, stack_output_info, tile_type_info):
        """ Overrides abstract function in stacker class. Called in Stacker.stack_derived() function. 
        
        Arguments:
            input_dataset_info: Dict keyed by processing level (e.g. ORTHO, NBAR, PQA, DEM)
                containing all tile info which can be used within the function
                A sample is shown below:
                
input_dataset_info = {'NBAR': {'band_name': 'Visible Blue',
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
    'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_NBAR_150_-025_2000-02-09T23-46-12.722217.tif'},
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
     'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_ORTHO_150_-025_2000-02-09T23-46-12.722217.tif'},
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
    'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_PQA_150_-025_2000-02-09T23-46-12.722217.tif'}
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
                      
        Arguments (cont'd):
            tile_type_info: dict containing tile type information. 
                Obtained from stacker object (e.g: stacker.tile_type_info) after instantiation. 
                A sample is shown below
                
tile_type_info = {'crs': 'EPSG:4326',
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
                containing filenames of GDAL-supported output datasets created by this function.
                Note that the key(s) will be used as the output filename for the VRT temporal stack
                and each dataset created must contain only a single band.
        """
        # Replace this with code to do fancy stuff
        # Use the code for the Stacker.derive_datasets() as a template
        return Stacker.derive_datasets(self, input_dataset_info, stack_output_info, tile_type_info)
            

if __name__ == '__main__':
    def date2datetime(input_date, time_offset=time.min):
        if not input_date:
            return None
        return datetime.combine(input_date, time_offset)
    
    # Stacker class takes care of command line parameters
    pqa_stacker = PQAStacker()
    
    if pqa_stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert pqa_stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert pqa_stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert pqa_stacker.output_dir, 'Output directory not specified (-o or --output)'
    
    
    stack_info_dict = pqa_stacker.stack_derived(x_index=pqa_stacker.x_index, 
                         y_index=pqa_stacker.y_index, 
                         stack_output_dir=pqa_stacker.output_dir, 
                         start_datetime=date2datetime(pqa_stacker.start_date, time.min), 
                         end_datetime=date2datetime(pqa_stacker.end_date, time.max), 
                         satellite=pqa_stacker.satellite, 
                         sensor=pqa_stacker.sensor)
    
    log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), pqa_stacker.output_dir)

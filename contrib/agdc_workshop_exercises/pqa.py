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

from agdc.stacker import Stacker
from EOtools.utils import log_multiline

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
          
# Creates derived datasets by getting masking all NBAR datasets in a stack with PQA data          
class PQAStacker(Stacker):

    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
        assert type(input_dataset_dict) == dict, 'input_dataset_dict must be a dict'
        
        log_multiline(logger.debug, input_dataset_dict, 'input_dataset_dict', '\t')    
       
        # Figure out our input/output files
        nbar_dataset_path = input_dataset_dict['NBAR']['tile_pathname']
        output_tile_path = os.path.join(self.output_dir, re.sub('\.\w+$', 
                                                                '_pqa_masked%s' % (tile_type_info['file_extension']),
                                                                os.path.basename(nbar_dataset_path)))
        output_stack_path = os.path.join(self.output_dir, 'pqa_masked.vrt')
        
        nbar_dataset = gdal.Open(nbar_dataset_path)
        assert nbar_dataset, 'Unable to open dataset %s' % nbar_dataset
        total_bands = nbar_dataset.RasterCount
        logger.debug('Opened NBAR dataset %s', nbar_dataset_path)
                
        # Copy metadata for eventual inclusion in stack file output
        # This could also be written to the output tile if required
        output_dataset_info = dict(input_dataset_dict['NBAR'])
        output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find 
        output_dataset_info['band_name'] = 'NBAR with PQA mask applied' 
        output_dataset_info['band_tag'] = 'NBAR-PQA' 
        output_dataset_info['tile_layer'] = 1

        # Get the pixel mask as a single numpy array
        # Be mindful of memory usage, should be fine in this instance
        pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
        
        # Create a new geotiff for the masked output
        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
        output_dataset = gdal_driver.Create(output_tile_path, 
                                            nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                            1, nbar_dataset.GetRasterBand(1).DataType,
                                            tile_type_info['format_options'].split(','))
        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
        output_dataset.SetGeoTransform(nbar_dataset.GetGeoTransform())
        output_dataset.SetProjection(nbar_dataset.GetProjection()) 
        
        # Mask our band (each band is a numpy array of values)
        input_band = nbar_dataset.GetRasterBand(1)
        input_band_data = input_band.ReadAsArray()
        
        # Apply the mask in place on input_band_data
        no_data_value = -32767
        self.apply_pqa_mask(input_band_data, pqa_mask, no_data_value)
        
        # Write the data as a new band
        output_band = output_dataset.GetRasterBand(1)
        output_band.WriteArray(input_band_data)
        output_band.SetNoDataValue(no_data_value)
        output_band.FlushCache()
            
         
        # This is not strictly necessary - copy metadata to output dataset
        output_dataset_metadata = nbar_dataset.GetMetadata()
        if output_dataset_metadata:
            output_dataset.SetMetadata(output_dataset_metadata) 
        
        output_dataset.FlushCache()
        logger.info('Finished writing %s', output_tile_path)
        
        output_dataset_dict = {}
        output_dataset_dict[output_stack_path] = output_dataset_info

        log_multiline(logger.debug, output_dataset_dict, 'output_dataset_dict', '\t')    
        
        return output_dataset_dict
    
# This is the main function when this script is directly executed - You can mostly
# ignore it's contents. The bulk of the "interesting work" is in the above class
if __name__ == '__main__':
    def date2datetime(input_date, time_offset=time.min):
        if not input_date:
            return None
        return datetime.combine(input_date, time_offset)
    
    # Stacker class takes care of command line parameters
    stacker = PQAStacker()
    
    if stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert (stacker.x_index and stacker.y_index), 'You must specify Tile X/Y-index (-x/-y or --x_index/--y_index)'
    assert stacker.output_dir, 'Output directory not specified (-o or --output)'
    
    
    stack_info_dict = stacker.stack_derived(x_index=stacker.x_index, 
                         y_index=stacker.y_index, 
                         stack_output_dir=stacker.output_dir, 
                         start_datetime=date2datetime(stacker.start_date, time.min), 
                         end_datetime=date2datetime(stacker.end_date, time.max), 
                         satellite=stacker.satellite, 
                         sensor=stacker.sensor)
    
    log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), stacker.output_dir)

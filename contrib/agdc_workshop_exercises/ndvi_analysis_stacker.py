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
import EOtools.stats.temporal_stats

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
        assert type(input_dataset_dict) == dict, 'input_dataset_dict must be a dict'
        
        log_multiline(logger.debug, input_dataset_dict, 'input_dataset_dict', '\t')    

        output_dataset_dict = {}
        nbar_dataset_info = input_dataset_dict['NBAR'] # Only need NBAR data for NDVI
        
        nbar_dataset_path = nbar_dataset_info['tile_pathname']
        

        # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
        pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
        
        nbar_dataset = gdal.Open(nbar_dataset_path)
        assert nbar_dataset, 'Unable to open dataset %s' % nbar_dataset
        logger.debug('Opened NBAR dataset %s', nbar_dataset_path)
        
        #no_data_value = nbar_dataset_info['nodata_value']
        no_data_value = -32767 # Need a value outside the scaled range -10000 - +10000
                                
        output_stack_path = os.path.join(self.output_dir, 'NDVI_pqa_masked.vrt')
                
        output_tile_path = os.path.join(self.output_dir, re.sub('\.\w+$', 
                                                               '_NDVI%s' % (tile_type_info['file_extension']),
                                                               os.path.basename(nbar_dataset_path)
                                                               )
                                       )
            
        # Copy metadata for eventual inclusion in stack file output
        # This could also be written to the output tile if required
        output_dataset_info = dict(nbar_dataset_info)
        output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find 
        output_dataset_info['band_name'] = 'NDVI with PQA mask applied'
        output_dataset_info['band_tag'] = 'NDVI-PQA'
        output_dataset_info['tile_layer'] = 1

        # NBAR bands into 2D NumPy arrays. 
        near_ir_band_data = nbar_dataset.GetRasterBand(4).ReadAsArray() # Near Infrared light
        visible_band_data = nbar_dataset.GetRasterBand(3).ReadAsArray() # Red Visible Light

        # Calculate NDVI for every element in the array using
        # ((NIR - VIS) / (NIR + VIS)) * SCALE_FACTOR
        # HINT - Use numpy.true_divide(numerator, denominator) to avoid divide by 0 errors
        data_array = numpy.zeros((tile_type_info['x_pixels'], tile_type_info['y_pixels'])) # Replace this with your NDVI calculation
        
        
        self.apply_pqa_mask(data_array, pqa_mask, no_data_value)
        
        # Create our output file
        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
        output_dataset = gdal_driver.Create(output_tile_path, 
                                            nbar_dataset.RasterXSize, nbar_dataset.RasterYSize,
                                            1, nbar_dataset.GetRasterBand(1).DataType,
                                            tile_type_info['format_options'].split(','))
        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
        output_dataset.SetGeoTransform(nbar_dataset.GetGeoTransform())
        output_dataset.SetProjection(nbar_dataset.GetProjection()) 
        output_band = output_dataset.GetRasterBand(1)
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
        
        output_dataset_dict[output_stack_path] = output_dataset_info

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
    assert (ndvi_stacker.x_index and ndvi_stacker.y_index), 'You must specify Tile X/Y-index (-x/-y or --x_index/--y_index)'
    assert ndvi_stacker.output_dir, 'Output directory not specified (-o or --output)'
    
    # Create derived datasets
    stack_info_dict = ndvi_stacker.stack_derived(x_index=ndvi_stacker.x_index, 
                         y_index=ndvi_stacker.y_index, 
                         stack_output_dir=ndvi_stacker.output_dir, 
                         start_datetime=date2datetime(ndvi_stacker.start_date, time.min), 
                         end_datetime=date2datetime(ndvi_stacker.end_date, time.max), 
                         satellite=ndvi_stacker.satellite, 
                         sensor=ndvi_stacker.sensor)
    
    log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), ndvi_stacker.output_dir)
    
    # Create statistics on derived datasets
    logger.info('Beginning creation of statistics')
    for vrt_stack_path in stack_info_dict:
        # Find a place to write the stats
        stats_dataset_path = vrt_stack_path.replace('.vrt', '_stats_envi')
        
        # Calculate and write the stats
        temporal_stats_numexpr_module.main(vrt_stack_path, stats_dataset_path,
                                               noData=stack_info_dict[vrt_stack_path][0]['nodata_value'],
                                               provenance=True)
                                               
        logger.info('Finished creating stats file %s', stats_dataset_path)

'''
Created on 21/02/2013

@author: u76345
'''
import os
import sys
import logging
import re
from datetime import datetime, time
from time import sleep
import gc
import numpy
from osgeo import gdal, gdalconst

from agdc.stacker import Stacker
from EOtools.utils import log_multiline

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)

# Doesn't do any useful work, just prints the contents of all matched files
class ListingStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
    
        logger.info("List of files are as follows:")
        
        for key in input_dataset_dict:
            ds_info = input_dataset_dict[key]
            logger.info("[%s]", key)
	    if 'start_datetime' in ds_info:
                logger.info("for daterange (%s -> %s). Path: %s", ds_info['start_datetime'], ds_info['end_datetime'], ds_info['tile_pathname'])
        
        return None # We aren't deriving any datasets - just listing the stack contents

        
        
# This is the main function when this script is directly executed - You can mostly
# ignore it's contents. The bulk of the "interesting work" is in the above class
if __name__ == '__main__':

    def date2datetime(input_date, time_offset=time.min):
        if not input_date:
            return None
        return datetime.combine(input_date, time_offset)
        
                     
    # Main function starts here
    # Stacker class takes care of command line parameters
    stacker = ListingStacker()
    
    if stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert (stacker.x_index and stacker.y_index), 'You must specify Tile X/Y-index (-x/-y or --x_index/--y_index)'
    assert stacker.output_dir, 'Output directory not specified (-o or --output)'
    assert not os.path.exists(stacker.output_dir) or os.path.isdir(stacker.output_dir), 'Invalid output directory specified (-o or --output)'
    stacker.output_dir = os.path.abspath(stacker.output_dir)
    
    # Begin stacking
    stack_info_dict = stacker.stack_derived(x_index=stacker.x_index, 
                            y_index=stacker.y_index, 
                            stack_output_dir=stacker.output_dir, 
                            start_datetime=date2datetime(stacker.start_date, time.min), 
                            end_datetime=date2datetime(stacker.end_date, time.max), 
                            satellite=stacker.satellite, 
                            sensor=stacker.sensor)
        
    logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), stacker.output_dir)
    



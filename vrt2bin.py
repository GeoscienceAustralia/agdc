'''
Created on 27/02/2013

@author: u76345
'''

import re
import sys
import logging
import os

from ULA3.utils import log_multiline
from edit_envi_hdr import edit_envi_hdr
from ULA3.utils import execute

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
def vrt2bin(input_vrt_path, output_dataset_path=None,
            file_format='ENVI', file_extension='_envi', format_options=None,
            layer_name_list=None, no_data_value=None, 
            overwrite=False, debug=False):
    if debug:
        console_handler.setLevel(logging.DEBUG)
        
    logger.debug('vrt2bin(input_vrt_path=%s, output_dataset_path=%s, file_format=%s, file_extension=%s, format_options=%s, layer_name_list=%s, no_data_value=%s, debug=%s) called' %
        (input_vrt_path, output_dataset_path,
        file_format, file_extension, format_options,
        layer_name_list, no_data_value, debug))
        
    assert output_dataset_path or file_extension, 'Output path or file extension must be provided'
    
    # Derive the output dataset path if it wasn't provided
    if not output_dataset_path:
        output_dataset_path = re.sub('\.\w+$', file_extension, input_vrt_path)
        
    if os.path.exists(output_dataset_path) and not overwrite:
        logger.info('Skipped existing dataset %s', output_dataset_path)
        return output_dataset_path
    
    command_string = 'gdal_translate'
    if not debug:
        command_string += ' -q'
        
    command_string += ' -of %s' % file_format
        
    if format_options:
        for format_option in format_options.split(','):
            command_string += ' -co %s' % format_option     
            
    command_string += ' %s %s' % (input_vrt_path, output_dataset_path)
                                                                                
    logger.debug('command_string = %s', command_string)

    result = execute(command_string=command_string)

    if result['stdout']:
        log_multiline(logger.info, result['stdout'], 'stdout from ' + command_string, '\t') 

    if result['returncode']:
        log_multiline(logger.error, result['stderr'], 'stderr from ' + command_string, '\t')
        raise Exception('%s failed', command_string) 
                
    if layer_name_list and file_format == 'ENVI':
        edit_envi_hdr(envi_file=output_dataset_path, 
                      noData=no_data_value, 
                      band_names=layer_name_list)
        
    return output_dataset_path    
        
if __name__ == '__main__':
    pass

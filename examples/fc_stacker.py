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

Hacked version of landsat_tiler.py to ingest FC datasets. 
This is NOT really how it should be done.
'''
import os
import sys
import logging
import re
import numpy
from datetime import datetime, time
from osgeo import gdal, gdalconst
from time import sleep

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
                
class FCStacker(Stacker):
    """ Subclass of Stacker
    Used to implement specific functionality to create stacks of derived datasets.
    """
    def derive_datasets(self, input_dataset_dict, stack_output_info, tile_type_info):
        """ Overrides abstract function in stacker class. Called in Stacker.stack_derived() function. 
        Creates PQA-masked NDVI stack
        
        Arguments:
            fc_dataset_dict: Dict keyed by processing level (e.g. ORTHO, FC, PQA, DEM)
                containing all tile info which can be used within the function
                A sample is shown below (including superfluous band-specific information):
                
{
'FC': {'band_name': 'Visible Blue',
    'band_tag': 'B10',
    'end_datetime': datetime.datetime(2000, 2, 9, 23, 46, 36, 722217),
    'end_row': 77,
    'level_name': 'FC',
    'nodata_value': -999L,
    'path': 91,
    'satellite_tag': 'LS7',
    'sensor_name': 'ETM+',
    'start_datetime': datetime.datetime(2000, 2, 9, 23, 46, 12, 722217),
    'start_row': 77,
    'tile_layer': 1,
    'tile_pathname': '/g/data/v10/datacube/EPSG4326_1deg_0.00025pixel/LS7_ETM/150_-025/2000/LS7_ETM_FC_150_-025_2000-02-09T23-46-12.722217.tif',
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
        
        def create_rgb_tif(input_dataset_path, output_dataset_path, pqa_mask=None, rgb_bands=None, 
                           input_no_data_value=-999, output_no_data_value=0,
                           input_range=()):
            if os.path.exists(output_dataset_path):
                logger.info('Output dataset %s already exists - skipping', output_dataset_path)
                return
            
            if not self.lock_object(output_dataset_path):
                logger.info('Output dataset %s already locked - skipping', output_dataset_path)
                return
            
            if not rgb_bands:
                rgb_bands = [3, 1, 2]
                
            scale_factor = 10000.0 / 255.0 # Scale factor to translate from +ve int16 to byte
            
            input_gdal_dataset = gdal.Open(input_dataset_path) 
            assert input_gdal_dataset, 'Unable to open input dataset %s' % (input_dataset_path)
        
            try:
                # Create multi-band dataset for masked data
                logger.debug('output_dataset path = %s', output_dataset_path)
                gdal_driver = gdal.GetDriverByName('GTiff')
                log_multiline(logger.debug, gdal_driver.GetMetadata(), 'gdal_driver.GetMetadata()')
                output_gdal_dataset = gdal_driver.Create(output_dataset_path, 
                    input_gdal_dataset.RasterXSize, input_gdal_dataset.RasterYSize,
                    len(rgb_bands), gdal.GDT_Byte, ['INTERLEAVE=PIXEL']) #['INTERLEAVE=PIXEL','COMPRESS=NONE','BIGTIFF=YES'])
                assert output_gdal_dataset, 'Unable to open input dataset %s' % output_dataset_path
                output_gdal_dataset.SetGeoTransform(input_gdal_dataset.GetGeoTransform())
                output_gdal_dataset.SetProjection(input_gdal_dataset.GetProjection())
                
                dest_band_no = 0
                for source_band_no in rgb_bands:
                    dest_band_no += 1  
                    logger.debug('Processing source band %d, destination band %d', source_band_no, dest_band_no)
                    input_band_array = input_gdal_dataset.GetRasterBand(source_band_no).ReadAsArray()
                    input_gdal_dataset.FlushCache()
                    
                    output_band_array = (input_band_array / scale_factor).astype(numpy.byte)
                    
                    output_band_array[numpy.logical_or((input_band_array < 0), (input_band_array > 10000))] = output_no_data_value # Set any out-of-bounds values to no-data
                    
                    if pqa_mask is not None: # Need to perform masking
                        output_band_array[numpy.logical_or((input_band_array == input_no_data_value), ~pqa_mask)] = output_no_data_value # Apply PQA mask and no-data value
                    else:
                        output_band_array[(input_band_array == input_no_data_value)] = output_no_data_value # Re-apply no-data value
                    
                    output_band = output_gdal_dataset.GetRasterBand(dest_band_no)
                    output_band.SetNoDataValue(output_no_data_value)
                    output_band.WriteArray(output_band_array)
                    output_band.FlushCache()
                    
                output_gdal_dataset.FlushCache()
            finally:
                self.unlock_object(output_dataset_path)



                
        dtype = {'FC_PV' : gdalconst.GDT_Int16,
                 'FC_NPV' : gdalconst.GDT_Int16,
                 'FC_BS' : gdalconst.GDT_Int16}

        no_data_value = {'FC_PV' : -999,
                         'FC_NPV' : -999,
                         'FC_BS' : -999}
    
        log_multiline(logger.debug, input_dataset_dict, 'input_dataset_dict', '\t')    
       
        # Test function to copy ORTHO & FC band datasets with pixel quality mask applied
        # to an output directory for stacking

        output_dataset_dict = {}
        fc_dataset_info = input_dataset_dict['FC'] # Only need FC data for NDVI
        #thermal_dataset_info = input_dataset_dict['ORTHO'] # Could have one or two thermal bands
        
        if fc_dataset_info is None:
            logger.info('FC dataset does not exist')
            return 
        
        fc_dataset_path = fc_dataset_info['tile_pathname']
        
        if input_dataset_dict['PQA'] is None:
            logger.info('PQA dataset for %s does not exist', fc_dataset_path)
            return 
        
        # Get a boolean mask from the PQA dataset (use default parameters for mask and dilation)
        pqa_mask = self.get_pqa_mask(input_dataset_dict['PQA']['tile_pathname']) 
        
        fc_dataset = gdal.Open(fc_dataset_path)
        assert fc_dataset, 'Unable to open dataset %s' % fc_dataset
        
        band_array = None;
        # List of outputs to generate from each file
        output_tag_list = ['FC_PV', 'FC_NPV', 'FC_BS']
        input_band_index = 0
        for output_tag in output_tag_list: 
        # List of outputs to generate from each file
            # TODO: Make the stack file name reflect the date range                    
            output_stack_path = os.path.join(self.output_dir, 
                                             re.sub(r'\+', '', '%s_%+04d_%+04d' % (output_tag,
                                                                                   stack_output_info['x_index'],
                                                                                    stack_output_info['y_index'])))
                                                                                    
            if stack_output_info['start_datetime']:
                output_stack_path += '_%s' % stack_output_info['start_datetime'].strftime('%Y%m%d')
            if stack_output_info['end_datetime']:
                output_stack_path += '_%s' % stack_output_info['end_datetime'].strftime('%Y%m%d')
                
            output_stack_path += '_pqa_stack.vrt'
            
            output_tile_path = os.path.join(self.output_dir, re.sub(r'\.\w+$', tile_type_info['file_extension'],
                                                                    re.sub('FC', 
                                                                           output_tag,
                                                                           os.path.basename(fc_dataset_path)
                                                                           )
                                                                   )
                                           )
                
            # Copy metadata for eventual inclusion in stack file output
            # This could also be written to the output tile if required
            output_dataset_info = dict(fc_dataset_info)
            output_dataset_info['tile_pathname'] = output_tile_path # This is the most important modification - used to find tiles to stack
            output_dataset_info['band_name'] = '%s with PQA mask applied' % output_tag
            output_dataset_info['band_tag'] = '%s-PQA' % output_tag
            output_dataset_info['tile_layer'] = 1
            output_dataset_info['nodata_value'] = no_data_value[output_tag]

            # Check for existing, valid file
            if self.refresh or not os.path.exists(output_tile_path):

                if self.lock_object(output_tile_path): # Test for concurrent writes to the same file
                    try:
                        # Read whole fc_dataset into one array. 
                        # 62MB for float32 data should be OK for memory depending on what else happens downstream
                        if band_array is None:
                            band_array = fc_dataset.ReadAsArray()

                            # Re-project issues with PQ. REDO the contiguity layer.
                            non_contiguous = (band_array < 0).any(0)
                            pqa_mask[non_contiguous] = False
                                                
                        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                        #output_dataset = gdal_driver.Create(output_tile_path, 
                        #                                    fc_dataset.RasterXSize, fc_dataset.RasterYSize,
                        #                                    1, fc_dataset.GetRasterBand(1).DataType,
                        #                                    tile_type_info['format_options'].split(','))
                        output_dataset = gdal_driver.Create(output_tile_path, 
                                                            fc_dataset.RasterXSize, fc_dataset.RasterYSize,
                                                            1, dtype[output_tag],
                                                            tile_type_info['format_options'].split(','))
                        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
                        output_dataset.SetGeoTransform(fc_dataset.GetGeoTransform())
                        output_dataset.SetProjection(fc_dataset.GetProjection()) 
            
                        output_band = output_dataset.GetRasterBand(1)
            
                        # Calculate each output here
                        # Remember band_array indices are zero-based

                        data_array = band_array[input_band_index].copy()
                                            
                        if no_data_value[output_tag]:
                            self.apply_pqa_mask(data_array=data_array, pqa_mask=pqa_mask, no_data_value=no_data_value[output_tag])
                        
                        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])
                        #output_dataset = gdal_driver.Create(output_tile_path, 
                        #                                    fc_dataset.RasterXSize, fc_dataset.RasterYSize,
                        #                                    1, fc_dataset.GetRasterBand(1).DataType,
                        #                                    tile_type_info['format_options'].split(','))
                        output_dataset = gdal_driver.Create(output_tile_path, 
                                                            fc_dataset.RasterXSize, fc_dataset.RasterYSize,
                                                            1, dtype[output_tag],
                                                            tile_type_info['format_options'].split(','))
                        assert output_dataset, 'Unable to open output dataset %s'% output_dataset                                   
                        output_dataset.SetGeoTransform(fc_dataset.GetGeoTransform())
                        output_dataset.SetProjection(fc_dataset.GetProjection()) 
            
                        output_band = output_dataset.GetRasterBand(1)
            
                        output_band.WriteArray(data_array)
                        output_band.SetNoDataValue(output_dataset_info['nodata_value'])
                        output_band.FlushCache()
                        
                        # This is not strictly necessary - copy metadata to output dataset
                        output_dataset_metadata = fc_dataset.GetMetadata()
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
            input_band_index += 1
#                    log_multiline(logger.debug, output_dataset_info, 'output_dataset_info', '\t') 
            # End of loop  
 
        fc_rgb_path = os.path.join(self.output_dir, re.sub(r'\.\w+$', '.tif', # Write to .tif file
                                                                re.sub(r'^LS\d_[^_]+_', '', # Remove satellite & sensor reference to allow proper sorting by filename
                                                                       re.sub('FC', # Write to FC_RGB file
                                                                              'FC_RGB',
                                                                              os.path.basename(fc_dataset_path)
                                                                              )
                                                                       )
                                                               )
                                       )
                
        logger.info('Creating FC RGB output file %s', fc_rgb_path)
        create_rgb_tif(input_dataset_path=fc_dataset_path, output_dataset_path=fc_rgb_path, pqa_mask=pqa_mask)
        
        log_multiline(logger.debug, output_dataset_dict, 'output_dataset_dict', '\t')    

        # Datasets processed - return info
        return output_dataset_dict
    

if __name__ == '__main__':
    def assemble_stack(fc_stacker):    
        """
        returns stack_info_dict - a dict keyed by stack file name containing a list of tile_info dicts
        """
        def date2datetime(input_date, time_offset=time.min):
            if not input_date:
                return None
            return datetime.combine(input_date, time_offset)
            
        stack_info_dict = fc_stacker.stack_derived(x_index=fc_stacker.x_index, 
                             y_index=fc_stacker.y_index, 
                             stack_output_dir=fc_stacker.output_dir, 
                             start_datetime=date2datetime(fc_stacker.start_date, time.min), 
                             end_datetime=date2datetime(fc_stacker.end_date, time.max), 
                             satellite=fc_stacker.satellite, 
                             sensor=fc_stacker.sensor)
        
        log_multiline(logger.debug, stack_info_dict, 'stack_info_dict', '\t')
        
        logger.info('Finished creating %d temporal stack files in %s.', len(stack_info_dict), fc_stacker.output_dir)
        return stack_info_dict
    
            
    def calc_stats(fc_stacker, stack_info_dict):
        stats_dataset_path_dict = {}
        for vrt_stack_path in sorted(stack_info_dict.keys()):
            stack_list = stack_info_dict[vrt_stack_path]
            
            stats_dataset_path = vrt_stack_path.replace('.vrt', '_stats_envi')
            stats_dataset_path_dict[vrt_stack_path] = stats_dataset_path
            
            if os.path.exists(stats_dataset_path) and not fc_stacker.refresh:
                logger.info('Skipping existing stats file %s', stats_dataset_path)
                continue
            
            logger.info('Creating stats file %s', stats_dataset_path)
            temporal_stats.main(vrt_stack_path, stats_dataset_path, 
                                               noData=stack_list[0]['nodata_value'], 
                                               #================================
                                               # xtile=fc_stacker.tile_type_dict[fc_stacker.default_tile_type_id]['x_pixels'], # Full tile width
                                               # ytile=fc_stacker.tile_type_dict[fc_stacker.default_tile_type_id]['y_pixels'], # Full tile height
                                               #================================
                                               ytile=200, # Full width x 200 rows
                                               provenance=True # Create two extra bands for datetime and satellite provenance
                                               )
            logger.info('Finished creating stats file %s', stats_dataset_path)
            
        logger.info('Finished calculating %d temporal summary stats files in %s.', len(stats_dataset_path_dict), fc_stacker.output_dir)
        return stats_dataset_path_dict
        
    def update_stats_metadata(fc_stacker, stack_info_dict, stats_dataset_path_dict):
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
            
        logger.info('Finished updating metadata in %d temporal summary stats files in %s.', len(stats_dataset_path_dict), fc_stacker.output_dir)
            
    def remove_intermediate_files(fc_stacker, stack_info_dict):
        """ Function to remove intermediate band files after stats have been computed
        """
        removed_file_count = 0
        for vrt_stack_path in sorted(stack_info_dict.keys()):           
            stack_list = stack_info_dict[vrt_stack_path]
            
            # Remove all derived quantity tiles (referenced by VRT)
            for tif_dataset_path in [tile_info['tile_pathname'] for tile_info in stack_list]:
                fc_stacker.remove(tif_dataset_path)
                logger.debug('Removed file %s', tif_dataset_path)
                fc_stacker.remove(tif_dataset_path + '.aux.xml')
                logger.debug('Removed file %s', tif_dataset_path + '.aux.xml')
               
            # Remove stack VRT file
            fc_stacker.remove(vrt_stack_path)
            logger.debug('Removed file %s', vrt_stack_path)
            fc_stacker.remove(vrt_stack_path + '.aux.xml')
            logger.debug('Removed file %s', vrt_stack_path + '.aux.xml')
            
            removed_file_count += len(stack_list) + 1
             
            #===================================================================
            # # Remove all Envi stack files except for NDVI   
            # if not re.match('NDVI', stack_list[0]['band_tag']): 
            #    fc_stacker.remove(envi_dataset_path_dict[vrt_stack_path])
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path])
            #    fc_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.hdr')
            #    fc_stacker.remove(envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    logger.debug('Removed file %s', envi_dataset_path_dict[vrt_stack_path] + '.aux.xml')
            #    removed_file_count += 1
            #===================================================================
                
        logger.info('Finished removing %d intermediate files in %s.', 
                    removed_file_count, 
                    fc_stacker.output_dir)
        
                     
    # Main function starts here
    # Stacker class takes care of command line parameters
    fc_stacker = FCStacker()
    
    if fc_stacker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    # Check for required command line parameters
    assert fc_stacker.x_index, 'Tile X-index not specified (-x or --x_index)'
    assert fc_stacker.y_index, 'Tile Y-index not specified (-y or --y_index)'
    assert fc_stacker.output_dir, 'Output directory not specified (-o or --output)'
    assert not os.path.exists(fc_stacker.output_dir) or os.path.isdir(fc_stacker.output_dir), 'Invalid output directory specified (-o or --output)'
    fc_stacker.output_dir = os.path.abspath(fc_stacker.output_dir)
    
    stack_info_dict = assemble_stack(fc_stacker)
    
    if not stack_info_dict:
        logger.info('No tiles to stack. Exiting.')
        exit(0)
    
    stats_dataset_path_dict = calc_stats(fc_stacker, stack_info_dict)
    update_stats_metadata(fc_stacker, stack_info_dict, stats_dataset_path_dict)

    #===========================================================================
    # if not fc_stacker.debug:
    #    remove_intermediate_files(fc_stacker, stack_info_dict)
    #===========================================================================
    



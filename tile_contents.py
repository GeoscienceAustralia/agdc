"""
TileContents: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging
import os
import re
import cube_util
from cube_util import DatasetError
import numpy as np
# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
#Constants for PQA mosaic formation:
#
#PQA has valid data if this bit is set
PQA_CONTIGUITY_BIT = 8
#For a mosaiced tile, if a pixel has the contiguity bit unset in all componenet
#tiles, then set it to PQA_NODATA_VALUE in the mosaiced tile
PQA_NODATA_VALUE = 16127

class TileContents(object):
    """TileContents database interface class."""
    
    def __init__(self, tile_root, tile_type_info,
                 tile_footprint, band_stack): 
        """set the tile_footprint over which we want to resample this dataset.
        """
        self.tile_type_id = tile_type_info['tile_type_id']
        self.tile_type_info = tile_type_info
        self.tile_footprint = tile_footprint
        self.band_stack = band_stack
        x_index, y_index = tile_footprint
        tile_output_root = \
            os.path.join(\
            tile_root, tile_type_info['tile_directory'],
            '%s_%s' %(band_stack.dataset_mdd['satellite_tag']+
                      re.sub('\W', '',
                             band_stack.dataset_mdd['sensor_name'])))
        
        tile_output_dir = \
            os.path.join(tile_output_root, 
                         re.sub('\+', '', '%+04d_%+04d' 
                                % (tile_footprint[0], tile_footprint[1])),
                         '%04d' % band_stack.dataset_mdd['start_datetime'].year
                         )

        self.tile_output_path = \
            os.path.join(
            tile_output_dir,
            '_'.join([band_stack.dataset_mdd['satellite_tag'], 
                      re.sub('\W', '', band_stack.dataset_mdd['sensor_name']),
                      band_stack.dataset_mdd['processing_level'],
                      re.sub('\+', '', '%+04d_%+04d' % (x_index, y_index)),
                      re.sub(':', '-', band_stack. \
                                 dataset_mdd['start_datetime'].isoformat())
                      ]) + tile_type_info['file_extension']
            )
        #Set the provisional tile location to be the same as the vrt created
        #for the scenes
        self.temp_tile_output_path = \
                        os.path.join(os.path.dirname(self.band_stack.vrt_name),
                                     os.path.basename(self.tile_output_path))

    def reproject(self):
        """Reproject the scene dataset into tile coordinate reference system
        and extent. This method uses gdalwarp to do the reprojection."""
        x_origin = self.tile_type_info['x_origin']
        y_origin = self.tile_type_info['y_origin']
        x_size = self.tile_type_info['x_size']
        y_size = self.tile_type_info['y_size']
        x_pixel_size = self.tile_type_info['x_pixel_size']
        y_pixel_size = self.tile_type_info['y_pixel_size']
        x0 = x_origin + self.tile_footprint[0] * x_size
        y0 = y_origin + self.tile_footprint[1] * y_size
        tile_extents = (x0, y0, x0 + x_size, y0 + y_size)
        nodata_value = self.band_stack.nodata_list[0]
        #Assume resampling method is the same for all bands, this is
        #because resampling_method is per proessing_level
        #TODO assert this is the case
        first_file_number = self.band_stack.band_dict.keys()[0]
        resampling_method = self.band_stack.band_dict[first_file_number] \
                                     ['resampling_method']
        if nodata_value is not None:
            #TODO: Check this works for PQA, where
            #band_dict[10]['resampling_method'] == None
            nodata_spec = " -srcnodata %d -dstnodata %d" %(nodata_value,
                                                           nodata_value)
        else:
            nodata_spec = ""
        format_spec = ""
        for format_option in self.tile_type_info['format_options'].split(','):
            format_spec = '%s -co %s' % (format_spec, format_option)
                                    
        reproject_cmd = ["gdalwarp -q",
                         " -t_srs %s" % self.tile_type_info['crs'],
                         " -te %f %f %f %f" % tile_extents,
                         "-tr %f %f" %(x_pixel_size, y_pixel_size),
                         "-tap -tap",
                         "-r %s" % resampling_method,
                         nodata_spec,
                         format_spec, 
                         "-overwrite %s %s" % (self.band_stack.vrt_name,
                                               self.temp_tile_output_path)
                         ]
        result = cube_util.execute(reproject_cmd)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalwarp: ' + ###test commit
                               '"%s" failed: %s' % (reproject_cmd,
                         result['stderr']))
                                    

    def has_data(self):
        """Check if the reprojection gave rise to a tile with valid data.

        Open the file and check if there is data"""
        tile_dataset = gdal.Open(self.temp_tile_output_path)
        data = tile_dataset.ReadAsArray()
        if len(data.shape) == 2:
            data = data[None, :]
        if data.shape[0] != len(self.band_stack.band_dict):
            raise DatasetError("Number of layers (%d) in tile file\n %s\n" \
                               "does not match number of bands (%d) from " \
                               "database." %(data.shape[0],
                                             self.temp_tile_output_path,
                                             len(self.band_stack.band_dict)))
        for file_number in self.band_stack.band_dict:
            nodata_val = self.band_stack.band_dict[file_number]['nodata_value']
            if nodata_val == None:
                if self.band_stack.band_dict[file_number] \
                                            ['level_name'] == 'PQA':
                    #Check if any pixel has the contiguity bit set
                    if (np.bitwise_and(data,
                                       1 << PQA_CONTIGUITY_BIT) > 0).any():
                        return True
                else:
                    #nodata_value of None means all array data is valid
                    return True
            else:
                if (data != nodata_val).any():
                    return True
        #If all comparisons have shown that all array contents are nodata:
        return False

    def remove(self):
        pass

    #
    #Methods that mosaic several tiles together
    #
    def make_pqa_mosaic_tile(self, tile_dict_list, mosaic_pathname):
        """From the PQA tiles in tile_dict_list, create a mosaic tile
        at mosaic_pathname.
        
        For a given pixel, the algorithm is as follows:
        1. If the pixel has contiguity bit unset in all component tiles, then
        set the pixel's value to PQA_NODATA_VALUE.
        2. For a pixel with the contiguity bit set in at least one component
        tile, the mosaic result of Bit n is 1 if and only if it is 1 on all
        componenet tiles for which the contiguity bit is set."""

        template_dataset = gdal.Open(tile_dict_list[0]['tile_pathname'])
        gdal_driver = gdal.GetDriverByName(self.tile_type_info['file_format'])
        #Set datatype formats appropriate to Create() and numpy
        gdal_dtype = template_dataset.GetRasterBand(1).DataType
        numpy_dtype = gdal.GetDataTypeName(gdal_dtype)
        mosaic_dataset = \
                gdal_driver.Create(mosaic_pathname,
                                   template_dataset.RasterXSize,
                                   template_dataset.RasterYSize,
                                   1, gdal_dtype,
                                   self.tile_type_info['format_options']
                                   .split(','))

        if not mosaic_dataset:
            raise DatasetError('Unable to open output dataset %s'
                               % output_dataset)
        
        mosaic_dataset.SetGeoTransform(template_dataset.GetGeoTransform())
        mosaic_dataset.SetProjection(template_dataset.GetProjection())

        output_band = mosaic_dataset.GetRasterBand(1)
        data_array=np.zeros(shape=(mosaic_dataset.RasterYSize,
                                   mosaic_dataset.RasterXSize),
                            dtype=numpy_dtype)
        data_array[...] = -1 # Set all background values to FFFF
    
        overall_data_mask = np.zeros(shape=(mosaic_dataset.RasterYSize, 
                                            mosaic_dataset.RasterXSize),
                                     dtype=np.bool)
        del template_dataset

        # Populate data_array with -masked PQA data
        for pqa_dataset_index in range(len(tile_dict_list)):
            pqa_dataset_path = tile_dict_list[pqa_dataset_index]['tile_pathname']
            pqa_dataset = gdal.Open(pqa_dataset_path)
            if not pqa_dataset:
                raise DatasetError('Unable to open %s' % pqa_dataset_path)
            pqa_array = pqa_dataset.ReadAsArray()
            del pqa_dataset
            # Set all data-containing pixels to true in data_mask
            pqa_data_mask = np.bitwise_and(pqa_array,
                                           1 << PQA_CONTIGUITY_BIT) > 0
            # Update overall_data_mask to true for all valid-data pixels
            overall_data_mask = overall_data_mask | pqa_data_mask
            # At those pixels where this source tile contains valid data,
            # update the mosaiced array with this source tile's PQ results,
            # setting to the mosaic value to 0 if it is 0 in this source tile.
            data_array[pqa_data_mask] = \
                        np.bitwise_and(data_array[pqa_data_mask],
                                       pqa_array[pqa_data_mask])
        # Set all pixels which don't contain data to PQA_NO_DATA_VALUE
        data_array[~overall_data_mask] = PQA_NO_DATA_VALUE
        output_band.WriteArray(data_array)  
        mosaic_dataset.FlushCache()
    
    @staticmethod
    def make_mosaic_vrt(tile_dict_list, mosaic_pathname):
        """From two or more source tiles create a vrt"""
        gdalbuildvrt_cmd = ["gdalbuildvrt -q",
                            "-overwrite",
                            "%s" %mosaic_pathname,
                            "%s" %(" ".join([t['tile_pathname']
                                             for t in tile_dict_list]))]
        result = cube_util.execute(gdalbuildvrt_cmd)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s'\
                               % (gdalbuildvrt_cmd, result['stderr']))

"""
TileContents: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""
import shutil
import logging
import os
import re
import cube_util
from cube_util import DatasetError
from osgeo import gdal
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
PQA_NODATA_VALUE = 0x3EFF

PQA_NO_DATA_BITMASK = 0x01FF
# Contiguity and band saturation bits all one, others zero.
PQA_NO_DATA_CHECK_VALUE = 0x00FF

PQA_CONTIGUITY = 0x0100

class TileContents(object):
    """TileContents database interface class."""
    # pylint: disable=too-many-instance-attributes
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
            '%s_%s' %(band_stack.dataset_mdd['satellite_tag'],
                      re.sub(r'\W', '',
                             band_stack.dataset_mdd['sensor_name'])))

        tile_output_dir = \
            os.path.join(tile_output_root,
                         re.sub(r'\+', '', '%+04d_%+04d'
                                % (tile_footprint[0], tile_footprint[1])),
                         '%04d' % band_stack.dataset_mdd['start_datetime'].year
                         )

        self.tile_output_path = \
            os.path.join(
            tile_output_dir,
            '_'.join([band_stack.dataset_mdd['satellite_tag'],
                      re.sub(r'\W', '', band_stack.dataset_mdd['sensor_name']),
                      band_stack.dataset_mdd['processing_level'],
                      re.sub(r'\+', '', '%+04d_%+04d' % (x_index, y_index)),
                      re.sub(':', '-', band_stack. \
                                 dataset_mdd['start_datetime'].isoformat())
                      ]) + tile_type_info['file_extension']
            )
        #Set the provisional tile location to be the same as the vrt created
        #for the scenes
        self.temp_tile_output_path = \
                        os.path.join(os.path.dirname(self.band_stack.vrt_name),
                                     os.path.basename(self.tile_output_path))
        self.tile_extents = None
        self.mosaic_temp_pathname = None
        self.mosaic_final_pathname = None

    def reproject(self):
        """Reproject the scene dataset into tile coordinate reference system
        and extent. This method uses gdalwarp to do the reprojection."""
        # pylint: disable=too-many-locals
        x_origin = self.tile_type_info['x_origin']
        y_origin = self.tile_type_info['y_origin']
        x_size = self.tile_type_info['x_size']
        y_size = self.tile_type_info['y_size']
        x_pixel_size = self.tile_type_info['x_pixel_size']
        y_pixel_size = self.tile_type_info['y_pixel_size']
        x0 = x_origin + self.tile_footprint[0] * x_size
        y0 = y_origin + self.tile_footprint[1] * y_size
        tile_extents = (x0, y0, x0 + x_size, y0 + y_size)
        # Make the tile_extents visible to tile_record
        self.tile_extents = tile_extents
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
            nodata_spec = ["-srcnodata",
                           "%d" %nodata_value,
                           "-dstnodata",
                           "%d" %nodata_value
                           ]
        else:
            nodata_spec = []
        format_spec = []
        for format_option in self.tile_type_info['format_options'].split(','):
            format_spec.extend(["-co", "%s" %format_option])

        reproject_cmd = ["gdalwarp",
                         "-q",
                         "-t_srs",
                         "%s" %self.tile_type_info['crs'],
                         "-te",
                         "%f" %tile_extents[0],
                         "%f" %tile_extents[1],
                         "%f" %tile_extents[2],
                         "%f" %tile_extents[3],
                         "-tr",
                         "%f" %x_pixel_size,
                         "%f" %y_pixel_size,
                         "-tap",
                         "-tap",
                         "-r",
                         "%s" % resampling_method,
                         ]
        reproject_cmd.extend(nodata_spec)
        reproject_cmd.extend(format_spec)
        reproject_cmd.extend(["-overwrite",
                              "%s" %self.band_stack.vrt_name,
                              "%s" %self.temp_tile_output_path
                              ])
        result = cube_util.execute(reproject_cmd, shell=False)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalwarp: ' +
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
        """Remove tiles that were in coverage but have no data. Also remove
        tiles if we are rolling back the transaction."""
        if os.path.isfile(self.temp_tile_output_path):
            os.remove(self.temp_tile_output_path)

    def make_permanent(self, tile_class_desc=None):
        """Depending on tile_class_desc, Move pre-mosaic tile contents or
        mosaic tile contents to permanent location."""

        if tile_class_desc is None:
            cube_util.create_directory(os.path.dirname(self.tile_output_path))
            shutil.move(self.temp_tile_output_path, self.tile_output_path)
            # Move mosaic if necessary
            if self.mosaic_temp_pathname:
                cube_util.create_directory(os.path.dirname(
                        self.mosaic_final_pathname))
                shutil.move(self.mosaic_temp_pathname,
                            self.mosaic_final_pathname)

        if tile_class_desc == 'pre-mosaic tiles':
            cube_util.create_directory(os.path.dirname(self.tile_output_path))
            shutil.move(self.temp_tile_output_path, self.tile_output_path)

        elif tile_class_desc == 'mosaic tiles':
            if not self.mosaic_temp_pathname:
                return
            # Move mosaic if necessary
            cube_util.create_directory(os.path.dirname(
                    self.mosaic_final_pathname))
            shutil.move(self.mosaic_temp_pathname, self.mosaic_final_pathname)

        else:
            raise AssertionError('Unknown tile_class_desc in ' +
                                 'TileContents.make_permanent')

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
        # pylint: disable=too-many-locals
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
                               % mosaic_dataset)

        mosaic_dataset.SetGeoTransform(template_dataset.GetGeoTransform())
        mosaic_dataset.SetProjection(template_dataset.GetProjection())

        output_band = mosaic_dataset.GetRasterBand(1)
        data_array = np.ones(shape=(mosaic_dataset.RasterYSize,
                                    mosaic_dataset.RasterXSize),
                             dtype=numpy_dtype) * (-1)
        no_data_array = np.zeros(shape=(mosaic_dataset.RasterYSize,
                                        mosaic_dataset.RasterXSize),
                                 dtype=numpy_dtype)

        overall_data_mask = np.zeros(shape=(mosaic_dataset.RasterYSize,
                                            mosaic_dataset.RasterXSize),
                                     dtype=np.bool)
        del template_dataset

        # Populate data_array with -masked PQA data
        for pqa_dataset_index in range(len(tile_dict_list)):
            pqa_dataset_path = \
                tile_dict_list[pqa_dataset_index]['tile_pathname']
            pqa_dataset = gdal.Open(pqa_dataset_path)
            if not pqa_dataset:
                raise DatasetError('Unable to open %s' % pqa_dataset_path)
            pqa_array = pqa_dataset.ReadAsArray()
            del pqa_dataset

            # Treat contiguous and non-contiguous pixels separately
            # Set all contiguous pixels to true in data_mask
            pqa_data_mask = (pqa_array & PQA_CONTIGUITY).astype(np.bool)
            # Expand overall_data_mask to true for any contiguous pixels
            overall_data_mask = overall_data_mask | pqa_data_mask
            # Perform bitwise-and on contiguous pixels in data_array
            data_array[pqa_data_mask] &= pqa_array[pqa_data_mask]
            # Perform bitwise-or on non-contiguous pixels in no_data_array
            no_data_array[~pqa_data_mask] |= pqa_array[~pqa_data_mask]

        # Set all pixels which don't contain data to PQA_NO_DATA_VALUE
        data_array[~overall_data_mask] = no_data_array[~overall_data_mask]
        output_band.WriteArray(data_array)
        mosaic_dataset.FlushCache()

    @staticmethod
    def make_mosaic_vrt(tile_dict_list, mosaic_pathname):
        """From two or more source tiles create a vrt"""
        source_file_list = [t['tile_pathname'] for t in tile_dict_list]
        print source_file_list
        gdalbuildvrt_cmd = ["gdalbuildvrt",
                            "-q",
                            "-overwrite",
                            "%s" %mosaic_pathname]
        gdalbuildvrt_cmd.extend(source_file_list)
        result = cube_util.execute(gdalbuildvrt_cmd, shell=False)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s'\
                               % (gdalbuildvrt_cmd, result['stderr']))

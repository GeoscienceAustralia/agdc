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
#from abstract_ingester import DatasetError

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class TileContents(object):
    """TileContents database interface class."""

    def __init__(self, tile_root, tile_type_info, tile_footprint, band_stack):
        """set the tile_footprint over which we want to resample this dataset.
        """
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
        self.temp_tile_output_path = \
            os.path.join(os.path.dirname(self.band_stack.vrt_name))
                         
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
        #Assume resampling method is the same for all bands, this is the case
        #because resampling_method is per proessing_level
        #TODO assert this is the case
        first_file_number = self.band_stack.band_dict.keys()[0]
        resampling_method = self.band_stack.band_dict[first_file_number]['resampling_method']
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
            raise DatasetError('Unable to perform gdalwarp: ' +
                               '"%s" failed: %s' % (reproject_cmd, result['stderr']))

    def has_data(self):
        pass

    def remove(self):
        pass

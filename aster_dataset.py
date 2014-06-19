"""
    aster_dataset.py - dataset class for ASTER (L1B) datasets.

    This is the implementation of the AbstractDataset class for ASTER
    datasets. At present it only works for level 1 B data.
"""

import os
import logging
import glob
import re

from ULA3.dataset import SceneDataset

import cube_util
from cube_util import DatasetError
from abstract_dataset import AbstractDataset
from aster_bandstack import AsterBandstack
from osgeo import gdal
import osr

#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Class definition
#


class AsterDataset(AbstractDataset):
    """Dataset class for ASTER datasets."""

    # pylint: disable=too-many-public-methods
    #
    # This class provides metadata using acessor functions. This is
    # both straight-forward and allows a docstring to be attached to
    # each to document the definition of the metadata being provided.
    #

    BAND_FILES = {'1' : 'B1-VNIR_Swath.tiff', '2' : 'B2-VNIR_Swath.tiff', '3N' : 'B3N-VNIR_Swath.tiff', '3B' : 'B3B-VNIR_Swath.tiff',
                  '4' : 'B4-SWIR_Swath.tiff', '5' : 'B5-SWIR_Swath.tiff', '6' : 'B6-SWIR_Swath.tiff', '7' : 'B7-SWIR_Swath.tiff', '8' : 'B8-SWIR_Swath.tiff', '9' : 'B9-SWIR_Swath.tiff',
                  '10' : 'B10-TIR_Swath.tiff', '11' : 'B11-TIR_Swath.tiff', '12' : 'B12-TIR_Swath.tiff', '13' : 'B13-TIR_Swath.tiff', '14' : 'B14-TIR_Swath.tiff'}
    PQA_FILES = {'VNIR' : 'BSATURATED-VNIR_Swath.tiff', 
                 'SWIR' : 'BSATURATED-SWIR_Swath.tiff' , 
                 'TIR' : 'BSATURATED-TIR_Swath.tiff'}
    SENSOR_BANDS = {'VNIR' : ['1', '2', '3N', '3B'], 
                    'SWIR' : ['4', '5', '6', '7', '8', '9'], 
                    'TIR' : ['10', '11', '12', '13', '14']}
        
                      
             
    
    
    def __init__(self, dataset_path, original_scene_path):
        """Opens the dataset and extracts metadata.
        
        Needs a reference to the original HDF along with the current dataset.
        This is because the original HDF has a LOT of metadata that doesn't get copied
        to the geotiff's + VRT's
        """
        self._dataset_path = dataset_path
        self._original_scene = gdal.Open(original_scene_path)
        if not self._original_scene:
            raise DatasetError("Unable to open original scene: %s" % original_scene_path)

        self._md = self._original_scene.GetMetadata()
        self._md_list = self._original_scene.GetMetadata_List()
        
        if 'VNIR' in os.path.basename(dataset_path):
            self._sensor = 'VNIR'
        elif 'SWIR' in os.path.basename(dataset_path):
            self._sensor = 'SWIR'
        elif 'TIR' in os.path.basename(dataset_path):
            self._sensor = 'TIR'
        else:
            raise DatasetError("Unable to determine sensor type from: %s" % dataset_path)
        
        self._ds = gdal.Open(dataset_path)
        
        # Calculate spatial bounds
        ds_gtrn = self._ds.GetGeoTransform()
        ds_proj = self._ds.GetProjectionRef()
        ds_srs = osr.SpatialReference(ds_proj)
        geo_srs =ds_srs.CloneGeogCS()
        transform = osr.CoordinateTransformation( ds_srs, geo_srs)

        ds_bbox_cells = (
            (0., 0.),
            (0, self._ds.RasterYSize),
            (self._ds.RasterXSize, self._ds.RasterYSize),
            (self._ds.RasterXSize, 0),
          )

        self._ds_bounds = [] # Top Left, Bottom Left, Bottom Right, Top Right
        for x, y in ds_bbox_cells:
            x2 = ds_gtrn[0] + ds_gtrn[1] * x + ds_gtrn[2] * y
            y2 = ds_gtrn[3] + ds_gtrn[4] * x + ds_gtrn[5] * y
            self._ds_bounds.append((x2, y2))
        
        AbstractDataset.__init__(self)

    #
    # Methods to extract extra metadata
    #


    #
    # Metadata accessor methods
    #

    def get_dataset_path(self):
        """The path to the dataset on disk."""
        return self._dataset_path

    def get_satellite_tag(self):
        """A short unique string identifying the satellite."""
        return self._md['INSTRUMENTSHORTNAME']

    def get_sensor_name(self):
        """A short string identifying the sensor.

        The combination of satellite_tag and sensor_name must be unique.
        """
        return self._sensor

    def get_processing_level(self):
        """A short string identifying the processing level or product.

        The processing level must be unique for each satellite and sensor
        combination.
        """

        return self._md['PROCESSINGLEVELID']

    def get_x_ref(self):
        """The x (East-West axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        
        # Not strictly applicable to ASTER but it's the best we can do without path/row
        return self._md['ASTERSCENEID'].split(', ')[0]

    def get_y_ref(self):
        """The y (North-South axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        # Not strictly applicable to ASTER but it's the best we can do without path/row
        return self._md['ASTERSCENEID'].split(', ')[1]

    def get_start_datetime(self):
        """The start of the acquisition.

        This is a datetime without timezone in UTC.
        """
        
        date_string = sorted([m.replace('SETTINGTIMEOFPOINTING=','') for m in self._md_list if 'SETTINGTIMEOFPOINTING=' in m])[0]
        return datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
         

    def get_end_datetime(self):
        """The end of the acquisition.

        This is a datatime without timezone in UTC.
        """
        date_string = sorted([m.replace('SETTINGTIMEOFPOINTING=','') for m in self._md_list if 'SETTINGTIMEOFPOINTING=' in m])[-1]
        return datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

    def get_datetime_processed(self):
        """The date and time when the dataset was processed or created.

        This is used to determine if that dataset is newer than one
        already in the database, and so should replace it.

        It is a datetime without timezone in UTC.
        """
        date_string = self._md['PRODUCTIONDATETIME']
        return datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

    def get_dataset_size(self):
        """The size of the dataset in kilobytes as an integer."""
        return os.path.getsize(self._dataset_path) / 1000 # Assuming HDD KB (1000 bytes) not 2^10 B

    def get_ll_lon(self):
        """The longitude of the lower left corner of the coverage area."""
        return self._ds_bounds[1][0]

    def get_ll_lat(self):
        """The lattitude of the lower left corner of the coverage area."""
        return self._ds_bounds[1][1]

    def get_lr_lon(self):
        """The longitude of the lower right corner of the coverage area."""
        return self._ds_bounds[2][0]

    def get_lr_lat(self):
        """The lattitude of the lower right corner of the coverage area."""
        return self._ds_bounds[2][1]

    def get_ul_lon(self):
        """The longitude of the upper left corner of the coverage area."""
        return self._ds_bounds[0][0]

    def get_ul_lat(self):
        """The lattitude of the upper left corner of the coverage area."""
        return self._ds_bounds[0][1]

    def get_ur_lon(self):
        """The longitude of the upper right corner of the coverage area."""
        return self._ds_bounds[3][0]

    def get_ur_lat(self):
        """The lattitude of the upper right corner of the coverage area."""
        return self._ds_bounds[0][1]

    def get_projection(self):
        """The coordinate refererence system of the image data."""
        return self._ds.GetProjection()

    def get_ll_x(self):
        """The x coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lon() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_ll_y(self):
        """The y coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lat() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_lr_x(self):
        """The x coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_lr_lon() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_lr_y(self):
        """The y coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lat() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_ul_x(self):
        """The x coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ul_lon() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_ul_y(self):
        """The y coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ul_lat() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_ur_x(self):
        """The x coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ur_lon() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_ur_y(self):
        """The y coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ur_lat() # Data is already WGS:84 so can reuse the lon/lat coords

    def get_x_pixels(self):
        """The width of the dataset in pixels."""
        return self._ds.RasterXSize

    def get_y_pixels(self):
        """The height of the dataset in pixels."""
        return self._ds.RasterYSize

    def get_gcp_count(self):
        """The number of ground control points?"""
        return 0

    def get_mtl_text(self):
        """Text information?"""
        return None

    def get_cloud_cover(self):
        """Percentage cloud cover of the aquisition if available."""
        return None

    def get_xml_text(self):
        """XML metadata text for the dataset if available."""
        return None

    def get_pq_tests_run(self):
        """The tests run for a Pixel Quality dataset.

        This is a 16 bit integer with the bits acting as flags. 1 indicates
        that the test was run, 0 that it was not.
        """
        return None

    #
    # Methods used for tiling
    #

    def get_geo_transform(self):
        """The affine transform between pixel and geographic coordinates.

        This is a list of six numbers describing a transformation between
        the pixel x and y coordinates and the geographic x and y coordinates
        in dataset's coordinate reference system.

        See http://www.gdal.org/gdal_datamodel for details.
        """
        return self._ds.GetGeoTransform()

    def find_band_file(self, file_pattern):
        """Find the file in dataset_dir matching file_pattern and check
        uniqueness.

        Returns the path to the file if found, raises a DatasetError
        otherwise."""

        return self._dataset_path # All of our bands are already stacked inside this dataset

    def stack_bands(self, band_dict):
        """Creates and returns a band_stack object from the dataset.

        band_dict: a dictionary describing the bands to be included in the
        stack.

        PRE: The numbers in the band list must refer to bands present
        in the dataset. This method (or things that it calls) should
        raise an exception otherwise.

        POST: The object returned supports the band_stack interface
        (described below), allowing the datacube to chop the relevent
        bands into tiles.
        """
        return AsterBandstack(self, band_dict)
        #raise NotImplementedError

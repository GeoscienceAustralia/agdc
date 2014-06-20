"""
    abstract_bandstack.py - interface for the bandstack class.

    Different types of dataset will have different versions of this
    class, obtained by sub-classing and overriding the abstract methods.

    It is the responsibility of the stack_bands method of the dataset
    object to instantiate the correct subclass.
"""
import os
import re
from osgeo import gdal
from abstract_bandstack import AbstractBandstack
import cube_util
from cube_util import DatasetError
from collections import OrderedDict

class LandsatBandstack(AbstractBandstack):
    """Landsat subclass of AbstractBandstack class"""
    def __init__(self, dataset, band_dict):
        #Order the band_dict by the file number key
        self.dataset = dataset
        self.band_dict = \
            OrderedDict(sorted(band_dict.items(), key=lambda t: t[0]))
            
        self.vrt_name = self.dataset.get_dataset_path()
        self.dataset_mdd = dataset.get_original_metadata()
        self.source_file_list = None
        self.nodata_list = None
        self.vrt_name = dataset.get_dataset_path()
        self.vrt_band_stack = None

    def buildvrt(self, temp_dir):
        """Given a dataset_record and corresponding dataset, build the vrt that
        will be used to reproject the dataset's data to tile coordinates"""
        pass

    def list_source_files(self):
        """Given the dictionary of band source information, form a list
        of scene file names from which a vrt can be constructed. Also return a
        list of nodata values for use by add_metadata"""
        pass

    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        return self.vrt_name

    def add_metadata(self, vrt_filename):
        """Add metadata and return the open band_stack"""
        band_stack_dataset = gdal.Open(vrt_filename)
        assert band_stack_dataset, 'Unable to open VRT %s' % vrt_filename
        band_stack_dataset.SetMetadata(
            {'satellite': self.dataset.get_satellite_tag().upper(),
             'sensor':  self.dataset.get_sensor_name().upper(),
             'start_datetime': self.dataset.get_start_datetime().isoformat(),
             'end_datetime': self.dataset.get_end_datetime().isoformat(),
             'path': '%03d' % self.dataset.get_x_ref(),
             'row': '%03d' % self.dataset.get_y_ref()}
            )
        
        return band_stack_dataset



















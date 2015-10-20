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

"""
    abstract_bandstack.py - interface for the bandstack class.

    Different types of dataset will have different versions of this
    class, obtained by sub-classing and overriding the abstract methods.

    It is the responsibility of the stack_bands method of the dataset
    object to instantiate the correct subclass.
"""
from __future__ import absolute_import
from osgeo import gdal
from agdc.ingest import AbstractBandstack
from collections import OrderedDict

class ModisBandstack(AbstractBandstack):
    """Modis subclass of AbstractBandstack class"""
    def __init__(self, dataset, band_dict):
        """The bandstack allows for the construction of a list, or stack, of
        bands from the given dataset."""
        super(ModisBandstack, self).__init__(dataset.metadata_dict)
        #Order the band_dict by the file number key
        self.dataset = dataset
        self.band_dict = \
            OrderedDict(sorted(band_dict.items(), key=lambda t: t[0]))
        self.source_file_list = None
        self.nodata_list = None
        self.vrt_name = None
        self.vrt_band_stack = None

    def buildvrt(self, temp_dir):
        """Given a dataset_record and corresponding dataset, build the vrt that
        will be used to reproject the dataset's data to tile coordinates"""

        self.vrt_name = self.dataset._vrt_file
        self.vrt_band_stack = self.dataset._vrt_file

        self.add_metadata(self.vrt_name)

    def list_source_files(self):
        """Given the dictionary of band source information, form a list
        of scene file names from which a vrt can be constructed. Also return a
        list of nodata values for use by add_metadata"""
        pass

    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        #dataset_basename = os.path.basename(self.dataset_mdd['dataset_path'])
        #return os.path.join(vrt_dir, dataset_basename)
        return self.vrt_name

    def add_metadata(self, vrt_filename):
        """Add metadata to the VRT."""
        band_stack_dataset = gdal.Open(vrt_filename)
        assert band_stack_dataset, 'Unable to open VRT %s' % vrt_filename
        band_stack_dataset.SetMetadata(
            {'satellite': self.dataset_mdd['satellite_tag'].upper(),
             'sensor':  self.dataset_mdd['sensor_name'].upper(),
             'start_datetime': self.dataset_mdd['start_datetime'].isoformat(),
             'end_datetime': self.dataset_mdd['end_datetime'].isoformat(),
             'path': '%03d' % self.dataset_mdd['x_ref']}
            )
        self.nodata_list = []
        for band_info in self.band_dict.values():
            band_number = band_info['tile_layer']
            band = band_stack_dataset.GetRasterBand(band_number)
            self.nodata_list.append(band.GetNoDataValue())
            if band.GetNoDataValue() is not None:
                band.SetNoDataValue(band.GetNoDataValue())
            band_stack_dataset.FlushCache()

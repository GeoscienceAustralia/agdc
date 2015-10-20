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
import os
import re
from collections import OrderedDict

from osgeo import gdal
from eotools.execute import execute

from agdc.ingest import AbstractBandstack
from agdc.cube_util import DatasetError, create_directory


class LandsatBandstack(AbstractBandstack):
    """Landsat subclass of AbstractBandstack class"""
    def __init__(self, dataset, band_dict):
        """The bandstack allows for the construction of a list, or stack, of
        bands from the given dataset."""
        super(LandsatBandstack, self).__init__(dataset.metadata_dict)
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

        #Make the list of filenames from the dataset_path/scene01 and each
        #file_number's file_pattern. Also get list of nodata_value.
        self.source_file_list, self.nodata_list = self.list_source_files()
        nodata_value = self.nodata_list[0]
        #TODO: check that this works for PQA where nodata_value is None
        if nodata_value is not None:
            nodata_spec = ["-srcnodata",
                           "%d" %nodata_value,
                           "-vrtnodata",
                           "%d" %(nodata_value)]
        else:
            nodata_spec = []
        #Form the vrt_band_stack_filename.
        #This is done using
        #args = shlex.split(command_line)
        #where command_line is the buildvrt command
        create_directory(temp_dir)
        self.vrt_name = self.get_vrt_name(temp_dir)
        #Build the vrt
        buildvrt_cmd = ["gdalbuildvrt",
                        "-separate",
                        "-q",
                        ]
        buildvrt_cmd.extend(nodata_spec)
        buildvrt_cmd.extend(["-overwrite", "%s" %self.vrt_name])
        buildvrt_cmd.extend(self.source_file_list)
        #for fle in self.source_file_list:
        #    buildvrt_cmd.append(fle)
        #buildvrt_cmd = ' '.join(buildvrt_cmd)
        result = execute(buildvrt_cmd, shell=False)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s'\
                                   % (buildvrt_cmd, result['stderr']))
        #Add the metadata and return the band_stack as a gdal datatset, storing
        #as an attribute of the Bandstack object
        self.vrt_band_stack = self.add_metadata(self.vrt_name)

    def list_source_files(self):
        """Given the dictionary of band source information, form a list
        of scene file names from which a vrt can be constructed. Also return a
        list of nodata values for use by add_metadata"""

        file_list = []
        nodata_list = []
        for file_number in self.band_dict:
            pattern = self.band_dict[file_number]['file_pattern']
            this_file = self.dataset.find_band_file(pattern)
            file_list.append(this_file)
            nodata_list.append(self.band_dict[file_number]['nodata_value'])
        return (file_list, nodata_list)

    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        #dataset_basename = os.path.basename(self.dataset_mdd['dataset_path'])
        #return os.path.join(vrt_dir, dataset_basename)
        level_name = self.dataset_mdd['processing_level']
        satellite = self.dataset_mdd['satellite_tag'].upper()
        sensor = self.dataset_mdd['sensor_name'].upper()
        sensor = re.sub(r'\+', r'', sensor)
        start_datetime = \
            self.dataset_mdd['start_datetime'].date().strftime('%Y%m%d')
        x_ref = self.dataset_mdd['x_ref']
        y_ref = self.dataset_mdd['y_ref']
        vrt_band_stack_basename = '%s_%s_%s_%s_%03d_%03d.vrt' \
            %(level_name, satellite, sensor, start_datetime, x_ref, y_ref)
        return os.path.join(vrt_dir, vrt_band_stack_basename)

    def add_metadata(self, vrt_filename):
        """Add metadata to the VRT."""
        band_stack_dataset = gdal.Open(vrt_filename)
        assert band_stack_dataset, 'Unable to open VRT %s' % vrt_filename
        band_stack_dataset.SetMetadata(
            {'satellite': self.dataset_mdd['satellite_tag'].upper(),
             'sensor':  self.dataset_mdd['sensor_name'].upper(),
             'start_datetime': self.dataset_mdd['start_datetime'].isoformat(),
             'end_datetime': self.dataset_mdd['end_datetime'].isoformat(),
             'path': '%03d' % self.dataset_mdd['x_ref'],
             'row': '%03d' % self.dataset_mdd['y_ref']}
            )
        for band_info in self.band_dict.values():
            #Assume ordering file_number keys also orders the tile_layer
            band_number = band_info['tile_layer']
            band = band_stack_dataset.GetRasterBand(band_number)
            band.SetMetadata \
                ({'name': band_info['band_name'],
                  'filename': self.source_file_list[band_number - 1]})
            if self.nodata_list[band_number - 1] is not None:
                band.SetNoDataValue(self.nodata_list[band_number - 1])
            band_stack_dataset.FlushCache()

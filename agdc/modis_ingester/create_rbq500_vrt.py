#!/usr/bin/env python
'''
Created on

@author:
'''

from __future__ import absolute_import
from __future__ import print_function
import os
from osgeo import gdal
from os.path import basename

from eotools.execute import execute

class VRTCreater:

    def get_NetCDF_list(self, directory):
        result = []
        for path, subdirs, files in os.walk(directory):
           for name in files:
              fileName, fileExtension = os.path.splitext(name)
              if fileExtension == '.nc':
                  result.append(os.path.join(path, name))

        return result

if __name__ == '__main__':

    vrt_creater = VRTCreater()
    dataset_dir = "/g/data/u83/data/modis/datacube/"
    file_list = vrt_creater.get_NetCDF_list(dataset_dir)
#    print file_list
    for file in file_list:
        if not file.endswith("float64.nc"): continue
        print(file)
        fname = os.path.splitext(basename(file))[0]
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        subDataSets = dataset.GetSubDatasets()
        command_string = 'gdalbuildvrt -separate -overwrite '
        command_string += dataset_dir + fname
        command_string += '_rbq500.vrt'
        command_string += ' ' + subDataSets[13][0]
        print(command_string)
        result = execute(command_string=command_string)

#    dataset_size = os.path.getsize(dataset_file)


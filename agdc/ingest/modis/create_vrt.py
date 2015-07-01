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
        command_string += '.vrt'

        command_string += ' ' + subDataSets[14][0] # band 1
        command_string += ' ' + subDataSets[15][0] # band 2
        command_string += ' ' + subDataSets[16][0] # band 3
        command_string += ' ' + subDataSets[17][0] # band 4
        command_string += ' ' + subDataSets[18][0] # band 5
        command_string += ' ' + subDataSets[19][0] # band 6
        command_string += ' ' + subDataSets[20][0] # band 7

        command_string += ' ' + subDataSets[11][0] # band 8
        command_string += ' ' + subDataSets[12][0] # band 9

        command_string += ' ' + subDataSets[3][0] # band 10
        command_string += ' ' + subDataSets[4][0] # band 11
        command_string += ' ' + subDataSets[5][0] # band 12
        command_string += ' ' + subDataSets[6][0] # band 13
        command_string += ' ' + subDataSets[7][0] # band 14
        command_string += ' ' + subDataSets[8][0] # band 15
        command_string += ' ' + subDataSets[9][0] # band 16

        command_string += ' ' + subDataSets[10][0] # band 26

        result = execute(command_string=command_string)
'''
        for i in range(1,len(subDataSets)):
            print subDataSets[i][0]
            command_string += ' '
            command_string += subDataSets[i][0]
        print command_string
'''
#    dataset_size = os.path.getsize(dataset_file)


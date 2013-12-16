'''
Created on 28/06/2013

@author: u76345
'''

from osgeo import gdal
import numpy
import sys
import os

def process(input_vrt_path, output_dir):
    
    water_rgb = (0, 169, 230) 
    
    assert os.path.exists(input_vrt_path), 'Input file %s does not exist' % input_vrt_path
    assert os.path.isdir(output_dir), 'Input output directory %s' % output_dir
    
    input_dataset = gdal.Open(input_vrt_path)
    assert input_dataset, 'Unable to open dataset %s' % input_vrt_path
    
    file_list = input_dataset.GetFileList() 
    
    for band_number in range(1,input_dataset.RasterCount + 1):
        input_band = input_dataset.GetRasterBand(band_number)
        water_mask = (input_band.ReadAsArray() == 128) # True==WET, False==DRY
        
        water_file_path = os.path.join(output_dir,
                                       os.path.basename(file_list[band_number])
                                       )
        
        if os.path.exists(water_file_path):
            print('Skipping existing dataset %s' % water_file_path)
            continue
    
        gdal_driver = gdal.GetDriverByName('GTiff')
        output_dataset = gdal_driver.Create(water_file_path, 
                                            input_dataset.RasterXSize, input_dataset.RasterYSize,
                                            3, gdal.GDT_Byte,
                                            ['INTERLEAVE=PIXEL'])
        
        assert output_dataset, 'Unable to open output dataset %s'% water_file_path   
                                        
        output_dataset.SetGeoTransform(input_dataset.GetGeoTransform())
        output_dataset.SetProjection(input_dataset.GetProjection()) 

        for output_band_index in range(3):
            output_band = output_dataset.GetRasterBand(output_band_index + 1)
            output_array = (water_mask * water_rgb[output_band_index]).astype(numpy.uint8)
            print('output_array = %s' % output_array)
            print('output_array[water_mask] = %s' % output_array[water_mask])
            output_band.WriteArray(output_array)
            output_band.SetNoDataValue(0)
            output_band.FlushCache()
            
        output_dataset.FlushCache()
        print('Finished writing output dataset %s' % water_file_path)
            

if __name__ == '__main__':
    process(sys.argv[1], sys.argv[2])
#!/usr/bin/env python
###############################################################################
# 
# Purpose:  Create a browse image with 1000 pixels wide from RGB TIF input files.
# 
# Fei.Zhang@ga.gov.au
#
# from ~/github/Gdal_examples/thumbnail/browseimg_creator.py
#
###############################################################################

from osgeo import gdal, osr, ogr
import numpy.ma as ma
import numpy as numpy
import sys
import subprocess
from gdalconst import *
import math as math
import os

class BrowseImgCreator:
    def __init__(self):
        
        pass
    
    def setInput_RGBfiles(self, rfile, gfile, bfile, nodata_value):
        self.red_file = rfile
        self.green_file = gfile
        self.blue_file = bfile
        self.nodata = nodata_value #sys.argv[5]
        
        return
    
    def setOut_Browsefile(self, browsefile, cols):
        self.outthumb = browsefile
        self.outcols = cols # int(sys.argv[6])
        return

    def create(self):
        
        # working files
        file_to = "/tmp/RGB.vrt"
        warp_to_file = "/tmp/RGBwarped.vrt" 
        outtif = "/tmp/browseimg.tif"
        
        # Build the RGB Virtual Raster at full resolution
        subprocess.call(["gdalbuildvrt", "-overwrite", "-separate", file_to, self.red_file, self.green_file, self.blue_file])
        
        # Determine the pixel scaling to get the 1024 wide thumbnail
        vrt = gdal.Open(file_to)
        intransform = vrt.GetGeoTransform()
        inpixelx = intransform[1]
        inpixely = intransform[5]
        inrows = vrt.RasterYSize 
        incols = vrt.RasterXSize 
        print incols, inrows
        outresx = (inpixelx * incols) / self.outcols
        outrows = int(math.ceil((float(inrows) / float(incols)) * self.outcols))
        print "jpeg reolution and pixels: %s,%s,%s" % (str(outresx), str(self.outcols), str(outrows))
        
        subprocess.call(["gdalwarp", "-of", "VRT", "-tr", str(outresx), str(outresx), "-r", "near", "-overwrite", file_to, warp_to_file])
        
        # Open VRT file to array
        vrt = gdal.Open(warp_to_file)
        bands = (1, 2, 3)
        
        driver = gdal.GetDriverByName ("GTiff")  
        outdataset = driver.Create(outtif, self.outcols, outrows, 3, GDT_Byte)
        rgb_composite = numpy.zeros((outrows, self.outcols, 3))
        
        # Loop through bands and apply Scale and Offset
        for bandnum, band in enumerate(bands):
            vrtband = vrt.GetRasterBand(band)
            
            vrtband_array = vrtband.ReadAsArray()
            nbits = gdal.GetDataTypeSize(vrtband.DataType)
            
            print "debug nbits=%s" % (str(nbits))
            dfScaleDstMin, dfScaleDstMax = 0.0, 255.0
            # Determine scale limits
            #dfScaleSrcMin = dfBandMean - 2.58*(dfBandStdDev)
            #dfScaleSrcMax = dfBandMean + 2.58*(dfBandStdDev)
            if (nbits == 16):
                count = 31768
                histogram = vrtband.GetHistogram(-32767, 32767, 65536)
            else:
                count = 0
                histogram = vrtband.GetHistogram()
                
            total = 0
            
            cliplower = int(0.01 * (sum(histogram)-histogram[count]))
            clipupper = int(0.99 * (sum(histogram)-histogram[count]))
            #print sum(histogram)
            #print cliplower,clipupper
            #print histogram[31768]
            
            while total < cliplower and count < len(histogram)-1:
                count = count + 1
                total = int(histogram[count]) + total
                dfScaleSrcMin = count
                #print "total",total
            
            if (nbits == 16):
                count = 31768
            else: count = 0
            #print "count for max",count
            total = 0
            while total < clipupper and count < len(histogram)-1:
                count = count + 1
                #print count,clipupper,total
                total = int(histogram[count]) + total
                dfScaleSrcMax = count
            #print "total",total
            #print "dfScaleMin",dfScaleSrcMin
            #print "dfScaleMax",dfScaleSrcMax			
            if (nbits == 16):
                dfScaleSrcMin = dfScaleSrcMin - 32768
                dfScaleSrcMax = dfScaleSrcMax - 32768       
                
                
                
        # Determine gain and offset
            dfScale = (dfScaleDstMax - dfScaleDstMin) / (dfScaleSrcMax - dfScaleSrcMin)
            dfOffset = -1 * dfScaleSrcMin * dfScale + dfScaleDstMin
            
            #Apply gain and offset    
            outdataset.GetRasterBand(band).WriteArray((ma.masked_less_equal(vrtband_array, int(self.nodata)) * dfScale) + dfOffset)
            
            pass  # for loop
        
        outdataset = None
        vrt = None  # do this is necessary to allow close the files and remove warp_to_file below
        
        # GDAL Create doesn't support JPEG so we need to make a copy of the GeoTIFF
        subprocess.call(["gdal_translate", "-of", "JPEG", outtif, self.outthumb])
        
        # Cleanup working VRT files
        os.remove(file_to)
        os.remove(warp_to_file)
        os.remove(outtif)
        # Done
        
        return outresx

#############################################################################################
# Example Usage:
# python browseimg_creator.py 
#    /home/fzhang/SoftLabs/geolab/data/LandsatImages/LT5_20080310_091_077/L5091077_07720080310_B70.TIF 
#    /home/fzhang/SoftLabs/geolab/data/LandsatImages/LT5_20080310_091_077/L5091077_07720080310_B40.TIF 
#    /home/fzhang/SoftLabs/geolab/data/LandsatImages/LT5_20080310_091_077/L5091077_07720080310_B10.TIF 
#     0   LT5_20080310_091_077_test.jpg 1024
# nodata=0 for 8-bits images
# nodata=-999 for 16-bits images
############################################################################################        
if __name__ == "__main__":
# check for correct usage - if not prompt user

    if len(sys.argv) < 7:
        print "*----------------------------------------------------------------*"
        print ""
        print " thumbnail.py computes a linear stretch and applies to input image"
        print ""
        print "*----------------------------------------------------------------*"
        print ""
        print " usage: python %s " % (sys.argv[0])
        print " <red image file> <green image file> <blue image file> <input null value> "
        print " <output image file> <output width in pixels>"
        print ""
        sys.exit(1)

    bimgObj = BrowseImgCreator()  # browse image width pixels = sys.argv[6]
    
    bimgObj.setInput_RGBfiles(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    bimgObj.setOut_Browsefile(sys.argv[5], int(sys.argv[6]))
    
    r = bimgObj.create()

    print "Jpeg resolution is %s" % (str(r))




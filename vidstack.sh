#!/bin/bash

last_tif=''
for rgb in `ls $1 | grep -v _stack`
do
  vrt=`echo $rgb | sed s/\.tif$/_stack.vrt/g`
  tif=`echo $rgb | sed s/\.tif$/_stack.tif/g`
  jpg=`echo $rgb | sed s/\.tif$/_stack.jpg/g`
  if [ ! -f $jpg ]
  then
    echo Creating temporary VRT file $vrt
    gdalbuildvrt -srcnodata 0 -vrtnodata 0 $vrt $last_tif $rgb
#    rm $last_tif
    echo Creating GeoTIFF file $tif
    gdal_translate -of GTiff -co INTERLEAVE=PIXEL -a_nodata 0 $vrt $tif
    rm $vrt
    echo Creating JPEG file $jpg
    gdal_translate -of JPEG -outsize 1080 1080 -a_nodata 0 $tif $jpg
  else
    echo Skipping existing JPEG file $jpg    
  fi
  last_tif=$tif
done

#!/bin/bash

video_dir=$1
shift
translate_params=$@

pushd $video_dir
last_tif=''
for rgb in `ls LS*_RGB.tif | cut -d_ -f4-6 | sort`
do
  rgb_tif=`ls LS*_${rgb}_RGB.tif`
  vrt=${rgb}_stack.vrt
  tif=${rgb}_stack.tif
  png=${rgb}.png
  if [ ! -f $png ]
  then
    if [ ! -f $tif ]
    then
      echo Creating temporary VRT file $vrt
      gdalbuildvrt -srcnodata 0 -vrtnodata 0 $vrt $last_tif $rgb_tif
      echo Creating GeoTIFF file $tif
      gdal_translate -of GTiff -co INTERLEAVE=PIXEL -a_nodata 0 $vrt $tif
      rm $vrt
    else
      echo Skipping existing stack file $tif
    fi
    echo Creating PNG file $png
#    gdal_translate -of PNG -srcwin 2080 1720 1920 1080 -outsize 1920 1080 -a_nodata 0 $tif $png
    gdal_translate -of PNG -a_nodata 0 ${translate_params} $tif $png
  else
    echo Skipping existing PNG file $png
  fi
  last_tif=$tif
done
popd


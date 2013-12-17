#!/bin/bash

video_dir=$1
shift
translate_params=$@

pushd $video_dir
for rgb in `ls LS*_RGB.tif | cut -d_ -f4-6 | sort`
do
  rgb_tif=`ls LS*_${rgb}_RGB.tif`
  png=${rgb}.png
  if [ ! -f $png ]
  then
    echo Creating PNG file $png
#    gdal_translate -of PNG -srcwin 2080 1720 1920 1080 -outsize 1920 1080 -a_nodata 0 $tif $png
    gdal_translate -of PNG -a_nodata 0 ${translate_params} $rgb_tif $png
  else
    echo Skipping existing PNG file $png
  fi
done
popd


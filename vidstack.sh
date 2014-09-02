#!/bin/bash

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither [copyright holder] nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

# Bash script to generate cumulative stack of images for video. Works on outputs from rgb_stacker.py

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


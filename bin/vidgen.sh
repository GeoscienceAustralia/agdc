#!/bin/bash

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

# Bash script to run rgb_stacker.py on several tiles

tile_list=$1
output_dir=$2

#DATACUBE_ROOT=/projects/u46/opt/modules/datacube/0.1.0
export DATACUBE_ROOT=$(readlink -f ${0%/*})
rgb_stacker=${DATACUBE_ROOT}/rgb_stacker.sh
while read line
do
  x_index=`echo $line | cut -d' ' -f1`
  y_index=`echo $line | cut -d' ' -f2`
  echo x=$x_index y=$y_index

  shell_path=video_${x_index}_${y_index}.sh
  cat $rgb_stacker | \
sed s/export\ DATACUBE_ROOT=.*\$/export\ DATACUBE_ROOT=`echo ${DATACUBE_ROOT} | sed s/\\\\//\\\\\\\\\\\\//g`/g | \
sed s/\$@/-x\ $x_index\ -y\ $y_index\ -o\ `echo ${output_dir} | sed s/\\\\//\\\\\\\\\\\\//g`\\/${x_index}_${y_index}/g > $shell_path
  qsub $shell_path
done < ${tile_list}

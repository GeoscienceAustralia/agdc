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

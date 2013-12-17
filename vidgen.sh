#!/bin/bash
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

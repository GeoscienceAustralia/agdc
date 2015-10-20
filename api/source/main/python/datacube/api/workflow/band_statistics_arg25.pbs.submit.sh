#!/bin/bash

# ===============================================================================
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

#PBS_SCRIPT="$HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/band_statistics_arg25.pbs.sh"
PBS_SCRIPT="$(which band_statistics_arg25.pbs.sh)"

if [ $# -ne 5 ] && [ $# -ne 7 ]
then
    echo "Usage is $0 <x min> <x max> <y min> <y max> <output directory> [<x increment> <y increment>]"
    exit -1
fi

xmin=$1
xmax=$2

ymin=$3
ymax=$4

outputdir="$5"

xinc=1
yinc=1

if [ $# -eq 7 ]
then
    xinc=$6
    yinc=$7
fi

echo "Submitting jobs for x=[$xmin to $xmax increment=$xinc] y=[$ymin to $ymax increment=$yinc] to output directory $outputdir..."

for x in $(seq $xmin $xinc $xmax)
do
    for y in $(seq $ymin $yinc $ymax)
    do
        qsub -v outputdir=$outputdir,xmin=$x,xmax=$((x+$xinc-1)),ymin=$y,ymax=$((y+$yinc-1)) ${PBS_SCRIPT}
    done
done

#[ $# -lt 3 ] && echo "Usage is $0 <x> <y> <output directory>" && exit
#
#x=$1
#y=$2
#
#outputdir="$3"
#
#epoch=6
#epoch=5
#
#echo "Submitting jobs for cell [$x/$y] to output directory [$outputdir]..."
#
#for acqmin in $(seq 1985 $((epoch-1)) 2010)
#do
#    for season in SUMMER AUTUMN WINTER SPRING
#    do
#        qsub -v outputdir=$outputdir,season=$season,acqmin=$acqmin,acqmax=$((acqmin+5)),epoch=$epoch,xmin=$x,xmax=$x,ymin=$y,ymax=$y ${PBS_SCRIPT}
#    done
#done

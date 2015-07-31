#!/bin/bash

# ===============================================================================
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
#     * Neither Geoscience Australia nor the names of its contributors may be
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

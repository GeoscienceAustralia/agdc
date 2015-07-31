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

PBS_SCRIPT="$HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/retrieve_dataset_stack_wetness.pbs.sh"

outputdir=$PWD

# Full depth WETNESS and GREENNESS for Leo
qsub -v outputdir=$outputdir,x=128,y=-16,dataset=TCI,band=WETNESS,acqmin=1985,acqmax=2015 ${PBS_SCRIPT}
qsub -v outputdir=$outputdir,x=128,y=-16,dataset=TCI,band=GREENNESS,acqmin=1985,acqmax=2015 ${PBS_SCRIPT}

#xmin=127
#xmax=129
#
#ymin=-17
#ymax=-15
#
#echo "Submitting jobs for the Ord River x=[$xmin to $xmax] y=[$ymin to $ymax] to output directory $outputdir..."
#
#for x in $(seq $xmin $xmax)
#do
#    for y in $(seq $ymin $ymax)
#    do
#        for season in "WET 11 03" "DRY 04 10"
#        do
#            qsub -v acqmin=2006,acqmax=2013,outputdir=$outputdir,x=$x,y=$y,dataset=TCI,band=WETNESS,season="$season" ${PBS_SCRIPT}
#            qsub -v acqmin=2006,acqmax=2013,outputdir=$outputdir,x=$x,y=$y,dataset=WATER,band=WATER,season="$season" ${PBS_SCRIPT}
#            qsub -v acqmin=2006,acqmax=2013,outputdir=$outputdir,x=$x,y=$y,dataset=NDVI,band=NDVI,season="$season" ${PBS_SCRIPT}
#        done
#    done
#done

#xmin=140
#xmax=144
#
#ymin=-35
#ymax=-32
#
#echo "Submitting jobs for the Lower Darling x=[$xmin to $xmax] y=[$ymin to $ymax] to output directory $outputdir..."
#
#for x in $(seq $xmin $xmax)
#do
#    for y in $(seq $ymin $ymax)
#    do
#        qsub -v acqmin=2006,acqmax=2009,outputdir=$outputdir,x=$x,y=$y,dataset=TCI,band=WETNESS ${PBS_SCRIPT}
#        qsub -v acqmin=2006,acqmax=2009,outputdir=$outputdir,x=$x,y=$y,dataset=WATER,band=WATER ${PBS_SCRIPT}
#        qsub -v acqmin=2006,acqmax=2009,outputdir=$outputdir,x=$x,y=$y,dataset=NDVI,band=NDVI ${PBS_SCRIPT}
#
#        qsub -v acqmin=2010,acqmax=2012,outputdir=$outputdir,x=$x,y=$y,dataset=TCI,band=WETNESS ${PBS_SCRIPT}
#        qsub -v acqmin=2010,acqmax=2012,outputdir=$outputdir,x=$x,y=$y,dataset=WATER,band=WATER ${PBS_SCRIPT}
#        qsub -v acqmin=2010,acqmax=2012,outputdir=$outputdir,x=$x,y=$y,dataset=NDVI,band=NDVI ${PBS_SCRIPT}
#    done
#done

### /usr/bin/time -f '\nelapsed %E | kernel %S | user %s | Max RSS %M | Avg %K' python ~/source/agdc/agdc-api/api/source/main/python/datacube/api/tool/retrieve_dataset_stack.py --acq-min 2006 --acq-max 2013 --season WET 11 03 --satellite LS5 LS7 LS8 --mask-pqa-apply --no-ls7-slc-off --no-ls8-pre-wrs2 --x 127 --y -17 --dataset-type TCI --band WETNESS --output-directory $PWD

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

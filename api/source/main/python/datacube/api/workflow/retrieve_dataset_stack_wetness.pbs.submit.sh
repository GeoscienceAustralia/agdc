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

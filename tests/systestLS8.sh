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

#PBS -P v10
#PBS -q express
#PBS -l walltime=00:55:00,mem=2048MB,ncpus=1

MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.5

export PYTHONPATH=$CODE_PATH/.local/lib/python2.7/site-packages:$CODE_PATH:$HOME_PATH/ga-neo-landsat-processor:$PYTHONPATH

module load gdal
module load pyephem
module load numexpr
module load psycopg2

export path=($path $HOME_PATH.local/lib/python2.7/site-packages)
export path=($path $CODE_PATH)

export DATACUBE_ROOT=$CODE_PATH

## python ${DATACUBE_ROOT}/LS8_test_tile_record.py $START_LINE $END_LINE

## python ${DATACUBE_ROOT}/mph_test_dataset_record.py

SCENE_DIR=`sed -n ${START_LINE}p $DATASETS_TO_INGEST`

echo Ingesting $SCENE_DIR

## python ${DATACUBE_ROOT}/acquisition_test.py $START_LINE $END_LINE

python ${DATACUBE_ROOT}/landsat_ingester.py --config ${DATACUBE_ROOT}/datacubeLS8.conf --source $SCENE_DIR --followsymlinks










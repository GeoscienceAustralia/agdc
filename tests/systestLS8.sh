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










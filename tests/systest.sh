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
#PBS -l walltime=00:05:00,mem=4096MB,ncpus=1
#PBS -l wd
#PBS -j oe
#PBS -o 'systest.log'

REPO_NAME=ga-datacube
export SCRIPT_PATH="$(readlink -f $0)"
echo SCRIPT_PATH=$SCRIPT_PATH
export CODE_PATH=${SCRIPT_PATH%/$REPO_NAME/*}/$REPO_NAME
echo CODE_PATH=$CODE_PATH
PROJECT_CODE=v10
echo "PROJECT_CODE=$PROJECT_CODE (hard-coded)"
userID=$(whoami)
echo userID=$userID
HOME_PATH=$(echo `ls -d ~`)
echo HOME_PATH=$HOME_PATH

MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.5

## export PYTHONPATH=/home/651/mxh651/.local/lib/python2.7/site-packages:/home/651/mxh651/datacube/ga-datacube:/home/651/mxh651/datacube/ga-neo-landsat-processor:$PYTHONPATH
## Assume ga-neo-landsat-processor code in same directory as ga-datacube
export PYTHONPATH=$CODE_PATH/.local/lib/python2.7/site-packages:$CODE_PATH:$CODE_PATH../ga-neo-landsat-processor:$PYTHONPATH
echo PYTHONPATH=$PYTHONPATH
module load gdal
module load pyephem
module load numexpr
module load psycopg2


echo PATH1=$PATH
## export PATH=($PATH /home/651/mxh651/.local/lib/python2.7/site-packages)
## export PATH=($PATH /home/651/mxh651/datacube/ga-datacube)



export PATH=$PATH:$CODE_PATH.local/lib/python2.7/site-packages
export PATH=$PATH:$CODE_PATH


## export DATACUBE_ROOT=/home/651/mxh651/datacube/ga-datacube

export DATACUBE_ROOT=$CODE_PATH
echo $DATACUBE_ROOT

## python ${DATACUBE_ROOT}/systest.py systest_multi.conf

python ${DATACUBE_ROOT}/systest.py systest_multi.conf



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



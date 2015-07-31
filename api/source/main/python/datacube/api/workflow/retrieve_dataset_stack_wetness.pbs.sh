#!/bin/bash
#PBS -N wetness_stack
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=2,mem=4GB
#PBS -l walltime=24:00:00
#PBS -l wd

##PBS -q normal
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module unload python
module load python/2.7.6
module load enum34
module load psutil
module load psycopg2
module load gdal
module load luigi-mpi
module load numpy
module load scipy


### /usr/bin/time -f '\nelapsed %E | kernel %S | user %s | Max RSS %M | Avg %K' python ~/source/agdc/agdc-api/api/source/main/python/datacube/api/tool/retrieve_dataset_stack.py --acq-min 2006 --acq-max 2013 --season WET 11 03 --satellite LS5 LS7 LS8 --mask-pqa-apply --no-ls7-slc-off --no-ls8-pre-wrs2 --x 127 --y -17 --dataset-type TCI --band WETNESS --output-directory $PWD

# GDAL settings
export GDAL_CACHEMAX=1073741824
export GDAL_SWATH_SIZE=1073741824
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE

export PYTHONPATH=$HOME/source/agdc/agdc-api/api/source/main/python:$PYTHONPATH
#COMMAND="python $HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/band_stack_arg25.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax"
#COMMAND="python $HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/tool/retrieve_dataset_stack.py --acq-min $acqmin --acq-max $acqmax --satellite LS5 LS7 LS8 --mask-pqa-apply --no-ls7-slc-off --no-ls8-pre-wrs2 --x $x --y $y --dataset-type $dataset --band $band --output-directory $outputdir"
COMMAND="python $HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/tool/retrieve_dataset_stack.py --acq-min $acqmin --acq-max $acqmax --satellite LS5 LS7 LS8 --mask-pqa-apply --x $x --y $y --dataset-type $dataset --band $band --output-directory $outputdir"

[ ! -z "$season" ] && COMMAND="$COMMAND --season $season"

${COMMAND}
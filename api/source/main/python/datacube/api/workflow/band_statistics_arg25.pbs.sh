#!/bin/bash
#PBS -N arg25stats
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=16,mem=32GB
#PBS -l walltime=24:00:00
#PBS -l wd

##PBS -q normal
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

#module unload python
#module load python/2.7.6
#module load enum34
#module load psutil
#module load psycopg2
#module unload gdal
#module load gdal/1.11.1-python
#module load luigi-mpi
#module load numpy
#module load scipy

module unload python
module unload gdal
module unload numpy
module unload scipy
module unload openmpi

module load agdc-api/0.1.0-b20150722-DEWNR

#export PYTHONPATH=$HOME/source/agdc-api/api-examples/source/main/python:$HOME/source/agdc-api/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH
#COMMAND="python $HOME/source/agdc-api/api-examples/source/main/python/bare_soil.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax"

# TODO make params (e.g. season) optional

# GDAL settings
export GDAL_CACHEMAX=1073741824
export GDAL_SWATH_SIZE=1073741824
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE

#export PYTHONPATH=$HOME/source/agdc/agdc-api/api/source/main/python:$PYTHONPATH
#COMMAND="python $HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/band_statistics_arg25.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax --epoch $epoch --season $season"
#COMMAND="python $HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/band_statistics_arg25.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax"

COMMAND="band_statistics_arg25.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --chunk-size-x 1000 --chunk-size-y 1000"

${COMMAND}

#!/bin/bash
#PBS -N soil
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=8,mem=16GB
#PBS -l wd
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module unload python
module load python/2.7.6
module load enum34
module load psutil
module load psycopg2
module load gdal
module load luigi-mpi
module load numpy/1.9.1

#module load agdc-api

#export PYTHONPATH=$HOME/source/agdc-api/api-examples/source/main/python:$HOME/source/agdc-api/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH
#COMMAND="python $HOME/source/agdc-api/api-examples/source/main/python/bare_soil.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax"

export PYTHONPATH=$HOME/source/agdc-api-stable/api-examples/source/main/python:$HOME/source/agdc-api-stable/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH
COMMAND="python $HOME/source/agdc-api-stable/api-examples/source/main/python/workflow/wetness.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax --mask-pqa-apply"

# MPI
mpirun -n 8 $COMMAND

## NO MPI
#$COMMAND --local-scheduler
#!/bin/bash
#PBS -N mosaic
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=16,mem=32GB
#PBS -l wd
#PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module unload python
module load python/2.7.6
module load psycopg2
module load gdal
module load luigi-mpi

export PYTHONPATH=$HOME/source/agdc-api/api-examples/source/main/python:$HOME/source/agdc-api/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH

#module unload python
#module load agdc-api

COMMAND="python $HOME/source/agdc-api/api-examples/source/main/python/landsat_mosaic.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax"
[ "${pqfilter}" == "false" ] && COMMAND="${COMMAND} --skip-pq"
[ -n "${satellites}" ] && COMMAND="${COMMAND}  --satellites $satellites"

# MPI
mpirun -n 16 $COMMAND

# NO MPI
#$COMMAND --local-scheduler

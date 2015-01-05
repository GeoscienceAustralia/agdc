#!/bin/bash
#PBS -N obscount
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=16,mem=32GB
#PBS -l wd
#PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

#module unload python
#module load agdc-api

module unload python
module load python/2.7.6
module load psycopg2
module load gdal
module load luigi

export PYTHONPATH=$HOME/source/agdc-api/api-examples/source/main/python:$HOME/source/agdc-api/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH

COMMAND="python $HOME/source/agdc-api/api-examples/source/main/python/observation_count.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --year-min $yearmin --year-max $yearmax"

# MPI
mpirun -n 16 $COMMAND

# NO MPI
#$COMMAND --local-scheduler
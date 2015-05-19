#!/bin/bash
#PBS -N witl
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=16,mem=32GB
#PBS -l walltime=1:00:00
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

#module load agdc-api

#export PYTHONPATH=$HOME/source/agdc-api/api-examples/source/main/python:$HOME/source/agdc-api/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH
#COMMAND="python $HOME/source/agdc-api/api-examples/source/main/python/bare_soil.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax"

export PYTHONPATH=$HOME/source/agdc-api/witl/api/source/main/python:$HOME/source/agdc-api/witl/api-examples/source/main/python:$PYTHONPATH
COMMAND="python $HOME/source/agdc-api/witl/api-examples/source/main/python/workflow/wetness_with_stats.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax --mask-pqa-apply --chunk-size-x 2000 --chunk-size-y 200 --csv"

# MPI
#mpirun -n 16 $COMMAND

# NO MPI
${COMMAND} --local-scheduler --workers 16
#! /bin/bash

WORKDIR=/short/v10/fxz547/CleanPixel

mkdir -p $WORKDIR/input

echo "working dir is created $WORKDIR/input"

ls $WORKDIR/input

# modules and environment vars required
module load luigi-mpi
SRCROOT=/home/547/fxz547/github
export PYTHONPATH=$SRCROOT/agdc/api/source/main/python:$SRCROOT/eo-tools:$PYTHONPATH
export AGDC_API_CONFIG=$HOME/.datacube/config

python prepare_csv.py --x-min 110 --x-max 155 --y-min -45 --y-max -10 --acq-min 2013 --acq-max 2015 --satellite LS8 --season APR_TO_OCT --output-directory $WORKDIR/input


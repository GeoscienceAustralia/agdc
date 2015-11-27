#!/usr/bin/env bash

# USAGE: qsub clean_pixel_statistics_6.pbs.sh

#PBS -N apr_to_oct
#PBS -P u46
#PBS -q normal
#PBS -l walltime=20:00:00,ncpus=320,mem=643GB
#PBS -l wd

#DIR=/g/data/u46/bb/output/clean_pixel
#node_count=201

#module load python/2.7.6  gdal/1.9.2  
#module load datacube  eo-tools luigi-mpi
#module load agdc-api

DIR=/short/v10/fxz547/CleanPixel
node_count=20

for node in $(seq 1 $node_count)
do
    cells=$(cat $DIR/input_cells/cell.$node.txt)
    echo $cells
    echo node=$node cell=$cells
    pbsdsh -n $((node*16)) -- bash -l -c "cd $DIR ; export GDAL_CACHEMAX=1073741824 ;  export GDAL_SWATH_SIZE=1073741824 ;  export GDAL_DISABLE_READDIR_ON_OPEN=TRUE ;  source /projects/u46/venvs/agdc/bin/activate ; export PYTHONPATH=/home/547/fxz547/github/agdc/api/source/main/python:$PYTHONPATH; \\
    python $HOME/github/agdc/api/source/main/python/datacube/api/workflow2/clean_pixel_statistics_6.py --acq-min 2013 --acq-max 2015 --season APR_TO_OCT --satellite LS8 --statistic PERCENTILE_10 PERCENTILE_50 PERCENTILE_90 --mask-pqa-apply $cells --dataset-type ARG25 --output-directory $DIR " &
done

echo "Waiting for jobs to finish..."
wait

echo "done"

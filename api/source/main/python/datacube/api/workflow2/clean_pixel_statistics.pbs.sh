#!/usr/bin/env bash

#PBS -N apr_to_sep
#PBS -P u46
#PBS -q normal
#PBS -l walltime=01:00:00,ncpus=3216,mem=6432GB
#PBS -l wd

DIR=/g/data/u46/sjo/output/clean_pixel_poster_6_month

node_count=201

for node in $(seq 1 $node_count)
do
    cells=$(cat $DIR/input_cells/cell.$node.txt)
    echo node=$node cell=$cells
    pbsdsh -n $((node*16)) -- bash -l -c "cd $DIR ; \\
        export GDAL_CACHEMAX=1073741824 ; \\
        export GDAL_SWATH_SIZE=1073741824 ; \\
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE ; \\
        source $HOME/bin/setup-agdc-api-arg25-index-statistics ; \\
        python $HOME/source/agdc/agdc-api-arg25-index-statistics/api/source/main/python/datacube/api/workflow2/clean_pixel_statistics.py --acq-min 2013 --acq-max 2015 --season APR_TO_SEP --satellite LS8 --mask-pqa-apply $cells --dataset-type ARG25 --output-directory $DIR" &
done

echo "Waiting for jobs to finish..."
wait

echo "done"
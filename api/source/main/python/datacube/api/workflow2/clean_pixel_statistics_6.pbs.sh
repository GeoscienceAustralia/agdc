#!/usr/bin/env bash

#PBS -N apr_to_oct
#PBS -P u46
#PBS -q normal
#PBS -l walltime=03:00:00,ncpus=3216,mem=6432GB
#PBS -l wd

DIR=/g/data/u46/bb/output/clean_pixel

node_count=201

for node in $(seq 1 $node_count)
do
    cells=$(cat $DIR/input_cells/cell.$node.txt)
    echo node=$node cell=$cells
    pbsdsh -n $((node*16)) -- bash -l -c "cd $DIR ; \\
        export GDAL_CACHEMAX=1073741824 ; \\
        export GDAL_SWATH_SIZE=1073741824 ; \\
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE ; \\
        source /projects/u46/venvs/agdc/bin/activate ; \\
        python $HOME/source/agdc/api/source/main/python/datacube/api/workflow2/clean_pixel_statistics_6.py --acq-min 2013 --acq-max 2015 --season APR_TO_OCT --satellite LS8 --statistic PERCENTILE_10 PERCENTILE_50 PERCENTILE_90 --mask-pqa-apply $cells --dataset-type ARG25 --output-directory $DIR" &
done

echo "Waiting for jobs to finish..."
wait

echo "done"

#!/usr/bin/env bash

#PBS -N 1987-2015
#PBS -P u46
#PBS -q normal
#PBS -l walltime=20:00:00,ncpus=64,mem=256GB
#PBS -l wd

APPS_FILE=$(which band_statistics_all.py)
DIR=/g/data/u46/bb/output/clean_pixel/workflow
cd $DIR

        export GDAL_CACHEMAX=1073741824 ; \\
        export GDAL_SWATH_SIZE=1073741824 ; \\
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE ; \\
	source /projects/u46/venvs/agdc/bin/activate ; \\ 
	 python $APPS_FILE --x-min 142 --x-max 143 --y-min -33 --y-max -32 --output-directory $PWD --season FINANCIAL_YEAR --satellite LS5 LS7 LS8 --dataset-type TCI --band WETNESS --statistic COUNT_OBSERVED MIN MAX MEAN STANDARD_DEVIATION VARIANCE PERCENTILE_5 PERCENTILE_10 PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95 --chunk-size-x 4000 --chunk-size-y 400 --epoch 1 1 --acq-min 1987 --acq-max 2015 --workers 16 --file-per-statistic

echo "Waiting for jobs to finish..."
wait

echo "done"

#!/bin/bash
#PBS -N ndvi_stats
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=16,mem=30GB
#PBS -l walltime=05:00:00
#PBS -l wd

##PBS -q normal
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module unload python
module unload gdal
module unload numpy
module unload scipy
module unload openmpi

module load agdc-api/0.1.0-b20151023

# GDAL settings
export GDAL_CACHEMAX=1073741824
export GDAL_SWATH_SIZE=1073741824
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE

COMMAND="band_statistics_arg25.py --x-min $x --x-max $x --y-min $y --y-max $y --output-directory $outputdir --season FINANCIAL_YEAR --satellite LS5 LS7 LS8 --dataset-type NDVI --band NDVI --statistic COUNT_OBSERVED MIN MAX MEAN STANDARD_DEVIATION VARIANCE PERCENTILE_5 PERCENTILE_10 PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95 --chunk-size-x 4000 --chunk-size-y 400 --epoch 1 1 --acq-min 1987 --acq-max 2014 --workers 16 --file-per-statistic"

${COMMAND}

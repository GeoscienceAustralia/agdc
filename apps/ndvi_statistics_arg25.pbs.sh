#!/bin/bash
#PBS -N ndvi_stats
#PBS -P u46
#PBS -q  normal 
#PBS -l ncpus=8,mem=16GB
#PBS -l walltime=06:00:00
#PBS -l wd

##PBS -q normal
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.6
module load gdal/1.10.1

module load enum34
module load psutil
module load psycopg2
module load numpy
module load scipy
module load luigi-mpi

#module load gdal/1.11.1 
#module load gdal/1.11.1-python

SRCROOT=/home/547/fxz547/github
export PYTHONPATH=$SRCROOT/agdc/api/source/main/python:$SRCROOT/eo-tools:$PYTHONPATH
export PATH=$SRCROOT/agdc/api/source/main/python/datacube/api/workflow:$PATH
export AGDC_API_CONFIG=$HOME/.datacube/config


# GDAL settings
export GDAL_CACHEMAX=1073741824
export GDAL_SWATH_SIZE=1073741824
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE


echo "...............begin testing environment..."
module list

python -V
python -c "import gdal"

echo $PYTHONPATH
echo "...............end testing environment..."

COMMAND="band_statistics_arg25.py --x-min $x --x-max $x --y-min $y --y-max $y --output-directory $outputdir --season FINANCIAL_YEAR --satellite LS5 LS7 LS8 --dataset-type NDVI --band NDVI --statistic COUNT_OBSERVED MIN MAX MEAN STANDARD_DEVIATION VARIANCE PERCENTILE_10 PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 --chunk-size-x 4000 --chunk-size-y 400 --epoch 1 1 --acq-min 1987 --acq-max 2000 --workers 8 --file-per-statistic"

${COMMAND}

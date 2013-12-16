#!/bin/bash
#PBS -P v27
#PBS -q normal
#PBS -l walltime=48:00:00,mem=4096MB,ncpus=1
#PBS -l wd
#@#PBS -m e
#PBS -M alex.ip@ga.gov.au

MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.5
module load datacube
module load gdal
module load pyephem
module load numexpr
module load ga-neo-landsat-processor
module load psycopg2

#python /projects/v10/datacube/fc_tiler.py --config=fc_tiler.conf $@
export DATACUBE_ROOT=$(readlink -f ${0%/*})

python ${DATACUBE_ROOT}/fc_tiler.py --config=${DATACUBE_ROOT}/fc_tiler.conf $@

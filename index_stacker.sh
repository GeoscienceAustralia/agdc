#!/bin/bash
#PBS -P v10
#PBS -q normal
#PBS -l walltime=08:00:00,mem=12GB,ncpus=1
#PBS -l wd
#@#PBS -m e
##PBS -M alex.ip@ga.gov.au

MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.5
module load datacube
module load gdal
module load pyephem
module load numexpr
module load ga-neo-landsat-processor
module load psycopg2

#python /home/547/axi547/datacube/index_stacker.py -x 144 -y -36 -o /short/v10/mdb_indices $@
export DATACUBE_ROOT=$(readlink -f ${0%/*})

python ${DATACUBE_ROOT}/index_stacker.py $@

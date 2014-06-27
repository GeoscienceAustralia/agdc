#!/bin/bash
#PBS -P v10
#PBS -q express
#PBS -l walltime=00:20:00,mem=2048MB,ncpus=1

MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module load python/2.7.5

export PYTHONPATH=$CODE_PATH/.local/lib/python2.7/site-packages:$CODE_PATH:$HOME_PATH/ga-neo-landsat-processor:$PYTHONPATH

module load gdal
module load pyephem
module load numexpr
module load psycopg2

export path=($path $HOME_PATH.local/lib/python2.7/site-packages)
export path=($path $CODE_PATH)

## export DATACUBE_ROOT=$CODE_PATH

## python ${DATACUBE_ROOT}/LS8_test_tile_record.py $START_LINE $END_LINE

## python ${DATACUBE_ROOT}/mph_test_dataset_record.py

# #python ${DATACUBE_ROOT}/acquisition_test.py $START_LINE $END_LINE
echo SYSTEST_DIR=$SYSTEST_DIR
echo SCENE_DIR=$SCENE_DIR

python ${DATACUBE_ROOT}/landsat_ingester.py --config $SYSTEST_DIR/datacube.conf --source $SCENE_DIR --followsymlinks

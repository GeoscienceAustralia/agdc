#!/bin/bash
#PBS -N clean_pyramid
#PBS -P u46
#PBS -q normal
#PBS -l ncpus=2,mem=8GB
#PBS -l wd
#PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH

module unload python
module load python/2.7.6
module load gdal

gdal_retile.py -v -r near -ps 4000 4000 -co "TILED=YES" -pyramidOnly -levels 6 -targetDir $dir $dir/*.tif

#!/bin/bash
#PBS -P v10
#PBS -q normal
#PBS -l walltime=24:00:00,mem=4GB,ncpus=1
#PBS -l wd
#@#PBS -m e
#PBS -M alex.ip@ga.gov.au

export PYTHONPATH=/home/547/axi547/temp_datacube/EO_tools:/home/547/axi547/temp_datacube/ga-datacube:/projects/u46/opt/modules/psycopg2/2.5.1/lib/python2.7/site-packages:/projects/u46/opt/modules/ga-neo-landsat-processor/3.1.0:/projects/u46/opt/modules/numexpr/2.2/lib/python2.7/site-packages:/projects/u46/opt/modules/pyephem/3.7.5.1/lib/python2.7/site-packages:/apps/gdal/1.9.2/lib/python2.7/site-packages:/apps/gdal/1.9.2/bin

export PATH=/home/547/axi547/temp_datacube/ga-datacube:/apps/gdal/1.9.2/bin:/apps/python/2.7.5/bin:/apps/openmpi/wrapper:/apps/openmpi/1.6.3/bin:/opt/bin:/bin:/usr/bin:/opt/pbs/default/bin:.:/projects/u46/opt/modules/openev/1.8.0/bin

scene_root='/g/data1/rs0/scenes'
scene_types='ARG25_V0.0 PQA25_V0.0 FC25_V0.0'
years='2013 2014'
paths='109'
rows='081 082'

for dataset in $(\
for scene_type in ${scene_types}
do
  for year in ${years}
  do
    for path in ${paths}
    do
      for row in ${rows}
      do
        find ${scene_root}/${scene_type}/${year}*  -type d -name "LS8_*_${path}_${row}_*"
      done
    done
  done
done | sort)
do
  landsat_ingester.sh -C=/home/547/axi547/temp_datacube/ga-datacube/ls8_test.conf --source $dataset --debug
done

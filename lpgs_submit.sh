#!/bin/bash
#PBS -P v10
#PBS -q normal
#PBS -l walltime=08:00:00,vmem=4096MB,ncpus=1
#PBS -wd
#@#PBS -m e
#PBS -M alex.ip@ga.gov.au

rm -rf /g/data/v10/tmp/dbupdater/g_data_v10_L1_*

for year in 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000
do
  for month in 12 11 10 09 08 07 06 05 04 03 02 01
  do
    cat dbupdater | sed s/\$@/--debug\ --source=\\/g\\/data\\/v10\\/L1\\/${year}-${month}/g > lpgs_updater_${year}-${month}
    chmod 770  lpgs_updater_${year}-${month}
    qsub  lpgs_updater_${year}-${month}

  done
done

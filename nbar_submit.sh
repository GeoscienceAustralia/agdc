#!/bin/bash
#PBS -P v10
#PBS -q normal
#PBS -l walltime=08:00:00,mem=4096MB,ncpus=1
#PBS -l wd
#@#PBS -m e
#PBS -M alex.ip@ga.gov.au

rm -rf /g/data/v10/tmp/dbupdater/g_data_v10_NBAR_*

for year in 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000
do
  for month in 12 11 10 09 08 07 06 05 04 03 02 01
  do
    cat dbupdater | sed s/\$@/--debug\ --source=\\/g\\/data\\/v10\\/NBAR\\/${year}-${month}\ --purge/g > nbar_updater_${year}-${month}
    chmod 770  nbar_updater_${year}-${month}
    qsub  nbar_updater_${year}-${month}

  done
done

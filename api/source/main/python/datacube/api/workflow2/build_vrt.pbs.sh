#!/usr/bin/env bash

#PBS -N build_6_bands
#PBS -P u46
#PBS -q normal
#PBS -l walltime=05:00:00,ncpus=16,mem=64GB
#PBS -l wd

./build_vrt.sh

#!/bin/bash

#===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

#@PBS -P v10
#PBS -q normal
#PBS -l walltime=08:00:00,mem=4096MB,ncpus=1
#PBS -l wd
#@#PBS -m e
#@PBS -M alex.ip@ga.gov.au

# Script assumes MODULEPATH has previously been set (e.g. in .profile script) as follows:
# export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH # GA in-house testing only
# export MODULEPATH=/projects/el8/opt/modules/modulefiles:$MODULEPATH # Collaborative AGDC users

# Script assumes that agdc module has already been loaded as follows:
# module load agdc # Should load all dependencies

python -m agdc.stacker $@

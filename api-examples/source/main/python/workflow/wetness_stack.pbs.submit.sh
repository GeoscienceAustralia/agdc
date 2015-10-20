#!/bin/bash

# ===============================================================================
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

PBS_SCRIPT="$HOME/source/agdc-api/witl/api-examples/source/main/python/workflow/wetness_stack.pbs.sh"

OUTPUT_DIR="/g/data/u46/sjo/output/wetness/2015-04-17/lower_darling/existing"

#qsub -v outputdir=${OUTPUT_DIR}/2006-2009,xmin=140,xmax=140,ymin=-35,ymax=-35,acqmin=2006,acqmax=2009 ${PBS_SCRIPT}

# Lower Darling

## 2006 - 2009
#
#for x in {140..145}
#do
#    for y in {-36..-30}
#    do
#        qsub -v outputdir=${OUTPUT_DIR}/2006-2009,xmin=$x,xmax=$x,ymin=$y,ymax=$y,acqmin=2006,acqmax=2009 ${PBS_SCRIPT}
#    done
#done
#
## 2010 - 2012
#
#for x in {140..145}
#do
#    for y in {-36..-30}
#    do
#        qsub -v outputdir=${OUTPUT_DIR}/2010-2012,xmin=$x,xmax=$x,ymin=$y,ymax=$y,acqmin=2010,acqmax=2012 ${PBS_SCRIPT}
#    done
#done

# 2006 - 2009

for x in {140..145}
do
    for y in {-36..-30}
    do
        qsub -v outputdir=${OUTPUT_DIR}/2006-2012,xmin=$x,xmax=$x,ymin=$y,ymax=$y,acqmin=2006,acqmax=2012 ${PBS_SCRIPT}
    done
done

#Ord
#
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}"
#
#TODO
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}" # --month 11 12 1 2 3
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}" # --month 4 5 6 7 8 9 10


#!/bin/bash

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

PBS_SCRIPT="$HOME/source/agdc/agdc-api/api/source/main/python/datacube/api/workflow/band_stack_arg25.pbs.sh"

OUTPUT_DIR="/g/data/u46/sjo/testing/arg25_stack_3"

qsub -v outputdir=${OUTPUT_DIR},xmin=140,xmax=140,ymin=-36,ymax=-36 ${PBS_SCRIPT}

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

#for x in {140..145}
#do
#    for y in {-36..-30}
#    do
#        qsub -v outputdir=${OUTPUT_DIR}/2006-2012,xmin=$x,xmax=$x,ymin=$y,ymax=$y,acqmin=2006,acqmax=2012 ${PBS_SCRIPT}
#    done
#done

#Ord
#
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}"
#
#TODO
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}" # --month 11 12 1 2 3
#qsub -v outputdir="${OUTPUT_DIR}/ord",xmin=127,xmax=130,ymin=-18,ymax=-14,acqmin=2006,acqmax=2013 "${PBS_SCRIPT}" # --month 4 5 6 7 8 9 10


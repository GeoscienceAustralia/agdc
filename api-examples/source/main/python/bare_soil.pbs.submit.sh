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

PBS_SCRIPT="$HOME/source/agdc-api/api-examples/source/main/python/bare_soil.pbs.sh"

#OUTPUT_DIR="/g/data/u46/sjo/output/bare_soil/2014-10-29"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=110,xmax=119,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=120,xmax=129,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=130,xmax=139,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=140,xmax=149,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=150,xmax=155,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"

OUTPUT_DIR="/g/data/u46/sjo/output/refactor/bare_soil"
qsub -v outputdir="${OUTPUT_DIR}",xmin=124,xmax=125,ymin=-25,ymax=-24,yearmin=2000,yearmax=2005 "${PBS_SCRIPT}"
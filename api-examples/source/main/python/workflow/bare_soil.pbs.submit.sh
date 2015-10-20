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

PBS_SCRIPT="$HOME/source/agdc-api-stable/api-examples/source/main/python/workflow/bare_soil.pbs.sh"

#OUTPUT_DIR="/g/data/u46/sjo/output/bare_soil/2014-10-29"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=110,xmax=119,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=120,xmax=129,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=130,xmax=139,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=140,xmax=149,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=150,xmax=155,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"

#OUTPUT_DIR="/g/data/u46/sjo/output/refactor/bare_soil"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=124,xmax=125,ymin=-25,ymax=-24,yearmin=2000,yearmax=2005 "${PBS_SCRIPT}"

OUTPUT_DIR="/g/data/u46/sjo/output/bare_soil/2015-03-26"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=120,xmax=120,ymin=-20,ymax=-20,acqmin=1987,acqmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=123,xmax=123,ymin=-25,ymax=-25,acqmin=1987,acqmax=2014 "${PBS_SCRIPT}"
qsub -v outputdir="${OUTPUT_DIR}",xmin=135,xmax=135,ymin=-18,ymax=-18,acqmin=1987,acqmax=2014 "${PBS_SCRIPT}"
qsub -v outputdir="${OUTPUT_DIR}",xmin=142,xmax=142,ymin=-22,ymax=-22,acqmin=1987,acqmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=146,xmax=146,ymin=-34,ymax=-34,acqmin=1987,acqmax=2014 "${PBS_SCRIPT}"


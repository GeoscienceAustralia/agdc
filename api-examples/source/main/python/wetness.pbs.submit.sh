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
# ===============================================================================

PBS_SCRIPT="$HOME/source/agdc-api/api-examples/source/main/python/wetness.pbs.sh"

#OUTPUT_DIR="/g/data/u46/sjo/output/refactor/wetness"
OUTPUT_DIR="/g/data/u46/sjo/output/refactor/wetness_statistics"

qsub -v outputdir="${OUTPUT_DIR}",xmin=124,xmax=125,ymin=-25,ymax=-24,yearmin=2000,yearmax=2005 "${PBS_SCRIPT}"
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

BASE_DIR="/g/data/u46/sjo/clean_pixel/LS8/geoserver/2013-12-31"

PBS_SCRIPT="$HOME/source/agdc-api/api-examples/source/main/python/landsat_mosaic_pyramid.pbs.sh"

for i in DATE NBAR SAT
do
	qsub -v dir="${BASE_DIR}/${i}/pyramid" "${PBS_SCRIPT}"
done

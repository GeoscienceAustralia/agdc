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

PBS_SCRIPT="$HOME/source/agdc-api/api-examples/source/main/python/landsat_mosaic.pbs.sh"

## LS5/7 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01 "${PBS_SCRIPT}"
#
## LS5/7 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,pqfilter=false "${PBS_SCRIPT}"
#
## LS5 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls5_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS5 "${PBS_SCRIPT}"
#
## LS5 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls5",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS5,pqfilter=false "${PBS_SCRIPT}"
#
## LS7 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls7_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS7 "${PBS_SCRIPT}"
#
## LS7 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls7",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS7,pqfilter=false "${PBS_SCRIPT}"
#
## LS8 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls8_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2014-01,acqmax=2014-01,satellites=LS8 "${PBS_SCRIPT}"
#
## LS8 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls8",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2014-01,acqmax=2014-01,satellites=LS8,pqfilter=false "${PBS_SCRIPT}"
#
## TODO
#
## LS5/7/8 mosaic with PQA applied
##qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01 "${PBS_SCRIPT}"
#
## LS5/7/8 mosaic without PQA applied
##qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,pqfilter=false "${PBS_SCRIPT}"


# LS8 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2013-12,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"

qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=110,xmax=119,ymin=-45,ymax=-10,acqmin=2013-01,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"
qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=120,xmax=129,ymin=-45,ymax=-10,acqmin=2013-01,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"
qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=130,xmax=139,ymin=-45,ymax=-10,acqmin=2013-01,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"
qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=140,xmax=149,ymin=-45,ymax=-10,acqmin=2013-01,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"
qsub -v outputdir="/g/data/u46/sjo/clean_pixel/LS8/output/2013-12-31",xmin=150,xmax=155,ymin=-45,ymax=-10,acqmin=2013-01,acqmax=2013-12,satellites=LS8 "${PBS_SCRIPT}"

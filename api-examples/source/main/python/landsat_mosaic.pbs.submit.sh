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

PBS_SCRIPT="$HOME/source/agdc-api/api-examples/source/main/python/landsat_mosaic.pbs.sh"

#OUTPUT_DIR="/g/data/u46/sjo/output/landsat_mosaic"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=110,xmax=155,ymin=-45,ymax=-10,yearmin=2014,yearmax=2014 "${PBS_SCRIPT}"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=110,xmax=119,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=120,xmax=129,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=130,xmax=139,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=140,xmax=149,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=150,xmax=155,ymin=-45,ymax=-10,yearmin=1987,yearmax=2014 "${PBS_SCRIPT}"

# LS5/7 mosaic with PQA applied
qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01 "${PBS_SCRIPT}"

# LS5/7 mosaic without PQA applied
qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,pqfilter=false "${PBS_SCRIPT}"

# LS5 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls5_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS5 "${PBS_SCRIPT}"

# LS5 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls5",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS5,pqfilter=false "${PBS_SCRIPT}"

# LS7 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls7_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS7 "${PBS_SCRIPT}"

# LS7 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls7",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,satellites=LS7,pqfilter=false "${PBS_SCRIPT}"

# LS8 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls8_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2014-01,acqmax=2014-01,satellites=LS8 "${PBS_SCRIPT}"

# LS8 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls8",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2014-01,acqmax=2014-01,satellites=LS8,pqfilter=false "${PBS_SCRIPT}"

# TODO

# LS5/7/8 mosaic with PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57_pqa",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01 "${PBS_SCRIPT}"

# LS5/7/8 mosaic without PQA applied
#qsub -v outputdir="/g/data/u46/sjo/output/landsat_mosaic_ls57",xmin=120,xmax=125,ymin=-25,ymax=-20,acqmin=2005-01,acqmax=2005-01,pqfilter=false "${PBS_SCRIPT}"


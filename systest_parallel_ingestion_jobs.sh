#!/bin/bash

#===============================================================================
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
#     * Neither [copyright holder] nor the names of its contributors may be
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

TIME=`date +%s`
let SYNCHRONIZE_TIME=$TIME+60
echo SYNCHRONIZE_TIME=$SYNCHRONIZE_TIME
let iscene=0
while [ $iscene -ne $Nscenes ]; do
    export THIS_SCENE=SCENE_DIR$iscene
    export THIS_SCENE=${!THIS_SCENE}
    echo THIS_SCENE=XXX${THIS_SCENE}YYY
    if [ -z $THIS_SCENE ]; then
        break
    fi
    qsub -lncpus=1 -v DATACUBE_ROOT=$DATACUBE_ROOT,SYSTEST_DIR=$SYSTEST_DIR,SCENE_DIR=$THIS_SCENE,SYNCHRONIZE_TIME=$SYNCHRONIZE_TIME $DATACUBE_ROOT/ingestion_job.sh
    let iscene=iscene+1
done









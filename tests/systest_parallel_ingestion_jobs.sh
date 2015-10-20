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









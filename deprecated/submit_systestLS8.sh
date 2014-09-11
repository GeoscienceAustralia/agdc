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

REPO_NAME=ga-datacube
export SCRIPT_PATH="$(readlink -f $0)"
echo SCRIPT_PATH=$SCRIPT_PATH
export CODE_PATH=${SCRIPT_PATH%/$REPO_NAME/*}/$REPO_NAME
echo CODE_PATH=$CODE_PATH
PROJECT_CODE=v10
echo "PROJECT_CODE=$PROJECT_CODE (hard-coded)"
userID=$(whoami)
echo userID=$userID
HOME_PATH=$(echo `ls -d ~`)
echo HOME_PATH=$HOME_PATH

TOTAL_WORK_UNITS=47
WORK_UNITS_PER_JOB=1
NCPUS_PER_JOB=1

MAX_USER_JOBS_QUEUED=100
MAX_GROUP_JOBS_QUEUED=180

START_LINE=1

DATASETS_TO_INGEST=/g/data/v10/test_resources/scenes/dataset_testing/LS8_datasets_to_ingest_sorted

## Wait for existing jobs under userID to finish
# Get jobid of any existing jobs
let ijob=0
while read line
do
    jobid=`echo "$line" | cut -d '.' -f 1`
    qjobID[$ijob]=$jobid
    echo jobid=$jobid
    let ijob=ijob+1
done < <(qstat | grep $userID )

let Njob=ijob
let Njob=ijob
ijob=0
echo "Njob=$Njob"
while [ $ijob -ne $Njob ]; do
    echo "Job ${ijob} is ${qjobID[$ijob]}"
    let ijob=ijob+1
done

while true; do
   let continue_flag=0 
   let jobCount=0
   # Get a list of jobs still in the queue for writing to console
   let ijob=0
   while [ $ijob -ne $Njob ]; do
       jobInQueue[$ijob]=0
       let ijob=ijob+1
   done
   while read line
   do
       # Check queue
       let continue_flag=0
       jobid=`echo "$line" | cut -d '.' -f 1`
       let break_flag=0
       let ijob=0
       while [ $ijob -ne $Njob ]; do
	   if [ $jobid -eq ${qjobID[$ijob]} ]; then
	       ## let break_flag=1
	       # let continue_flag=1
               jobInQueue[$ijob]=1
               let jobCount=jobCount+1
	       # break
	   fi
	   let ijob=ijob+1
       done       
       if [ $break_flag -eq 1 ]; then
	   break
       fi
   done < <(qstat | grep $userID )
   # if [ $continue_flag -eq 0 ]; then
   #    break
   # fi
   echo `date`  still waiting for $jobCount jobs to finish...
   if [ $jobCount -eq 0 ]; then
      break
   fi
   let ijob=0
   while [ $ijob -ne $Njob ]; do
       if [ ${jobInQueue[$ijob]} -eq 1 ]; then
           echo "Job ${ijob} is ${qjobID[$ijob]}"
       fi
       let ijob=ijob+1
   done
#   echo "end of loop: continue_flag=$continue_flag"
   sleep 60
done

## Existing jobs have finished, now submit new ones


echo START_LINE=$START_LINE, TOTAL_WORK_UNITS=$TOTAL_WORK_UNITS
while [ $START_LINE -le $TOTAL_WORK_UNITS ]; do
    let END_LINE=START_LINE+WORK_UNITS_PER_JOB
    CMD="qsub -lncpus=$NCPUS_PER_JOB -v HOME_PATH=$HOME_PATH,START_LINE=$START_LINE,END_LINE=$END_LINE,DATASETS_TO_INGEST=$DATASETS_TO_INGEST,CODE_PATH=$CODE_PATH $CODE_PATH/systestLS8.sh"
    echo -e `python $CODE_PATH/waitForProjectUserQueueSlot.py -m $MAX_USER_JOBS_QUEUED -p $MAX_GROUP_JOBS_QUEUED -c $PROJECT_CODE`
    jobid=$(echo `python $CODE_PATH/executeWithoutFail.py -c "$CMD"`)
    qjobID[$ijob]=$(echo $jobid|cut -d '.' -f1)
    echo "submitted work units $START_LINE to $END_LINE (from total $TOTAL_WORK_UNITS) as job number ${qjobID[$ijob]}"
    let ijob=ijob+1
    let START_LINE=END_LINE
done














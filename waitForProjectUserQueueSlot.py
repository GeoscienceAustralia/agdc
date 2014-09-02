#!/usr/bin/env python

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

'''Wait for a queue slot for the specified user ensuring maxJobsForUser is never exceeded
Sleep the specfied number of seconds between tests if queue is "full"

usage ./waitForUserQueueSlot.py -q <queuename> -m <maxJobsForUser> -s <sleepTimeSeconds>

TODO: Note, queuename is currently ignored, all queues are counted

@Author: Steven Ring
Modified by Matthew Hardy 27/02/2014 to set a limit on jobs running under a project
'''

import os, sys, re, argparse, subprocess, time

def waitForQueueSlot(queue, projectCode, maxJobsForUser, maxJobsForProject, sleepTimeSeconds) :
    while True:
        [userJobCount,projectJobCount] = countJobsForUserAndProject(os.getenv('LOGNAME'),projectCode)
        print "User has %d jobs in queue, max is %d\\nProject has %d jobs in queue, max is %d\n" % (userJobCount, maxJobsForUser, projectJobCount, maxJobsForProject)
        if userJobCount >= maxJobsForUser or projectJobCount >= maxJobsForProject: 
            print "sleeping %d seconds, waiting for queue slot" % sleepTimeSeconds
            time.sleep(sleepTimeSeconds) 
        else:
            return

def countJobsForUserAndProject(userId, projectCode) :

    s1 = "Job_Owner = %s@(.*)" % userId
    s2 = "group_list = %s" % projectCode
    userPattern = re.compile(s1)
    projectPattern = re.compile(s2)
    proc = subprocess.Popen(["/opt/pbs/default/bin/qstat","-f"], stdout=subprocess.PIPE)
    userCount = 0
    projectCount = 0
    while True:
         line = proc.stdout.readline()
         if line :
             if userPattern.search(line):
                 userCount += 1
             if projectPattern.search(line):
                 projectCount += 1
         else:
             break
    return [userCount, projectCount]

description=""
parser = argparse.ArgumentParser(description)
parser.add_argument('-q', dest="queue", help="Name of the queue for the water_stacker jobs (default: normal)", default="normal")
parser.add_argument('-m', dest="maxUserJobsInQueue", help="Maximum jobs queued under user ID at any instant (default: 10)", default=10)
parser.add_argument('-p', dest="maxProjectJobsInQueue", help="Maximum jobs queued under project ID at any instant (default: 10)", default=10)
parser.add_argument('-c', dest="projectCode", help="string defining project code (default: v10)", default="v10")
parser.add_argument('-s', dest="sleepTimeSeconds", help="Sleep time in seconds between slot availablity test (default: 60)", default=60)

args = parser.parse_args()
queue = args.queue
projectCode = args.projectCode

# parse and check maxJobsInQueue
maxUserJobsInQueue = int(args.maxUserJobsInQueue)
maxProjectJobsInQueue = int(args.maxProjectJobsInQueue)
if (not maxUserJobsInQueue) or (not maxProjectJobsInQueue):
    print "maxUserJobsInQueue and maxProjectJobsInQueue must be a positive integer"
    parser.print_usage()
    sys.exit(1)

waitForQueueSlot(args.queue, projectCode, maxUserJobsInQueue, maxProjectJobsInQueue, int(args.sleepTimeSeconds))


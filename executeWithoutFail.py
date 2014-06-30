#!/usr/bin/env python
#
# Execute a command ignoring errors
#
# @Author: Steven Ring
#=============================================================


import os, argparse, time

def executeWithoutFail(cmd, sleepTimeSeconds):
    ''' Execute the supplied command until it succeeds, sleeping after each failure
    '''
    while True:
        print "Launching process: %s" % cmd
        rc = os.system(cmd)
        if rc == 0:
            break
        print "Failed to launch (exitCode=%d), waiting %d seconds" % (rc, sleepTimeSeconds)
        time.sleep(sleepTimeSeconds)



description=""
parser = argparse.ArgumentParser(description)
parser.add_argument('-c', dest="command", help="command", required=True)
parser.add_argument('-s', dest="sleepTimeSeconds", help="time to wait between execution attempts", default=60)

args = parser.parse_args()

executeWithoutFail(args.command, int(args.sleepTimeSeconds))

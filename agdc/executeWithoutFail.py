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

#===============================================================================
# Execute a command ignoring errors
#
# @Author: Steven Ring
#===============================================================================

from __future__ import absolute_import
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


if __name__ == '__main__':

    description=""
    parser = argparse.ArgumentParser(description)
    parser.add_argument('-c', dest="command", help="command", required=True)
    parser.add_argument('-s', dest="sleepTimeSeconds", help="time to wait between execution attempts", default=60)

    args = parser.parse_args()

    executeWithoutFail(args.command, int(args.sleepTimeSeconds))

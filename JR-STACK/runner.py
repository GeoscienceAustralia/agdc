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

'''Roger Edberg's job runner framework for the MDBA work (deprecated)
'''

import os
import sys
import argparse
import time
import pdb
import pprint
ppr = pprint.pprint

import logging
import logging.config


import jr2.configuration
import jr2.dataset_id
import jr2.jr_base
import jr2.jr_cube
import jr2.jr_report
import jr2.job_test
import jr2.job_fc
import jr2.job_kr


logging.config.fileConfig(os.path.join(jr2.configuration.ROOT, 'logging2.config'))
ROOT_LOGGER_NAME = os.path.basename(__file__).strip('.pyc')
log = logging.getLogger(ROOT_LOGGER_NAME)
log.setLevel(logging.INFO)

sys.path.append('/projects/v10/datacube/JR-STACK')


def parse_args():
    argp = argparse.ArgumentParser(__file__)
    argp.add_argument('-c', '--config', default=None, required=True,
                      metavar='CONFIGPATH',
                      help='Specify configuration file (REQUIRED)')
    argp.add_argument('-s', '--status', action='store_true', default=False,
                      help='Report processing status')
    argp.add_argument('-e', '--errors', action='store_true', default=False,
                      help='Report processing errors (inputs)')
    argp.add_argument('-j', '--jobs', type=int, default=0, metavar='NJOBS',
                      help='Submit jobs')
    argp.add_argument('-l', '--link', action='store_true', default=False,
                      help='Link jobs')
    argp.add_argument('--foo', action='store_true', default=False,
                      help='dummy flag (testing)')
    argp.add_argument('--all-inputs', action='store_true', default=False,
                      help='Attempt to inspect all available inputs')
    argp.add_argument('--debug', action='store_true', default=False,
                      help='Enables debug breakpoint in main function')
    argp.add_argument('--next-inputs', action='store_true', default=False,
                      help='Report next dataset(s), can be combined with --all-inputs')
    argp.add_argument('--log', type=str, default='', metavar='LOGMESSAGE',
                      help='Log a message with the JR system logger')

    # NOTE -- default job type is FC (set here)
    argp.add_argument('--fc', action='store_true', default=True,
                      help='Fractional cover calculation')

    argp.add_argument('--nbar', action='store_true', default=False,
                      help='NBAR calculation (NOT YET SUPPORTED)')
    argp.add_argument('--pq', action='store_true', default=False,
                      help='Pixel quality calculation (NOT YET SUPPORTED)')
    argp.add_argument('--test', action='store_true', default=False,
                      help='Test calculation')
    return argp.parse_args()



def main():

    log = logging.getLogger(ROOT_LOGGER_NAME + '.main')

    args = parse_args()

    if args.log:
        log.info(args.log)
        return

    jr_class = None

    if args.test:
       jr_class = jr2.job_test.JobRunner
    elif args.fc:
       jr_class = jr2.job_fc.JobRunner
    elif args.pq:
       jr_class = jr2.job_pq.JobRunner
    elif args.nbar:
       jr_class = jr2.job_nbar.JobRunner

    # DEBUG/TEST -- Hardwire job class type for testing.
    #jr_class = jr2.job_test.JobRunner
    #jr_class = jr2.job_fc.JobRunner
    jr_class = jr2.job_kr.JobRunner

    runner = jr_class(args.config)

    #try:
    #    runner = jr_class(args.config)
    #except AttributeError:
    #    raise jr2.jr_base.JRError('Job type was not resolved (jr_class=None)')

    if args.status:
        runner = jr2.jr_report.JRReport(args.config)
        runner.report_status(all_inputs=args.all_inputs)
    elif args.errors:
        runner = jr2.jr_report.JRReport(args.config)
        runner.report_errors(all_inputs=args.all_inputs)
    elif args.next_inputs:
        runner = jr2.jr_report.JRReport(args.config)
        runner.report_next_inputs(all_inputs=args.all_inputs)
    elif args.foo:
        log.debug('--foo enabled')
        runner = jr2.jr_report.JRReport(args.config)
        runner.foo()
    elif args.jobs:
        try:
            runner = jr_class(args.config)
        except AttributeError:
            raise jr2.jr_base.JRError('Job type was not resolved (jr_class=None)')

        #sys.exit('BAILING OUT -- NO JOB SUBMITTED')

        log.debug('jobs=%d link=%s' % (args.jobs, args.link))
        for n in xrange(args.jobs):
            jobid = runner.submit_job(link=args.link)
            if jobid is None:
                log.debug('No job submitted (processing complete?)')
                break
            time.sleep(jr2.configuration.JOB_DELAY)

    if args.debug:
        pdb.set_trace()



if __name__ == '__main__':
    main() 


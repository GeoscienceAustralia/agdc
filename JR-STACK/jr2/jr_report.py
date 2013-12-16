#!/usr/bin/env python

import os
import sys
import pprint
ppr = pprint.pprint
import logging

import configuration
import dataset_id
from jr_configuration import JRConfiguration
from jr_base import JRError
from jr_base import JRBase


ROOT_LOGGER_NAME = os.path.basename(__file__).strip('.pyc')
log = logging.getLogger(ROOT_LOGGER_NAME)
log.setLevel(logging.INFO)



class JRReport(JRBase):
    """Job runner reporting class
    """

    def submit_job(self, *args):
        pass

    def report_status(self, all_inputs=False):
        #fmt_config = 'CONFIGURATION %s (inputs=%s) %s'
        while self.config.is_valid():
            #desc = ('' if self.unprocessed() else '-- COMPLETED')
            #desc = ''
            #print
            #print '='*5, fmt_config % (self.config.path, self.config.input_dir, desc)

            print '='*3, 'CONFIGURATION', self.config.path
            print 'LOGGED (%s)' % self.config.log_dir
            ppr(self.logged())
            print 'PRODUCTS (%s)' % self.config.product_dir
            ppr(self.products())
            print 'ERRORS (%s)' % self.config.input_dir
            ppr(self.errors())
            print 'UNPROCESSED (%s)' % self.config.input_dir
            ppr(self.unprocessed())
            print 'NEXT', self.next_input()
            print
            if not all_inputs:
                break
            if not self.config.next_config(save=False):
                break

    def report_errors(self, all_inputs=False):
        error_input_list = []
        while self.config.is_valid():
            error_input_list.extend(self.errors())
            if not all_inputs:
                break
            self.config.next_config(save=False)
        print
        print 'ERRORS'
        ppr(error_input_list)

    def report_next_inputs(self, all_inputs=False):
        while self.config.is_valid():
            n = self.next_input()
            if n:
                print 'NEXT', n
            if not all_inputs:
                break
            if not self.config.next_config(save=False):
                break



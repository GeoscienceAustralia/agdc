#!/usr/bin/env python

import os
import sys
import errno
import re
import ConfigParser
import pdb
import pprint
ppr = pprint.pprint

import subprocess
import logging


import configuration


ROOT_LOGGER_NAME = os.path.basename(__file__).strip('.py')
log = logging.getLogger(ROOT_LOGGER_NAME)
log.setLevel(logging.INFO)



class JRConfigurationError(Exception):
    pass


class JRConfiguration(object):

    CFG_KEYS = ['job_runner_root', 'app_path', 'input_root', 'product_root',
                'product_subdirectory', 'job_template']

    def __init__(self, cfg_path=None):
        self.cfg_parser = None
        self.cfg_path_list = []
        if cfg_path:
            self.load(cfg_path)

    def load(self, cfg_path=None):
        self.cfg_parser = ConfigParser.RawConfigParser(allow_no_value=True)
        self.cfg_parser.optionxform = str
        path = (cfg_path if cfg_path.startswith(configuration.ROOT)
                else os.path.join(configuration.ROOT, cfg_path))
        try:
            with open(path) as __file:
                self.cfg_parser.readfp(__file)
        except IOError, e:
            if e.errno != errno.EEXIST:
                raise
        finally: 
            self.cfg_path_list.append(path)
        log.info('loaded configuration: %s' % path)

    def reload(self):
        try:
            self.load(self.cfg_path_list.pop())
        except IndexError:
            raise JRConfigurationError('Attempted configuration reload with empty path list')

    @property
    def config(self):
        cdict = dict(self.cfg_parser.items('JOB'))
        assert all([k in cdict for k in self.CFG_KEYS])
        return cdict

    def inputs(self):
        try:
            input_list = self.cfg_parser.options('INPUTS')    
            return (input_list if input_list else None)
        except ConfigParser.NoSectionError:
            return None

    @property
    def path(self):
        try:
            return self.cfg_path_list[-1]
        except IndexError:
            raise JRConfigurationError('Could not get load path (uninitialized object?)')

    @property
    def job_type(self):
        try:
            return self.config['job_type']
        except KeyError:
            raise JRError('Could not get job_type from configuration dict (%s)' % self.path)

    @property
    def product_dir(self):
        return os.path.join(self.config['product_root'],
                            self.config['product_subdirectory']) 

    @property
    def input_dir(self):
        return os.path.join(self.config['input_root'],
                            self.config['product_subdirectory']) 

    @property
    def log_dir(self):
        return os.path.join(self.config['log_root'],
                            self.config['product_subdirectory']) 

    @property
    def dynamic(self):
        if self.cfg_parser.has_section('INPUTS'):
            return False
        return (self.config['dynamic_configuration'].strip().upper() == 'TRUE')

    @property
    def active(self):
        return (self.config['active'].strip().upper() == 'TRUE')

    def is_valid(self):
        return os.path.isdir(self.input_dir) or (self.inputs() is not None)

    def next_config(self, save=True):

        if not self.dynamic:
            return False

        # Modify the configuration to point at the next set of inputs.

        ym = self.cfg_parser.get(self.job_type, 'product_subdirectory')
        try:
            m = re.search('(\d{4})-(\d{2})', ym)
            year, month = [int(x) for x in m.groups()]
        except AttributeError:
            raise JRConfigurationError('Could not parse year-month descriptor ("%s")' % ym)
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

        next_product_subdir = '%04d-%02d' % (year, month)
        self.cfg_parser.set(self.job_type, 'product_subdirectory', next_product_subdir)

        if not self.is_valid():
            log.debug('next_config: %s NOT VALID (no inputs)' % next_product_subdir)
            return False

        log.debug('next_config: %s' % next_product_subdir)

        # Write the updated configuration to disk.

        if save:
            cfg_path = self.path
            if not cfg_path.endswith('.WORKING'):
                cfg_path = '%s.WORKING' % cfg_path
            with open(cfg_path, 'w') as __file:
                self.cfg_parser.write(__file)
            log.debug('wrote configuration file: %s' % cfg_path)
            self.cfg_path_list.append(cfg_path)

        log.debug('next_config OK')
        return True



#!/usr/bin/env python

import os
import sys
import errno
import re
import pdb
import pprint
ppr = pprint.pprint
import subprocess
import logging


import configuration
import dataset_id
from jr_configuration import JRConfiguration


ROOT_LOGGER_NAME = os.path.basename(__file__).strip('.pyc')
log = logging.getLogger(ROOT_LOGGER_NAME)
log.setLevel(logging.INFO)



def _create_dir(path):
    log = logging.getLogger(ROOT_LOGGER_NAME + '._create_dir')
    try:
        os.makedirs(path)
        log.debug('path=%s' % path)
        return True
    except OSError, e:
        if e.errno == errno.EEXIST:
            return False
        else:
            log.error(str(e))
            raise

def _subdirs(path):
    try:
        return sorted(os.listdir(path))
    except OSError, e:
        if e.errno == errno.ENOENT:
            return []
        raise



class JRError(Exception):
    pass



class JRBase(object):
    """Job runner base class.
    """

    def __init__(self, cfg_path=None):
        self.config = JRConfiguration(cfg_path)
        self.executable = configuration.EXECUTABLE

    def products(self):
        return _subdirs(self.config.product_dir)

    def logged(self):
        return _subdirs(self.config.log_dir)

    def inputs(self):
        configured_inputs = self.config.inputs()
        if configured_inputs:
            return configured_inputs
        else:
            return [os.path.join(self.config.input_dir, x)
                    for x in _subdirs(self.config.input_dir)]

    def no_product(self):
        configured_inputs = self.config.inputs()
        if configured_inputs:
            return {
                os.path.basename(x): x for x in configured_inputs
                if not os.path.exists(self.product_path(os.path.basename(x))) 
            }
        else:
            return {
                k: os.path.join(self.config.input_dir, k) for k in self.inputs()
                if not os.path.exists(self.product_path(k)) 
            }

    def unprocessed(self):
        xpaths = self.no_product()
        lnames = self.logged()
        return sorted([xpaths[k] for k in xpaths if k not in lnames])

    def errors(self):
        xpaths = self.no_product()
        lnames = self.logged()
        return sorted([xpaths[k] for k in xpaths if k in lnames])

    def next_input(self):
        try:
            return self.unprocessed().pop(0)
        except IndexError:
            return None

    def submit_job(self, link=False):
        log = logging.getLogger(ROOT_LOGGER_NAME + '.submit_job')

        if not self.config.active:
            log.info('no job submitted (config.active=False)')
            return None

        dataset_path = self.next_input()

        if dataset_path is None:
            if self.config.next_config():
                log.debug('using next config: %s' % self.config.input_dir)
                return self.submit_job(link)
            log.debug('returning None')
            return None

        log.debug('%s link=%s' % (dataset_path, link))

        # Ensure product and log root directories exist.

        _create_dir(self.config.product_dir)
        _create_dir(self.config.log_dir)

        # Create the job log directory. If it's already there another runner
        # instance may have created it, so try the next job.

        dataset_name = os.path.basename(dataset_path)
        dataset_log_dir = os.path.join(self.config.log_dir, dataset_name)

        if not _create_dir(dataset_log_dir):
            return self.submit_job(link)

        # Bail out and submit the next job if the input is invalid.

        if not self.validate_input(dataset_path):
            log.info('ignoring invalid input %s' % dataset_path)
            # TODO create .../log/.../<input>/INVALID_INPUT file?
            return self.submit_job(link)

        # Write the job script into the log directory.

        template = self.config.config['job_template'].lstrip('\n')
        scr_path = os.path.join(dataset_log_dir, self.config.config['job_name'])
        with open(scr_path, 'w') as __file:
            __file.write(template % self.job_vars(dataset_path, link))
            log.debug('created job script: %s' % scr_path)

        #############
        ### DEBUG ###
        #############
        #log.debug('bailing out after writing job script: %s' % scr_path)
        #return 'no_job_submitted'

        # Submit the job.

        job_id = None
        p = subprocess.Popen('qsub -j oe %s' % self.config.config['job_name'],
                             shell=True, cwd=dataset_log_dir,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pout, perr = p.communicate()
        if p.returncode == 0 and re.search('^(\d+).(\w+)', pout):
            job_id = pout.strip('\n')
            log.info('%s %s (link=%s)' % (job_id, dataset_path, link))
        else:
            log.error('SUBMIT FAILED\n\n%s' % '\n'.join([pout, perr, '\n']))

        return job_id

    ###########################################################################
    # Virtual methods.
    # Each subclass of JRBase should provide these.
    ###########################################################################

    def validate_input(self, dataset_path):
        # VIRTUAL
        return True

    def product_path(self, dataset_path):
        # VIRTUAL
        return os.path.join(self.config.product_dir, os.path.basename(dataset_path))

    def job_vars(self, dataset_path, link):
        # VIRTUAL
        return {}

    ###########################################################################
    # Misc.
    ###########################################################################

    def foo(self):
        # DEBUG for testing logging
        log = logging.getLogger(ROOT_LOGGER_NAME + '.foo')
        log.info('fooey!')


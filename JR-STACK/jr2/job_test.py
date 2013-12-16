#!/usr/bin/env python

import os
import fnmatch

import jr_configuration
import jr_base



class JobRunner(jr_base.JRBase):
    """Test job runner.
    """

    def validate_input(self, dataset_path):
        if os.path.isdir(dataset_path):
            if fnmatch.filter(os.listdir(dataset_path), '*IGNORE*'):
                return False
        return True
        #return (dataset_path is not None)

    def job_vars(self, dataset_path=None, link=False):
        if dataset_path is None:
            raise jr_base.JRError('%s.job_vars received arg dataset_path=None' % self)
        c = self.config
        return {
            '__j_next_job'           : ('%s --jobs=1 --config=%s --link' % (self.executable, c.path) if link else '# link=False'),
            '__j_log_completion'     : '%s --config=%s --log="%s COMPLETED"' % (self.executable, c.path, os.getenv('PBS_JOBID')),
            '__j_config_path'        : c.path,
            '__j_dataset_path'       : dataset_path,
            '__j_product_path'       : os.path.join(c.product_dir, os.path.basename(dataset_path)),
            '__j_app_path'           : c.config['app_path'],
            '__j_notification_email' : c.config['notification_email'],
        }



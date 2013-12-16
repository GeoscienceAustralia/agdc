#!/usr/bin/env python

import os

import jr_base



class JobRunner(jr_base.JRBase):
    """NBAR job runner.
    """

    def job_vars(self, dataset_path, link):

        pass

        #c = self.config
        #return {
        #    '__j_next_job'           : ('%s --jobs=1 --config=%s --link' % (self.executable, c.path)
        #                                if link else '# link=False'),
        #    '__j_config_path'        : c.path,
        #    '__j_dataset_path'       : dataset_path,
        #    '__j_product_path'       : os.path.join(c.product_dir, os.path.basename(dataset_path)),
        #    '__j_app_path'           : c.config['app_path'],
        #    '__j_notification_email' : c.config['notification_email'],
        #}



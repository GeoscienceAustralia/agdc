#!/usr/bin/env python

import os
import fnmatch

import dataset_id
import jr_base



class JobRunner(jr_base.JRBase):
    """Fractional cover job runner.
    """

    def validate_input(self, dataset_path):
        if dataset_id.get_component(dataset_path, 'tag') == 'NBAR':
            contents = os.listdir(dataset_path)
            return ('scene01' in contents and fnmatch.filter(contents, '*_GANBAR*-*.jpg') != [])
        return False

    def product_path(self, dataset_path):
        return os.path.join(self.config.product_dir,
                            dataset_id.convert(dataset_path, 'FC'))

    def job_vars(self, dataset_path, link):
        if dataset_path is None:
            raise jr_base.JRError('%s.job_vars received arg dataset_path=None' % self)
        c = self.config
        return {
            '__j_next_job'               : ('%s --jobs=1 --config=%s --link' % (self.executable, c.path)
                                            if link else '# link=False'),
            '__j_config_path'            : c.path,
            '__j_dataset_path'           : dataset_path,
            '__j_product_path'           : os.path.join(c.product_dir, os.path.basename(dataset_path)),
            '__j_massdata_product_path'  : os.path.join('/massdata/v10/FC-WORK',
                                                        os.path.basename(c.product_dir),
                                                        os.path.basename(dataset_path)),
            '__j_app_path'               : c.config['app_path'],
            '__j_notification_email'     : c.config['notification_email'],
            '__j_fc_code_root'           : c.config['fc_code_root'],
        }


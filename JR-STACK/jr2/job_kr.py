#!/usr/bin/env python

import os
import fnmatch
import re

import dataset_id
import jr_cube



class JobRunner(jr_cube.JRCube):
    """Unleashes the killer rabbit.
    """

    #def validate_input(self, dataset_path):
    #    if dataset_id.get_component(dataset_path, 'tag') == 'NBAR':
    #        contents = os.listdir(dataset_path)
    #        return ('scene01' in contents and fnmatch.filter(contents, '*_GANBAR*-*.jpg') != [])
    #    return False

    def tile_label(self, dataset_path):
        x, y, __, __ = os.path.basename(dataset_path).split('_')
        #return ('%4d_%04d' % (int(x), int(y))) 
        return re.sub('\+', '', '%+04d_%+04d' % (int(x), int(y)))

    def date_range(self, dataset_path):
        __, __, start, end, = os.path.basename(dataset_path).split('_')
        return (start, end)

    def product_path(self, dataset_path):
        return os.path.join(self.config.product_dir, 
                            self.tile_label(dataset_path),
                            os.path.basename(dataset_path))

    def job_vars(self, dataset_path, link):
        if dataset_path is None:
            raise jr_base.JRError('%s.job_vars received arg dataset_path=None' % self)
        c = self.config
        # HACK
        x, y, s, e = os.path.basename(dataset_path).split('_')

        start_date = s
        end_date = e

        return {
            '__j_next_job'               : ('%s --jobs=1 --config=%s --link' % (self.executable, c.path)
                                            if link else '# link=False'),
            '__j_config_path': c.path,
            '__j_x': x,
            '__j_y': y,
            '__j_s': start_date,
            '__j_e': end_date,
            '__j_o': os.path.dirname(self.product_path(dataset_path)),
            #'__j_o': c.product_dir,
        }


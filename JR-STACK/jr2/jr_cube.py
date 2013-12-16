#!/usr/bin/env python

import os
import os.path
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


import jr_base



__TILE_INDICES = [(143,-35), (144, -35), (145, -35), (143,-36), (144, -36), (145, -36), (143,-37), (144, -37), (145, -37)]

TILE_INDICES = [
(138, -36),
(138, -35),
(138, -34),
(139, -37),
(139, -36),
(139, -35),
(139, -34),
(139, -33),
(140, -36),
(140, -35),
(140, -34),
(140, -33),
(141, -38),
(141, -37),
(141, -36),
(141, -35),
(141, -34),
(141, -33),
(141, -32),
(142, -38),
(142, -37),
(142, -36),
(142, -35),
(142, -34),
(142, -33),
(142, -32),
(142, -31),
(143, -38),
(143, -37),
(143, -36),
(143, -35),
(143, -34),
(143, -33),
(143, -32),
(143, -31),
(143, -30),
(143, -29),
(144, -38),
(144, -37),
(144, -36),
(144, -35),
(144, -34),
(144, -33),
(144, -32),
(144, -31),
(144, -30),
(144, -29),
(144, -28),
(144, -27),
(145, -38),
(145, -37),
(145, -36),
(145, -35),
(145, -34),
(145, -33),
(145, -32),
(145, -31),
(145, -30),
(145, -29),
(145, -28),
(145, -27),
(145, -26),
(145, -25),
(146, -38),
(146, -37),
(146, -36),
(146, -35),
(146, -34),
(146, -33),
(146, -32),
(146, -31),
(146, -30),
(146, -29),
(146, -28),
(146, -27),
(146, -26),
(146, -25),
(147, -38),
(147, -37),
(147, -36),
(147, -35),
(147, -34),
(147, -33),
(147, -32),
(147, -31),
(147, -30),
(147, -29),
(147, -28),
(147, -27),
(147, -26),
(147, -25),
(148, -37),
(148, -36),
(148, -35),
(148, -34),
(148, -33),
(148, -32),
(148, -31),
(148, -30),
(148, -29),
(148, -28),
(148, -27),
(148, -26),
(148, -25),
(149, -37),
(149, -36),
(149, -35),
(149, -34),
(149, -33),
(149, -32),
(149, -31),
(149, -30),
(149, -29),
(149, -28),
(149, -27),
(150, -34),
(150, -33),
(150, -32),
(150, -31),
(150, -30),
(150, -29),
(150, -28),
(150, -27),
(151, -32),
(151, -31),
(151, -30),
(151, -29),
(151, -28),
(151, -27),
(152, -30),
(152, -29),
(152, -28)
]

__DATE_RANGES = [('01/12/2000', '31/03/2001'), ('01/03/2001', '31/06/2001'), ('01/06/2000', '30/09/2001'), ('01/09/2000', '31/12/2001')]

DATE_RANGES = [
('01/01/2000', '31/03/2000'), ('01/03/2000', '30/06/2000'), ('01/06/2000', '30/09/2000'), ('01/09/2000', '31/12/2000'),
('01/12/2000', '31/03/2001'), ('01/03/2001', '30/06/2001'), ('01/06/2001', '30/09/2001'), ('01/09/2001', '31/12/2001'),
('01/12/2001', '31/03/2002'), ('01/03/2002', '30/06/2002'), ('01/06/2002', '30/09/2002'), ('01/09/2002', '31/12/2002'), 
('01/12/2002', '31/03/2003'), ('01/03/2003', '30/06/2003'), ('01/06/2003', '30/09/2003'), ('01/09/2003', '31/12/2003'), 
('01/12/2003', '31/03/2004'), ('01/03/2004', '30/06/2004'), ('01/06/2004', '30/09/2004'), ('01/09/2004', '31/12/2004'), 
('01/12/2004', '31/03/2005'), ('01/03/2005', '30/06/2005'), ('01/06/2005', '30/09/2005'), ('01/09/2005', '31/12/2005'), 
('01/12/2005', '31/03/2006'), ('01/03/2006', '30/06/2006'), ('01/06/2006', '30/09/2006'), ('01/09/2006', '31/12/2006'), 
('01/12/2006', '31/03/2007'), ('01/03/2007', '30/06/2007'), ('01/06/2007', '30/09/2007'), ('01/09/2007', '31/12/2007'), 
('01/12/2007', '31/03/2008'), ('01/03/2008', '30/06/2008'), ('01/06/2008', '30/09/2008'), ('01/09/2008', '31/12/2008'), 
('01/12/2008', '31/03/2009'), ('01/03/2009', '30/06/2009'), ('01/06/2009', '30/09/2009'), ('01/09/2009', '31/12/2009'), 
('01/12/2009', '31/03/2010'), ('01/03/2010', '30/06/2010'), ('01/06/2010', '30/09/2010'), ('01/09/2010', '31/12/2010'),
]

def date2intstring(eurodatestring):
    return eurodatestring[6:10] + eurodatestring[3:5] + eurodatestring[0:2]

DATE_RANGES.reverse() # Order from newest to oldest

ARGTUPLES = []
for d0, d1 in DATE_RANGES:
    for tx, ty in TILE_INDICES:
#        ARGTUPLES.append( (str(tx), str(ty), d0.replace('/', ''), d1.replace('/', '')) )
        ARGTUPLES.append( (re.sub('\+', '', '%+04d' % tx), re.sub('\+', '', '%+04d' % ty), date2intstring(d0), date2intstring(d1)) )


TAGS = []
for t in ARGTUPLES:
    TAGS.append('_'.join(t))



class JRCube(jr_base.JRBase):
    """Job runner base class.
    """

    def products(self):
        return jr_base._subdirs(self.config.product_dir)

    def logged(self):
        return jr_base._subdirs(self.config.log_dir)

    def inputs(self):
        #return sorted(TAGS)
        return TAGS
        
    def no_product(self):
        return [x for x in TAGS if x not in self.logged()]
        
    def unprocessed(self):
        #xpaths = self.no_product()
        #lnames = self.logged()
        #return sorted([xpaths[k] for k in xpaths if k not in lnames])
        return [x for x in TAGS if x not in self.logged()]

    #def errors(self):
    #    xpaths = self.no_product()
    #    lnames = self.logged()
    #    return sorted([xpaths[k] for k in xpaths if k in lnames])

    def tile_indices(self, dataset_path):
        x, y, __, __ = os.path.basename(dataset_path).split('_')
        return (x, y)

    def date_range(self, dataset_path):
        __, __, start, end, = os.path.basename(dataset_path).split('_')
        return (start, end)

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

        jr_base._create_dir(self.config.product_dir)
        jr_base._create_dir(self.config.log_dir)

        # Create the job log directory. If it's already there another runner
        # instance may have created it, so try the next job.

        dataset_name = os.path.basename(dataset_path)
        dataset_log_dir = os.path.join(self.config.log_dir, dataset_name)

        # Create product subdirectory.
 
        jr_base._create_dir(os.path.join(self.config.product_dir, 
                                         self.tile_label(dataset_path)))

        if not jr_base._create_dir(dataset_log_dir):
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



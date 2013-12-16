#!/usr/bin/env python

import os
import sys
import pprint

print __file__
pprint.pprint(sys.argv)

# Output directory is the last arg

product_path = os.path.join(sys.argv[-1], 'foo.%s' % os.getenv('PBS_JOBID'))
os.system('touch %s' % product_path)
print 'DONE', product_path


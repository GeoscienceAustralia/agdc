
import os.path
import logging
import logging.config


ROOT = '/projects/v10/datacube/JR-STACK'
EXECUTABLE = os.path.join(ROOT, 'runner.py')
JOB_DELAY = 1


logging.config.fileConfig(os.path.join(ROOT, 'logging2.config'))



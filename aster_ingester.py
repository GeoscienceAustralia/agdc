#!/usr/bin/env python
"""
    aster_ingester.py - Ingester script for ASTER datasets.
"""

import os
import sys
import re
import logging
import argparse

import cube_util
from abstract_ingester import AbstractIngester
from landsat_dataset import LandsatDataset
from itertools import chain

#
# Set up root logger
#
# Note that the logging level of the root logger will be reset to DEBUG
# if the --debug flag is set (by AbstractIngester.__init__). To recieve
# DEBUG level messages from a module do two things:
#    1) set the logging level for the module you are interested in to DEBUG,
#    2) use the --debug flag when running the script.
#

logging.basicConfig(stream=sys.stdout,
                    format='%(message)s',
                    level=logging.INFO)

#
# Set up logger (for this module).
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class AsterIngester(AbstractIngester):

    def parse_args(self):
        """Parse the command line arguments for the ingester.
    
        Returns an argparse namespace object.
        """
        LOGGER.debug('  Calling parse_args()')

        _arg_parser = argparse.ArgumentParser('dbupdater')
        
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
            help='LandsatIngester configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('--processed_source', dest='source_dir',
            required=True,
            help='Source root directory containing datasets after processing with aster-gain-adjust.py')
        _arg_parser.add_argument('--original_data_dir', dest='original_data_dir',
            required=True,
            help='Source root directory containing unprocessed HDF DN value scenes')
        return _arg_parser.parse_args()

    def find_datasets(self, source_dir):
        """Return a list of path to the datasets under 'source_dir'.

        Datasets are identified as a subdirectory containing 3 VRTs and related TIFFs. 
        (For 3 sensors with PQA bands)"""

        LOGGER.info('Searching for datasets in %s', source_dir)
        
        # Get all subdirectories of source_dir
        dir_path, child_dirs, files = os.walk(source_dir).next()
        
        # Convert all the directories to a full path (and remove any that don't have the requisite 18 TIFF files)
        valid_child_dirs = [ os.path.join(dir_path, x) for x in child_dirs if len(glob.glob(os.path.join(dir_path, x, "*.tiff"))) == 18 ]
        
        # For each element in the list, generate 3 datasets (SWIR, TIR, VNIR)
        # (i.e replace each directory with a path to each of the 3 VRT's in that directory)
        return list(chain.from_iterable((os.path.join(d, 'VNIR.vrt'), 
                                         os.path.join(d, 'SWIR.vrt'),
                                         os.path.join(d, 'TIR.vrt')) for d in valid_child_dirs))
        

    def open_dataset(self, dataset_path):
        """Create and return a dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.
        """
        original_name = os.path.basename(os.path.dirname(dataset_path)) + '.hdf'
        
        return AsterDataset(dataset_path, os.path.join(self.args.original_data_dir, original_name))

# Start ingest process
if __name__ == "__main__":

    ingester = AsterIngester()

    if ingester.args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ingester.ingest(ingester.args.source_dir)

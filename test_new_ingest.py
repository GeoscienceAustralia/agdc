"""
    Run all unit tests for the new ingest process.
"""

import unittest
import test_abstract_ingester
import test_landsat_dataset
import test_landsat_bandstack
import test_dataset_record
import test_tile_record
import test_tile_contents

def the_suite():
    """Returns a test suile of all the tests to be run."""

    suite_list = []

    suite_list.append(test_abstract_ingester.the_suite())
    suite_list.append(test_landsat_dataset.the_suite())
    suite_list.append(test_landsat_bandstack.the_suite())
    suite_list.append(test_dataset_record.the_suite())
#    suite_list.append(test_tile_record.the_suite(fast=True))
#    suite_list.append(test_tile_contests.the_suite(fast=True))

    return unittest.TestSuite(suite_list)

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())

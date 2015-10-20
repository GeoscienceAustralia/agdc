#!/usr/bin/env python

#===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

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

"""Tests for the cube_util.py module."""

import unittest
import datetime

import cube_util

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestUtilityFunctions(unittest.TestCase):
    """Unit tests for cube_util utility functions."""

    MODULE = 'cube_util'
    SUITE = 'TestUtilityFunctions'

    def test_parse_date_format1(self):
        """Test of parsing date from string, format 1."""

        date = cube_util.parse_date_from_string('20140313')
        self.assertEqual(date, datetime.date(2014, 3, 13))

    def test_parse_date_format2(self):
        """Test of parsing date from string, format 2."""

        date = cube_util.parse_date_from_string('2014-03-13')
        self.assertEqual(date, datetime.date(2014, 3, 13))

    def test_parse_date_format3(self):
        """Test of parsing date from string, format 3."""

        date = cube_util.parse_date_from_string('13/03/2014')
        self.assertEqual(date, datetime.date(2014, 3, 13))

    def test_parse_date_bad_format(self):
        """Test of parsing date from string, bad format."""

        date = cube_util.parse_date_from_string('A long time ago')
        self.assertEqual(date, None)


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestUtilityFunctions]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())

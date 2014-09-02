#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither [copyright holder] nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

"""Tests for the cube_util.py module."""

import unittest
import datetime
import time
import os
import random
import shutil
import logging
import subprocess

import dbutil
import cube_util

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestGetDatacubeRoot(unittest.TestCase):
    """Unit tests for the get_datacube_root utility function."""

    MODULE = 'cube_util'
    SUITE = 'TestGetDatacubeRoot'

    def setUp(self):
        """Save environment variable DATACUBE_ROOT."""

        self.save_dcr = os.environ.get('DATACUBE_ROOT', None)

    def test_datacube_root_from_env(self):
        """Test finding the datacube root from the environment."""

        test_dir = '/home/somewhere/ga-datacube'
        os.environ['DATACUBE_ROOT'] = test_dir
        datacube_root = cube_util.get_datacube_root()
        self.assertEqual(test_dir, datacube_root)

    def test_datacube_root_from_inspect(self):
        """Test finding the datacube root using the inspect module."""

        if 'DATACUBE_ROOT' in os.environ:
            del os.environ['DATACUBE_ROOT']

        datacube_root = cube_util.get_datacube_root()

        # Verify that the directory returned at least contains datacube
        # code. Probably about the best we can do without calling inspect
        # ourselves.
        module_path = os.path.join(datacube_root, 'test_cube_util.py')
        if not os.path.isfile(module_path):
            self.fail("Cannot find module file at datacube_root:\n" +
                      "   " + module_path + "\n")

    def tearDown(self):
        """Restore environment variable DATACUBE_ROOT."""

        if self.save_dcr is None:
            if 'DATACUBE_ROOT' in os.environ:
                del os.environ['DATACUBE_ROOT']
        else:
            os.environ['DATACUBE_ROOT'] = self.save_dcr


class TestParseDate(unittest.TestCase):
    """Unit tests for the parse_data_from_string utility function."""

    MODULE = 'cube_util'
    SUITE = 'TestParseDate'

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

        date = cube_util.parse_date_from_string('A Long Time Ago')
        self.assertEqual(date, None)


class TestLogMultiline(unittest.TestCase):
    """Unit tests for the log_multiline utility function."""

    MODULE = 'cube_util'
    SUITE = 'TestLogMultiline'

    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    def setUp(self):
        """Set up a test logger."""

        self.logger = logging.getLogger('TestLogMultiline')
        self.logger.setLevel(logging.DEBUG)

        self.handler = None

    def test_single_line_str(self):
        """Test of logging a single line string."""

        logfile_name = 'single_line_str.log'
        output_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        self.open_handler(output_path)

        line = 'This is a single line log message.'
        prefix = 'TEST: '
        cube_util.log_multiline(self.logger.debug, line, prefix=prefix)

        self.close_handler()
        self.check_expected_file(output_path, expected_path)

    def test_single_line_list(self):
        """Test of logging a single line as a list."""

        logfile_name = 'single_line_list.log'
        output_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        self.open_handler(output_path)

        line = ['This is a single line log message.']
        title = 'SINGLE LINE AS LIST'
        cube_util.log_multiline(self.logger.debug, line, title=title)

        self.close_handler()
        self.check_expected_file(output_path, expected_path)

    def test_multi_line_str(self):
        """Test of logging a multi-line string."""

        logfile_name = 'multi_line_str.log'
        output_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        self.open_handler(output_path)

        line = ('This is a multi-line log message.\n' +
                'line 2\n'
                )
        cube_util.log_multiline(self.logger.debug, line)

        self.close_handler()
        self.check_expected_file(output_path, expected_path)

    def test_multi_line_list(self):
        """Test of logging a multi-line message as a list."""

        logfile_name = 'multi_line_list.log'
        output_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        self.open_handler(output_path)

        title = 'MULTI-LINE AS LIST'
        prefix = 'TEST:'
        line = ['This is a multi-line log message.',
                'line 2',
                'line 3'
                ]
        cube_util.log_multiline(self.logger.debug,
                                line,
                                prefix=prefix,
                                title=title
                                )

        self.close_handler()
        self.check_expected_file(output_path, expected_path)

    def test_multi_line_dict(self):
        """Test of logging a dictionary."""

        logfile_name = 'multi_line_dict.log'
        output_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        self.open_handler(output_path)

        dictionary = {'first': 'first dict item',
                      'second': 'second dict item',
                      'third': 'third dict item'
                      }
        cube_util.log_multiline(self.logger.debug, dictionary)

        self.close_handler()
        self.check_expected_file(output_path, expected_path)

    def check_expected_file(self, output_path, expected_path):
        """Check the expected file matches the output file.

        This skips the test if the expected file does not exist.
        """

        if not os.path.isfile(expected_path):
            self.skipTest("Expected log file not found: " + expected_path)
        else:
            try:
                subprocess.check_output(['diff', output_path, expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("Log file does not match expected result:\n" +
                          err.output)

    def open_handler(self, output_path):
        """Set up a file handler to the output path."""

        self.handler = logging.FileHandler(output_path, mode='w')
        self.logger.addHandler(self.handler)

    def close_handler(self):
        """Flush, remove, and close the handler."""

        self.handler.flush()
        self.logger.removeHandler(self.handler)
        self.handler.close()
        self.handler = None


class TestExecute(unittest.TestCase):
    """Unit tests for the execute utility function."""

    def test_execute_echo(self):
        """Test the echo shell built-in command."""

        result = cube_util.execute('echo "Hello"')

        self.assertEqual(result['returncode'], 0)
        self.assertEqual(result['stdout'], 'Hello\n')
        self.assertEqual(result['stderr'], '')

    def test_execute_python(self):
        """Execute a python command as a list."""

        command = ['python',
                   '-c',
                   'print "Hello"'
                   ]
        result = cube_util.execute(command, shell=False)

        self.assertEqual(result['returncode'], 0)
        self.assertEqual(result['stdout'], 'Hello\n')
        self.assertEqual(result['stderr'], '')

    def test_execute_error(self):
        """Execute a python command causing an error."""

        command = ['python',
                   '-c',
                   'x = 1/0'
                   ]
        result = cube_util.execute(command, shell=False)

        self.assertNotEqual(result['returncode'], 0)
        self.assertEqual(result['stdout'], '')
        self.assertRegexpMatches(result['stderr'], 'Traceback')
        self.assertRegexpMatches(result['stderr'], 'ZeroDivisionError')


class TestCreateDirectory(unittest.TestCase):
    """Unit tests for the create_directory utility function."""

    MODULE = 'cube_util'
    SUITE = 'TestCreateDirectory'

    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)

    def setUp(self):
        self.single_dir_path = os.path.join(self.OUTPUT_DIR, 'single')
        self.multi_dir_top_path = os.path.join(self.OUTPUT_DIR, 'multi')
        self.multi_dir_full_path = os.path.join(self.multi_dir_top_path,
                                                'sub1',
                                                'sub2')

        try:
            os.remove(self.single_dir_path)
        except OSError:
            pass

        shutil.rmtree(self.single_dir_path, ignore_errors=True)
        shutil.rmtree(self.multi_dir_top_path, ignore_errors=True)

    def test_create_one(self):
        """Create a single level directory."""

        cube_util.create_directory(self.single_dir_path)
        self.assertTrue(os.path.isdir(self.single_dir_path),
                        "Directory %s not created." % self.single_dir_path)

    def test_create_multi_simple(self):
        """Create a multi level directory, simple test."""

        cube_util.create_directory(self.multi_dir_full_path)
        self.assertTrue(os.path.isdir(self.multi_dir_full_path),
                        "Directory %s not created." % self.multi_dir_full_path)

    def test_create_multi_complex(self):
        """Create a multi level directory, complex test."""

        cube_util.create_directory(self.multi_dir_top_path)
        self.assertTrue(os.path.isdir(self.multi_dir_top_path),
                        "Directory %s not created." % self.multi_dir_top_path)

        cube_util.create_directory(self.multi_dir_full_path)
        self.assertTrue(os.path.isdir(self.multi_dir_full_path),
                        "Directory %s not created." % self.multi_dir_full_path)

        cube_util.create_directory(self.multi_dir_full_path)

    def test_create_dir_error(self):
        """Trigger an unable to create directory error."""

        f = open(self.single_dir_path, 'w')
        f.close()

        self.assertRaises(cube_util.DatasetError,
                          cube_util.create_directory,
                          self.single_dir_path
                          )


class TestSynchronize(unittest.TestCase):
    """Unit tests for the synchronize utility function."""

    def test_synchronize(self):
        """Test the synchronize function."""

        sync_time = time.time() + 3.0  # 3 seconds in the future
        cube_util.synchronize(sync_time)
        finish_time = time.time()
        self.assertAlmostEqual(sync_time, finish_time, delta=0.1,
                               msg="finish_time did not match sync_time")


class TestStopwatch(unittest.TestCase):
    """Unit tests for the Stopwatch class."""

    DELTA = 0.05  # Allowable time delta for tests to pass (seconds).

    def setUp(self):
        self.sw = cube_util.Stopwatch()

    @staticmethod
    def waste_time():
        """Waste a small amount of time (~1 second).

        This uses the cube_util.synchronize function (which does
        a busy wait).
        """

        cube_util.synchronize(time.time() + random.gauss(1.0, 0.24))

    def check_times(self, estimated_time, estimated_cpu):
        """Check the estimated times against the stopwatch."""

        (elapsed_time, cpu_time) = self.sw.read()
        self.assertAlmostEqual(estimated_time, elapsed_time, delta=self.DELTA,
                               msg="elapsed_time does not match.")
        self.assertAlmostEqual(estimated_cpu, cpu_time, delta=self.DELTA,
                               msg="cpu_time does not match.")

    def test_simple(self):
        """Test a simple use of the stopwatch."""

        t = time.time()
        cpu = time.clock()
        self.sw.start()

        self.waste_time()

        t = time.time() - t
        cpu = time.clock() - cpu
        self.sw.stop()

        self.waste_time()

        self.check_times(t, cpu)

    def test_restart(self):
        """Test stopping and restarting the stopwatch."""

        t1 = time.time()
        cpu1 = time.clock()
        self.sw.start()

        self.waste_time()

        t2 = time.time()
        cpu2 = time.clock()
        self.sw.stop()

        self.waste_time()

        t3 = time.time()
        cpu3 = time.clock()
        self.sw.start()

        self.waste_time()

        t4 = time.time()
        cpu4 = time.clock()
        self.sw.stop()

        estimated_time = (t2 - t1) + (t4 - t3)
        estimated_cpu = (cpu2 - cpu1) + (cpu4 - cpu3)
        self.check_times(estimated_time, estimated_cpu)

    def test_reset(self):
        """Test resetting the stopwatch."""

        self.sw.start()

        self.waste_time()

        self.sw.reset()

        t1 = time.time()
        cpu1 = time.clock()
        self.sw.start()

        self.waste_time()

        t2 = time.time()
        cpu2 = time.clock()
        self.sw.stop()

        self.check_times(t2 - t1, cpu2 - cpu1)

#
# Test suite
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestParseDate,
                    TestGetDatacubeRoot,
                    TestLogMultiline,
                    TestExecute,
                    TestCreateDirectory,
                    TestSynchronize,
                    TestStopwatch
                    ]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())

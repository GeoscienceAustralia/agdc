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
#     * Neither Geoscience Australia nor the names of its contributors may be
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

"""
    cube_util.py - utility functions for the datacube.
"""

import os
import time
import datetime
import logging
import errno
import inspect

#
# Set up logger
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Utility Functions
#


def get_datacube_root():
    """Return the directory containing the datacube python source files.

    This returns the value of the DATACUBE_ROOT environment variable
    if it is set, otherwise it returns the directory containing the
    source code for this function (cube_util.get_datacube_root).
    """

    try:
        datacube_root = os.environ['DATACUBE_ROOT']
    except KeyError:
        this_file = inspect.getsourcefile(get_datacube_root)
        datacube_root = os.path.dirname(os.path.abspath(this_file))

    return datacube_root


def parse_date_from_string(date_string):
    """Attempt to parse a date from a command line or config file argument.

    This function tries a series of date formats, and returns a date
    object if one of them works, None otherwise.
    """

    format_list = ['%Y%m%d',
                   '%d/%m/%Y',
                   '%Y-%m-%d'
                   ]

    # Try the formats in the order listed.
    date = None
    for date_format in format_list:
        try:
            date = datetime.datetime.strptime(date_string, date_format).date()
            break
        except ValueError:
            pass

    return date


def get_file_size_mb(path):
    """Gets the size of a file (megabytes).

    Arguments:
    path: file path

    Returns:
    File size (MB)

    Raises:
    OSError [Errno=2] if file does not exist
    """
    return os.path.getsize(path) / (1024*1024)


def create_directory(dirname):
    """Create dirname, including any intermediate directories necessary to
    create the leaf directory."""
    # Allow group permissions on the directory we are about to create
    old_umask = os.umask(0o007)
    try:
        os.makedirs(dirname)
    except OSError, e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirname):
            raise DatasetError('Directory %s could not be created' % dirname)
    finally:
        # Put back the old umask
        os.umask(old_umask)


def synchronize(sync_time):
    """Pause the execution until sync_time, where sync_time is the seconds
    since 01/01/1970."""
    if sync_time is None:
        return

    float_sync_time = float(sync_time)
    while time.time() < float_sync_time:
        continue

#
# Utility classes
#


class Stopwatch(object):
    """Timer for simple performance measurements."""

    def __init__(self):
        """Initial state."""
        self.elapsed_time = 0.0
        self.cpu_time = 0.0
        self.start_elapsed_time = None
        self.start_cpu_time = None
        self.running = False

    def start(self):
        """Start the stopwatch."""
        if not self.running:
            self.start_elapsed_time = time.time()
            self.start_cpu_time = time.clock()
            self.running = True

    def stop(self):
        """Stop the stopwatch."""
        if self.running:
            self.elapsed_time += (time.time() - self.start_elapsed_time)
            self.cpu_time += (time.clock() - self.start_cpu_time)
            self.start_elapsed_time = None
            self.start_cpu_time = None
            self.running = False

    def reset(self):
        """Reset the stopwatch."""
        self.__init__()

    def read(self):
        """Read the stopwatch. Returns a tuple (elapsed_time, cpu_time)."""

        if self.running:
            curr_time = time.time()
            curr_clock = time.clock()

            self.elapsed_time += (curr_time - self.start_elapsed_time)
            self.cpu_time += (curr_clock - self.start_cpu_time)
            self.start_elapsed_time = curr_time
            self.start_cpu_time = curr_clock

        return (self.elapsed_time, self.cpu_time)

#
# Exceptions
#


class DatasetError(Exception):
    """
    A problem specific to a dataset. If raised it will cause the
    current dataset to be skipped, but the ingest process will continue.
    """

    pass


class DatasetSkipError(Exception):
    """
    A problem specific to a dataset which already exists in the DB. 
    If raised it will cause the current dataset to be skipped, but the 
    ingest process will continue.
    """

    pass

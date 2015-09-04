#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


from datetime import date
import filecmp
from pathlib import Path
from datacube.api import Satellite, DatasetType
from datacube.api.query import list_cells_to_file, list_tiles_to_file
import logging

__author__ = "Simon Oldfield"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

_log = logging.getLogger()

MIN_X = 20
MAX_X = 28

MIN_Y = 10
MAX_Y = 18

MIN_ACQ = date(2013, 1, 1)
MAX_ACQ = date(2013, 12, 31)

SATELLITE_LS578 = [Satellite.LS5, Satellite.LS7, Satellite.LS8]

DATASET_TYPE_SR = [DatasetType.USGSSR]


flatten = lambda *n: (e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,)))


def get_test_data_path(f=None):
    path = Path(__file__).parent.absolute() / "data"

    if f:
        for x in flatten(f):
            path = path / x

    return str(path)


def test_list_cells_ls578(config=None):
    filename = "usgs_cells_ls578.csv"

    list_cells_to_file(x=range(MIN_X, MAX_X + 1), y=range(MIN_Y, MAX_Y + 1),
                       acq_min=MIN_ACQ, acq_max=MAX_ACQ,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_SR,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


def test_list_tiles_ls578(config=None):
    filename = "usgs_tiles_ls578.csv"

    list_tiles_to_file(x=range(MIN_X, MAX_X + 1), y=range(MIN_Y, MAX_Y + 1),
                       acq_min=MIN_ACQ, acq_max=MAX_ACQ,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_SR,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))

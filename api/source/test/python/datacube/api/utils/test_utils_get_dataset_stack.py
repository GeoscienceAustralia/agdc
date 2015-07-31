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
import numpy
from datacube.api import Satellite, DatasetType, PqaMask
from datacube.api.model import Ls57Arg25Bands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_data_stack, NDV

__author__ = "Simon Oldfield"

import logging

_log = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def test_get_dataset_stack():

    tiles = list_tiles_as_list(x=[120], y=[-20], satellites=[Satellite.LS5, Satellite.LS7],
                               acq_min=date(2014, 1, 1), acq_max=date(2014, 12, 31),
                               dataset_types=[DatasetType.ARG25, DatasetType.PQ25])

    _log.info("\nFound %d tiles", len(tiles))

    stack = get_dataset_data_stack(tiles, DatasetType.ARG25, Ls57Arg25Bands.BLUE.name, ndv=NDV,
                                   mask_pqa_apply=True, mask_pqa_mask=[PqaMask.PQ_MASK_CLEAR])

    _log.info("\nStack is %s", numpy.shape(stack))


if __name__ == "__main__":
    test_get_dataset_stack()
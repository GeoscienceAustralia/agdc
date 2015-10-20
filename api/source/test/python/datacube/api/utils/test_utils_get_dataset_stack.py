#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


__author__ = "Simon Oldfield"


import logging
import numpy
from datacube.api import Satellite, DatasetType, PqaMask
from datacube.api.model import Ls57Arg25Bands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_data_stack, NDV
from datetime import date


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
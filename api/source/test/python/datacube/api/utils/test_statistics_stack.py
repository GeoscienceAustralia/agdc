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


import numpy
from pathlib import Path


def get_test_data_path(f=None):

    path = Path(__file__).parent.absolute()

    if f:
        from luigi.task import flatten

        for x in flatten(f):
            path = path.joinpath(x)

    return str(path)


def get_test_data(x=100, y=100, z=100):
    return numpy.load(get_test_data_path("random_{x}_{y}_{z}.npy".format(x=x, y=y, z=z)))

def test_count():
    print get_test_data()

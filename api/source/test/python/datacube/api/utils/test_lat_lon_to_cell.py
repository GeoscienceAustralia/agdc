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


import logging
from datacube.api import TileType
from datacube.api.utils import latlon_to_cell, latlon_to_xy

__author__ = "Simon Oldfield"


_log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


def test_lat_lon_to_cell_ga():
    assert latlon_to_cell(lon=142.25, lat=-24.25, tile_type=TileType.GA) == (142, -25)


def test_lat_lon_to_x_y_ga():
    assert latlon_to_xy(lon=142.25, lat=-24.25, transform=(142.0, 0.00025, 0.0, -24.0, 0.0, -0.00025), tile_type=TileType.GA) == (1000, 1000)


def test_lat_lon_to_cell_usgs():
    # CENTER of 20/10
    # -2250600.000, 3149800.000 -> -126.46197152702, 48.0708631399742
    assert latlon_to_cell(lon=-126.46197152702, lat=48.0708631399742, tile_type=TileType.USGS) == (20, 10)

    # CENTER of 20/12
    # -2250600, 3119800 -> -126.336712108846, 47.8126707927157
    assert latlon_to_cell(lon=-126.336712108846, lat=47.8126707927157, tile_type=TileType.USGS) == (20, 12)

    # CENTER of 22/22
    # -2220600, 2969800 -> -125.35370519859 46.6063235836554
    assert latlon_to_cell(lon=-125.35370519859, lat=46.6063235836554, tile_type=TileType.USGS) == (22, 22)


def test_lat_lon_to_x_y_usgs():
    assert latlon_to_xy(lon=-126.5, lat=48.0, transform=(-2265600.0, 30.0, 0.0, 3164800.0, 0.0, -30.0), tile_type=TileType.USGS) == (327, 717)

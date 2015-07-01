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

"""
Ingest WOfS tiles into the Data Cube.

Example usage:

Ingest all datasets within a folder (searched recursively):

    python -m agdc.wofs_ingester -C datacube.conf --source /path/to/datasets

Or a specific dataset:

    python -m agdc.wofs_ingester -C datacube.conf --source LS7_ETM_WATER_140_-027_2013-08-18T00-26-16.880864.tif

"""
from __future__ import absolute_import

import logging
from .pretiled import GdalMdDataset, PreTiledIngester
import agdc.ingest


_LOG = logging.getLogger(__name__)


def _is_water_file(f):
    """
    Is this the filename of a water file?
    :type f: str
    :rtype: bool

    >>> _is_water_file('LS7_ETM_WATER_144_-037_2007-11-09T23-59-30.500467.tif')
    True
    >>> _is_water_file('createWaterExtents_r3450_3752.log')
    False
    >>> _is_water_file('LC81130742014337LGN00_B1.tif')
    False
    >>> _is_water_file('LS8_OLITIRS_OTH_P51_GALPGS01-032_113_074_20141203')
    False
    >>> # We only currently care about the Tiffs:
    >>> _is_water_file('LS_WATER_150_-022_1987-05-27T23-23-00.443_2014-03-10T23-55-40.796.nc')
    False
    """
    return 'WATER' in f and f.endswith('.tif')


class WofsIngester(PreTiledIngester):
    """Ingester class for WOfS tiles."""
    def __init__(self, datacube=None, collection=None):
        super(WofsIngester, self).__init__(_is_water_file, datacube, collection)

    def open_dataset(self, dataset_path):
        """Create and return a dataset object.
        :type: dataset_path: str
        """
        return WofsDataset(dataset_path)


class WofsDataset(GdalMdDataset):
    """
    Water extent tile.

    All metadata is read from our default gdal properties.

    We may want to read some of the custom WOfS properties here in the future (?)
    """
    pass


if __name__ == '__main__':
    agdc.ingest.run_ingest(WofsIngester)

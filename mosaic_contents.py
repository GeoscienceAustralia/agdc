"""
MosaicContents: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import shutil
import logging
import os
import re
import cube_util
from cube_util import DatasetError

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
#Constants for PQA mosaic formation:
#

#PQA has valid data if this bit is set
PQA_CONTIGUITY_BIT = 8

#For a mosaiced tile, if a pixel has the contiguity bit unset in all componenet
#tiles, then set it to PQA_NODATA_VALUE in the mosaiced tile
PQA_NODATA_VALUE = 0x3EFF

PQA_NO_DATA_BITMASK = 0x01FF

# Contiguity and band saturation bits all one, others zero.
PQA_NO_DATA_CHECK_VALUE = 0x00FF

PQA_CONTIGUITY = 0x0100

class MosaicContents(object):
    """MosaicContents database interface class."""

    def __init__(self, tile_footprint, tile_record_list):
        """Create the mosaic contents."""

        assert len(tile_record_list) > 1, \
            "Attempt to make a mosaic out of a single tile."
        assert len(tile_record_list) <= 2, \
            ("Attempt to make a mosaic out of more than 2 tiles.\n" +
             "Handling for this case is not yet implemented.")

        (x_index, y_index, self.tile_type_id) = tile_footprint

        self.tile_output_path = ""

        self.temp_tile_output_path = ""

        self.tile_dict = tile_record_list[0]


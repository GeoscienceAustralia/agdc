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
TileRecord: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging
import os
from ingest_db_wrapper import IngestDBWrapper, TC_PENDING
from agdc.cube_util import get_file_size_mb
import re
import psycopg2

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class TileRecord(object):
    # pylint: disable=too-many-instance-attributes
    """TileRecord database interface class."""

    TILE_METADATA_FIELDS = ['tile_id',
                            'x_index',
                            'y_index',
                            'tile_type_id',
                            'dataset_id',
                            'tile_pathname',
                            'tile_class_id',
                            'tile_size',
                            'ctime'
                            ]
    def __init__(self, collection, dataset_record, tile_contents):
        self.collection = collection
        self.datacube = collection.datacube
        self.dataset_record = dataset_record
        self.tile_contents = tile_contents
        self.tile_footprint = tile_contents.tile_footprint
        self.tile_type_id = tile_contents.tile_type_id
        #Set tile_class_id to pending.
        self.tile_class_id = TC_PENDING
        #Set tile_id, determined below from database query
        self.tile_id = None
        self.db = IngestDBWrapper(self.datacube.db_connection)
        # Fill a dictionary with data for the tile
        tile_dict = {}
        self.tile_dict = tile_dict
        tile_dict['x_index'] = self.tile_footprint[0]
        tile_dict['y_index'] = self.tile_footprint[1]
        tile_dict['tile_type_id'] = self.tile_type_id
        tile_dict['dataset_id'] = self.dataset_record.dataset_id
        # Store final destination in the 'tile_pathname' field
        tile_dict['tile_pathname'] = self.tile_contents.tile_output_path
        tile_dict['tile_class_id'] = 1
        # The physical file is currently in the temporary location
        tile_dict['tile_size'] = \
            get_file_size_mb(self.tile_contents
                                       .temp_tile_output_path)

        self.update_tile_footprint()

        # Make the tile record entry on the database:
        self.tile_id = self.db.get_tile_id(tile_dict)
        if self.tile_id is None:
            self.tile_id = self.db.insert_tile_record(tile_dict)
        else:
            # If there was any existing tile corresponding to tile_dict then
            # it should already have been removed.
            raise AssertionError("Attempt to recreate an existing tile.")
        tile_dict['tile_id'] = self.tile_id

    def update_tile_footprint(self):
        """Update the tile footprint entry in the database"""

        if not self.db.tile_footprint_exists(self.tile_dict):
            # We may need to create a new footprint record.
            footprint_dict = {'x_index': self.tile_footprint[0],
                              'y_index': self.tile_footprint[1],
                              'tile_type_id': self.tile_type_id,
                              'x_min': self.tile_contents.tile_extents[0],
                              'y_min': self.tile_contents.tile_extents[1],
                              'x_max': self.tile_contents.tile_extents[2],
                              'y_max': self.tile_contents.tile_extents[3],
                              'bbox': 'Populate this within sql query?'}

            # Create an independent database connection for this transaction.
            my_db = IngestDBWrapper(self.datacube.create_connection())
            try:
                with self.collection.transaction(my_db):
                    if not my_db.tile_footprint_exists(self.tile_dict):
                        my_db.insert_tile_footprint(footprint_dict)

            except psycopg2.IntegrityError:
                # If we get an IntegrityError we assume the tile_footprint
                # is already in the database, and we do not need to add it.
                pass

            finally:
                my_db.close()

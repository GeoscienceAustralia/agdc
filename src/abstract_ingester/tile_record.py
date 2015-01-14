#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
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

import psycopg2

from ingest_db_wrapper import IngestDBWrapper, TC_PENDING
from agdc.cube_util import get_file_size_mb

_LOG = logging.getLogger(__name__)


class TileRecord(object):
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

    def __init__(self, dataset_id, tile_footprint, tile_type_id):
        self.dataset_id = dataset_id
        self.tile_footprint = tile_footprint
        self.tile_type_id = tile_type_id

        # Tile starts as pending
        self.tile_class_id = TC_PENDING
        # Tile ID will be set when persisted.
        self.tile_id = None


class TileRepository(object):
    def __init__(self, collection):
        super(TileRepository, self).__init__()
        self.collection = collection
        self.datacube = collection.datacube
        self.db = IngestDBWrapper(self.datacube.db_connection)

    def persist_tile(self, tile):
        # Fill a dictionary with data for the tile
        tile_dict = {
            'x_index': tile.tile_footprint[0],
            'y_index': tile.tile_footprint[1],
            'tile_type_id': tile.tile_type_id,
            'dataset_id': tile.dataset_record.dataset_id,
            # Store final destination in the 'tile_pathname' field
            # The physical file may currently be in the temporary location
            'tile_pathname': tile.tile_contents.tile_output_path,
            'tile_class_id': 1,
            'tile_size': get_file_size_mb(tile.tile_contents.temp_tile_output_path)
        }

        self._update_tile_footprint(tile, tile_dict)

        # Make the tile record entry on the database:
        tile.tile_id = tile.db.get_tile_id(tile_dict)
        if tile.tile_id is None:
            tile.tile_id = tile.db.insert_tile_record(tile_dict)
        else:
            # If there was any existing tile corresponding to tile_dict then
            # it should already have been removed.
            raise AssertionError("Attempt to recreate an existing tile.")
        tile_dict['tile_id'] = tile.tile_id

    def _update_tile_footprint(self, tile, tile_dict):
        """Update the tile footprint entry in the database"""

        if not self.db.tile_footprint_exists(tile_dict):
            # We may need to create a new footprint record.
            footprint_dict = {
                'x_index': tile.tile_footprint[0],
                'y_index': tile.tile_footprint[1],
                'tile_type_id': tile.tile_type_id,
                'x_min': tile.tile_contents.tile_extents[0],
                'y_min': tile.tile_contents.tile_extents[1],
                'x_max': tile.tile_contents.tile_extents[2],
                'y_max': tile.tile_contents.tile_extents[3],
                'bbox': 'Populate this within sql query?'
            }

            # Create an independent database connection for this transaction.
            my_db = IngestDBWrapper(self.datacube.create_connection())
            try:
                with self.collection.transaction(my_db):
                    if not my_db.tile_footprint_exists(footprint_dict):
                        my_db.insert_tile_footprint(footprint_dict)

            except psycopg2.IntegrityError:
                # If we get an IntegrityError we assume the tile_footprint
                # is already in the database, and we do not need to add it.

                # FIXME! This is migrated from the old codebase, but is clearly unsafe.
                # The query should return a count of inserted records to distinguish already-present records
                # from actual integrity errors (which are important).
                pass

            finally:
                my_db.close()
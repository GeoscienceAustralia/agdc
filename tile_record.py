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

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class TileRecord(object):
    """TileRecord database interface class."""

    def __init__(self, collection, acquisition_record,
                 dataset_record, tile_id):
        self.collection = collection
        self.acquisition_record = acquisition_record
        self.dataset_record = dataset_record
        self.tile_id = tile_id

    def make_mosaics(self):
        pass

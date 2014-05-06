"""
DatasetRecord: database interface class.

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


class DatasetRecord(object):
    """DatasetRecord database interface class."""

    def __init__(self, collection, acquisition_record, dataset_id):
        self.collection = collection
        self.acquisition_record = acquisition_record
        self.dataset_id = dataset_id

    def mark_as_tiled(self):
        pass

    def list_tile_types(self):
        pass

    def list_bands(self, tile_type_id):
        pass

    def get_coverage(self, tile_type_id):
        pass

    def create_tile_record(self, tile_footprint, tile_contents):
        pass

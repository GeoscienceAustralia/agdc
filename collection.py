"""
Collection: database interface classs.

These classes provide an interface between the database and the
top-level ingest algorithm (Ingester and its subclasses). They
also provide the implementation of the database and tile store
side of the ingest process. They are expected to be independent
of the structure of any particular dataset, but will change if
the database schema or tile store format changes.
"""

import logging

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class Collection(object):
    """Collection database interface class.

    """

    def __init__(self, datacube):
        self.datacube = datacube

    def check_metadata(self, dataset):
        pass

    def begin_transaction(self):
        pass

    def commit_transaction(self):
        pass

    def rollback_transaction(self):
        pass

    def create_acquisition_record(self, dataset):
        pass

    def create_tile_contents(self, tile_footprint, band_stack):
        pass

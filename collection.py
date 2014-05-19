"""
Collection: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging
from tile_contents import TileContents

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class Collection(object):
    """Collection database interface class."""

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

    def create_tile_contents(self, tile_type_info, tile_footprint, band_stack):
        """Factory method to create an instance of the TileContents class. The
        tile_type_dict contains the information required for resampling extents
        and resolution."""
        return TileContents(self.datacube.tile_root, tile_type_info,
                            tile_footprint, band_stack)

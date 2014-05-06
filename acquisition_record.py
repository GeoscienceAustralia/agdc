"""
AcquisitionRecord: database interface class.

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


class AcquisitionRecord(object):
    """AcquisitionRecord database interface class."""

    def __init__(self, collection, acquisition_id):
        self.collection = collection
        self.acquisition_id = acquisition_id

    def create_dataset_record(self, dataset):
        pass

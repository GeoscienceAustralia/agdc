#!/usr/bin/env python

#===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""
AcquisitionRecord: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""
from __future__ import absolute_import

import logging

from .ingest_db_wrapper import IngestDBWrapper
from .dataset_record import DatasetRecord

# Set up logger.
LOGGER = logging.getLogger(__name__)


class AcquisitionRecord(object):
    """AcquisitionRecord database interface class."""

    ACQUISITION_METADATA_FIELDS = ['satellite_tag',
                                   'sensor_name',
                                   'x_ref',
                                   'y_ref',
                                   'start_datetime',
                                   'end_datetime',
                                   'll_lon',
                                   'll_lat',
                                   'lr_lon',
                                   'lr_lat',
                                   'ul_lon',
                                   'ul_lat',
                                   'ur_lon',
                                   'ur_lat',
                                   'gcp_count',
                                   'mtl_text',
                                   'cloud_cover'
                                   ]

    def __init__(self, collection, dataset):

        self.collection = collection
        self.datacube = collection.datacube
        self.db = IngestDBWrapper(self.datacube.db_connection)
        self.acquisition_dict = {}
        self.acquisiton_id = None  # set below

        # Fill a dictonary with data for the acquisition.
        # Start with fields from the dataset metadata.
        for field in self.ACQUISITION_METADATA_FIELDS:
            self.acquisition_dict[field] = dataset.metadata_dict[field]

        # Next look up the satellite_id and sensor_id in the
        # database and fill these in.
        self.acquisition_dict['satellite_id'] = \
            self.db.get_satellite_id(self.acquisition_dict['satellite_tag'])
        self.acquisition_dict['sensor_id'] = \
            self.db.get_sensor_id(self.acquisition_dict['satellite_id'],
                                  self.acquisition_dict['sensor_name'])

        # Finally look up the acquisiton_id, or create a new record if it
        # does not exist, and fill it into the dictionary.
        self.acquisition_id = \
            self.db.get_acquisition_id_fuzzy(self.acquisition_dict)
        if self.acquisition_id is None:
            self.acquisition_id = \
                self.db.insert_acquisition_record(self.acquisition_dict)
        else:
            # Do we update the acquisition record here?
            pass
        self.acquisition_dict['acquisition_id'] = self.acquisition_id

    def create_dataset_record(self, dataset):
        """Factory method to create an instance of the DatasetRecord class.

        This method creates a new record in the database if one does not
        already exist. It will overwrite an earlier dataset record (and its
        tiles) if one exists. It will raise a DatasetError if a later (or
        equal time) record for this dataset already exists in the database.
        """

        return DatasetRecord(self.collection, self, dataset)

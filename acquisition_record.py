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
AcquisitionRecord: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging

from ingest_db_wrapper import IngestDBWrapper
from dataset_record import DatasetRecord

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


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

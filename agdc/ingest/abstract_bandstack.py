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
    abstract_bandstack.py - interface for the bandstack class.

    Different types of dataset will have different versions of this
    class, obtained by sub-classing and overriding the abstract methods.

    It is the responsibility of the stack_bands method of the dataset
    object to instantiate the correct subclass.
"""
from __future__ import absolute_import

from abc import abstractmethod, ABCMeta
from agdc.compat import with_metaclass

class AbstractBandstack(with_metaclass(ABCMeta)):
    """
    Abstract base class for band stack classes.

    The bandstack allows for the construction of a list, or stack, of
    bands from the given dataset. The band_dict is a data structure, (for
    example, a nested dictionary in the case of Landsat), which contains
    the information for each source band.
    """

    def __init__(self, dataset_mdd):
        """
        :type dataset_mdd: dict of (str, str)
        """
        self.dataset_mdd = dataset_mdd


    #
    # Interface for the band stack goes here. This is going to be used
    # by the tiling process. Most of the metadata used by the datacube,
    # including that for the tile entries, should be avaiable via the
    # Dataset class. This class just provides the data and any
    # metadata directly relevant to creating the tile contents, i.e. the
    # tile file associated with the tile database entry.
    #

    @abstractmethod
    def buildvrt(self, temporary_directory):
        """This creates a virtual raster transform (VRT), which is a reference
        to the files containing the dataset's source bands. The VRT file is
        created in the temporary_directory. This is subsequently used in the
        reprojection from the dataset's coordinate reference system to the
        datacube's coordinate reference system."""
        raise NotImplementedError

    @abstractmethod
    def list_source_files(self):
        """Given the dictionary of band source information, form a list
        of scene file names from which a VRT can be constructed. Also return a
        list of nodata values for use by add_metadata."""
        raise NotImplementedError

    @abstractmethod
    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        raise NotImplementedError

    @abstractmethod
    def add_metadata(self, vrt_filename):
        """Add metadata to the VRT."""
        raise NotImplementedError

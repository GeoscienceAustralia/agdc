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
    abstract_bandstack.py - interface for the bandstack class.

    Different types of dataset will have different versions of this
    class, obtained by sub-classing and overriding the abstract methods.

    It is the responsibility of the stack_bands method of the dataset
    object to instantiate the correct subclass.
"""

from abc import ABCMeta, abstractmethod


class AbstractBandstack(object):
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

    __metaclass__ = ABCMeta

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

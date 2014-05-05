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
    """

    __metaclass__ = ABCMeta

    #
    # Interface for the band stack goes here. This is going to be used
    # by the tiling process. Most of the metadata used by the datacube,
    # including that for the tile entries, should be avaiable via the
    # Dataset class. This class just provides the data and any
    # metadata directly relevant to creating the tile contents, i.e. the
    # tile file associated with the tile database entry.
    #

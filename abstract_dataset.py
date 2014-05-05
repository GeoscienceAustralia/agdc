"""
    abstract_dataset.py - interface for the dataset class.

    This abstract class describes the interface between the dataset
    on disk and the rest of the datacube. Datasets that have different
    packaging will have different version of this class. This is done
    by sub-classing the abstract class and overriding the abstract methods.

    It is the responsibility of the open_dataset method of the ingester
    to choose and instantiate the right dataset class for the dataset
    being opened.
"""

from abc import ABCMeta, abstractmethod


class AbstractDataset(object):
    """
    Abstract base class for dataset classes.
    """

    __metaclass__ = ABCMeta

    def __init__(self, dataset_path):
        """Initialise the dataset.

        Subclasses will likely override this, either to parse the
        metadata or to accept additional arguments or both.
        Subclasses can call the super class __init__ (i.e. this
        method) with 'AbstractDataset.__init__(self, dataset_path)'.
        """

        self.dataset_path = dataset_path

    #
    # Interface for dataset metadata goes here. Simple accessor methods
    # should be fine. This is used by the database classes to check
    # and/or fill in the dataset's metadata in the relevent database
    # records.
    #

    @abstractmethod
    def stack_bands(self, band_list):
        """Creates and returns a band_stack object from the dataset.

        band_list: a list of band numbers describing the bands to
        be included in the stack.

        PRE: The numbers in the band list must refer to bands present
        in the dataset. This method (or things that it calls) should
        raise an exception otherwise.

        POST: The object returned supports the band_stack interface
        (described below), allowing the datacube to chop the relevent
        bands into tiles.
        """

        raise NotImplementedError

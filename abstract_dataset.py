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

import os
import re
from abc import ABCMeta, abstractmethod


class AbstractDataset(object):
    """
    Abstract base class for dataset classes.
    """

    # pylint: disable=too-many-public-methods
    #
    # This class provides metadata using acessor functions. This is
    # both straight-forward and allows a docstring to be attached to
    # each to document the definition of the metadata being provided.
    #

    __metaclass__ = ABCMeta

    def __init__(self, dataset_path):
        """Initialise the dataset.

        Subclasses will likely override this, either to parse the
        metadata or to accept additional arguments or both.

        Subclasses can call the super class __init__ (i.e. this
        method) with 'AbstractDataset.__init__(self, dataset_path)'.
        """

        self._dataset_path = os.path.abspath(dataset_path)

    #
    # Accessor method for the dataset path
    #

    def get_dataset_path(self):
        """The path to the dataset on disk."""
        return self._dataset_path

    #
    # Metadata as dict utility method
    #

    def build_metadata_dict(self):
        """Returns the metadata as a python dict.

        The keys are the same as the accessor methods but without
        the 'get_' prefix, the values come from calling the accessor
        methods.

        Note that the keys are from AbstractDataset, i.e. this interface,
        but the values come from the instance, self, which will be a subclass.

        Anything *not* wanted in this dictionary should not be returned from
        an accessor method starting with 'get_', i.e. the method should be
        renamed.
        """

        mdd = {}

        for attribute in AbstractDataset.__dict__.keys():
            accessor_match = re.match(r'get_(.+)$', attribute)
            if accessor_match:
                md_key = accessor_match.group(1)
                md_value = getattr(self, accessor_match.group(0))()
                mdd[md_key] = md_value

        return mdd

    #
    # Accessor methods for dataset metadata.
    #
    # These are used to populate the database tables for new acquisition
    # records and dataset records, or to find existing records. Satellite
    # tags, sensor names, and processing levels must already exist in the
    # database for the dataset to be recoginised.
    #

    @abstractmethod
    def get_satellite_tag(self):
        """A short unique string identifying the satellite."""
        raise NotImplementedError

    @abstractmethod
    def get_sensor_name(self):
        """A short string identifying the sensor.

        The combination of satellite_tag and sensor_name must be unique.
        """
        raise NotImplementedError

    @abstractmethod
    def get_processing_level(self):
        """A short string identifying the processing level or product.

        The processing level must be unique for each satellite and sensor
        combination.
        """
        raise NotImplementedError

    @abstractmethod
    def get_x_ref(self):
        """The x (East-West axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        raise NotImplementedError

    @abstractmethod
    def get_y_ref(self):
        """The y (North-South axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        raise NotImplementedError

    @abstractmethod
    def get_start_datetime(self):
        """The start of the acquisition.

        This is a datetime without timezone in UTC.
        """
        raise NotImplementedError

    @abstractmethod
    def get_end_datetime(self):
        """The end of the acquisition.

        This is a datatime without timezone in UTC.
        """
        raise NotImplementedError


    @abstractmethod
    def get_datetime_processed(self):
        """The date and time when the dataset was processed or created.

        This is used to determine if that dataset is newer than one
        already in the database, and so should replace it.

        It is a datetime without timezone in UTC.
        """
        raise NotImplementedError

    @abstractmethod
    def get_dataset_size(self):
        """The size of the dataset in kilobytes as an integer."""
        raise NotImplementedError

    @abstractmethod
    def get_ll_lon(self):
        """The longitude of the lower left corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_ll_lat(self):
        """The lattitude of the lower left corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_lr_lon(self):
        """The longitude of the lower right corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_lr_lat(self):
        """The lattitude of the lower right corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_ul_lon(self):
        """The longitude of the upper left corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_ul_lat(self):
        """The lattitude of the upper left corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_ur_lon(self):
        """The longitude of the upper right corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_ur_lat(self):
        """The lattitude of the upper right corner of the coverage area."""
        raise NotImplementedError

    @abstractmethod
    def get_projection(self):
        """The coordinate refererence system of the image data."""
        raise NotImplementedError

    @abstractmethod
    def get_ll_x(self):
        """The x coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ll_y(self):
        """The y coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_lr_x(self):
        """The x coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_lr_y(self):
        """The y coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ul_x(self):
        """The x coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ul_y(self):
        """The y coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ur_x(self):
        """The x coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ur_y(self):
        """The y coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        raise NotImplementedError

    @abstractmethod
    def get_x_pixels(self):
        """The width of the dataset in pixels."""
        raise NotImplementedError

    @abstractmethod
    def get_y_pixels(self):
        """The height of the dataset in pixels."""
        raise NotImplementedError

    @abstractmethod
    def get_gcp_count(self):
        """The number of ground control points?"""
        raise NotImplementedError

    @abstractmethod
    def get_mtl_text(self):
        """Text information?"""
        raise NotImplementedError

    @abstractmethod
    def get_cloud_cover(self):
        """Percentage cloud cover of the aquisition if available."""
        raise NotImplementedError

    @abstractmethod
    def get_xml_text(self):
        """XML metadata text for the dataset if available."""
        raise NotImplementedError

    #
    # Methods used for tiling
    #

    @abstractmethod
    def get_geo_transform(self):
        """The affine transform between pixel and geographic coordinates.

        This is a list of six numbers describing a transformation between
        the pixel x and y coordinates and the geographic x and y coordinates
        in dataset's coordinate reference system.

        See http://www.gdal.org/gdal_datamodel for details.
        """
        raise NotImplementedError

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

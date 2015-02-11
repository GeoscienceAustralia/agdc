from collections import OrderedDict
from datetime import datetime
import logging
import os
import dateutil.parser
import dateutil.tz
from osgeo import gdal
from . import AbstractBandstack, AbstractDataset, SourceFileIngester


_LOG = logging.getLogger(__name__)


def _get_extent_gdal(dataset):
    """ Get the corner coordinates for a gdal Dataset
    :type dataset: gdal.Dataset
    :rtype: list of (list of float)
    :return: List of four corner coords: ul, ll, lr, ur
    """
    return _get_extent(dataset.GetGeoTransform(), dataset.RasterXSize, dataset.RasterYSize)


def _get_extent(gt, cols, rows):
    """ Return the corner coordinates from a geotransform

    :param gt: geotransform
    :type gt: (float, float, float, float)
    :param cols: number of columns in the dataset
    :type cols: int
    :param rows: number of rows in the dataset
    :type rows: int
    :rtype: list of (list of float)
    :return: List of four corner coords: ul, ll, lr, ur

    >>> gt = (144.0, 0.00025, 0.0, -36.0, 0.0, -0.00025)
    >>> cols = 4000
    >>> rows = 4000
    >>> _get_extent(gt, cols, rows)
    [[144.0, -36.0], [144.0, -37.0], [145.0, -37.0], [145.0, -36.0]]
    """
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([x, y])
        yarr.reverse()
    return ext


def _get_file_size(path):
    """ File size in KBs.
    :type path: str
    :rtype: int
    """
    if os.path.isdir(path):
        raise NotImplementedError('Directory size not yet supported: {!r}'.format(path))

    return os.path.getsize(path) / 1024


class PreTiledBandstack(AbstractBandstack):
    """Simple bandstack for pretiled ingestion.

    PreTiled ingestion doesn't need VRTs etc. This class can probably be eliminated
    in the future after a refactor of the framework.
    """

    def __init__(self, dataset, band_dict):
        """The bandstack allows for the construction of a list, or stack, of
            bands from the given dataset."""
        super(PreTiledBandstack, self).__init__(dataset.metadata_dict)
        self.dataset = dataset
        self.band_dict = OrderedDict(sorted(band_dict.items(), key=lambda t: t[0]))
        self.source_file_list = None
        self.nodata_list = None
        self.vrt_name = None
        self.vrt_band_stack = None

    def list_source_files(self):
        """Given the dictionary of band source information, form a list
        of scene file names from which a vrt can be constructed. Also return a
        list of nodata values for use by add_metadata"""

        file_list = []
        nodata_list = []
        for file_number in self.band_dict:
            pattern = self.band_dict[file_number]['file_pattern']
            this_file = self.dataset.find_band_file(pattern)
            file_list.append(this_file)
            nodata_list.append(self.band_dict[file_number]['nodata_value'])

        return file_list, nodata_list

    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        raise RuntimeError('No VRT for pre-tiled datasets.')

    def add_metadata(self, vrt_filename):
        """Add metadata to the VRT."""
        raise RuntimeError('No VRT for pre-tiled datasets.')

    def buildvrt(self, temporary_directory):
        raise RuntimeError('Pre-tiled ingestion does not need reprojection: No VRT option available.')


class GdalMdDataset(AbstractDataset):
    """
    A Dataset whose metadata is read from Gdal metadata.
    """

    def __init__(self, dataset_path):
        #: :type: gdal.Dataset
        self._path = dataset_path
        self._ds = gdal.Open(dataset_path)

        self._md = self._ds.GetMetadata_Dict()

        self._ul, self._ll, self._lr, self._ur = _get_extent_gdal(self._ds)

        super(GdalMdDataset, self).__init__()

    def get_processing_level(self):
        return self._md.get('level_name')

    def get_x_pixels(self):
        return self._ds.RasterXSize

    def get_y_pixels(self):
        return self._ds.RasterYSize

    def get_datetime_processed(self):
        t = os.path.getctime(self._path)
        return datetime.utcfromtimestamp(t)

    def get_geo_transform(self):
        return self._ds.GetGeoTransform()

    def get_x_ref(self):
        # Path for landsat, otherwise none. Should we support other Satellite schemes?
        return self._md.get('path')

    def get_y_ref(self):
        # TODO: WoFS has a row range, not a specific row, which doesn't fit this model.
        return None

    def get_satellite_tag(self):
        return self._md.get('satellite_tag')

    def get_gcp_count(self):
        return int(self._md['gcp_count'])

    def get_dataset_size(self):
        return _get_file_size(self._path)

    def get_xml_text(self):
        return None  # N/A?

    def get_dataset_path(self):
        return self._path

    def get_sensor_name(self):
        sensor_name = self._md.get('sensor_name')

        # FIXME: Hard-coded correction.
        # This difference is common across our systems. Maybe add an 'alias' column to sensor table?
        if sensor_name == 'ETM':
            return 'ETM+'

        return sensor_name

    def get_mtl_text(self):
        return None  # N/A?

    def _get_date_param(self, param_name):
        t = self._md[param_name]
        return dateutil.parser.parse(t, tzinfos=dateutil.tz.tzutc)

    def get_start_datetime(self):
        return self._get_date_param('start_datetime')

    def get_end_datetime(self):
        return self._get_date_param('end_datetime')

    def _get_float_param(self, param_name):
        val = self._md.get(param_name)
        if not val or val == 'None':
            return None

        return float(val)

    def get_cloud_cover(self):
        return self._get_float_param('cloud_cover')

    def get_projection(self):
        return self._ds.GetProjection()

    def get_ll_lat(self):
        return self._ll[1]

    def get_ll_lon(self):
        return self._ll[0]

    def get_ul_lat(self):
        return self._ul[0]

    def get_ul_lon(self):
        return self._ul[1]

    def get_lr_lat(self):
        return self._lr[0]

    def get_lr_lon(self):
        return self._lr[1]

    def get_ur_lat(self):
        return self._ur[0]

    def get_ur_lon(self):
        return self._ur[1]

    # ?
    get_ll_x = get_ll_lon
    get_ll_y = get_ll_lat
    get_lr_x = get_lr_lon
    get_lr_y = get_lr_lat
    get_ur_x = get_ur_lon
    get_ur_y = get_ur_lat
    get_ul_x = get_ul_lon
    get_ul_y = get_ul_lat

    def find_band_file(self, file_pattern):
        return self._path

    def stack_bands(self, band_dict):
        return PreTiledBandstack(self, band_dict)


class PreTiledIngester(SourceFileIngester):
    """Ingest data that is already tiled"""

    def tile(self, dataset_record, dataset):
        """Create tiles for a newly created or updated dataset.

        This is similar to the parent method, but performs
        no reprojections, and adds the tile in place (no file moves).

        TODO: This could share more code with parent.

        :type dataset_record: DatasetRecord
        :type dataset: AbstractDataset
        """

        # TODO: Access these fields cleanly.
        tile_type_id = int(dataset._md['tile_type_id'])
        tile_footprint = int(dataset._md['x_index']), int(dataset._md['y_index'])

        tile_bands = dataset_record.get_tile_bands(tile_type_id)
        band_stack = dataset.stack_bands(tile_bands)

        tile_contents = dataset_record.collection.create_tile_contents(
            tile_type_id,
            tile_footprint,
            band_stack,
            # Use existing path: we're not creating new tiles.
            tile_output_path=dataset.get_dataset_path()
        )

        with self.collection.lock_datasets([dataset_record.dataset_id]):
            with self.collection.transaction():
                dataset_record.store_tiles([tile_contents])
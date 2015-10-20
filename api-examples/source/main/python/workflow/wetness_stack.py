#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


__author__ = "Simon Oldfield"


import gdal
import logging
import luigi
import numpy
import os
import osr
from datacube.api.workflow.cell import Workflow, SummaryTask, CellTask
from datacube.api.model import DatasetType, TciBands
from datacube.api.utils import log_mem
from datacube.api.workflow import format_date


_log = logging.getLogger()


class WetnessWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Wetness In the Landscape - 2015-04-17")

    def create_summary_tasks(self):

        return [WetnessSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                   mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)]


class WetnessSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return WetnessCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)


class WetnessCellTask(CellTask):

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = "{satellites}_WETNESS_STACK_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
            satellites=get_satellite_string(self.satellites), x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        tiles = self.get_tiles()

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        driver = gdal.GetDriverByName("GTiff")
        assert driver

        # TODO

        raster = driver.Create(self.output().path, 4000, 4000, len(tiles), gdal.GDT_Float32, options=["BIGTIFF=YES", "INTERLEAVE=BAND"])
        assert raster

        # TODO
        raster.SetGeoTransform(transform)
        raster.SetProjection(projection)

        raster.SetMetadata(self.generate_raster_metadata())

        for index, tile in enumerate(tiles, start=1):

            # The Tassel Cap dataset is a virtual dataset derived from the NBAR so it's path is actually the NBAR path

            filename = tile.datasets[DatasetType.TCI].path

            filename = map_filename_nbar_to_wetness(filename)

            filename = os.path.join(self.output_directory, filename)

            print "+++", filename

            log_mem("Before get data")

            data = read_dataset_data(filename, bands=[TciBands.WETNESS])

            log_mem("After get data")

            band = raster.GetRasterBand(index)

            band.SetDescription(os.path.basename(filename))
            band.SetNoDataValue(numpy.nan)
            band.WriteArray(data)
            band.ComputeStatistics(True)
            band.SetMetadata({"ACQ_DATE": format_date(tile.end_datetime), "SATELLITE": tile.datasets[DatasetType.TCI].satellite.name})

            band.FlushCache()
            del band

        raster.FlushCache()
        del raster

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "WETNESS STACK",
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""
        }


def map_filename_nbar_to_wetness(filename):

        filename = os.path.basename(filename)

        filename = filename.replace("_NBAR_", "_WETNESS_")
        filename = filename.replace(".vrt", ".tif")
        filename = filename.replace(".tiff", ".tif")

        return filename


# def read_dataset_data(path, bands, x=0, y=0, x_size=None, y_size=None):
def read_dataset_data(path, bands, x=0, y=0, x_size=None, y_size=None):

    """
    Return one or more bands from a raster file

    .. note::
        Currently only support GeoTIFF

    :param path: The path of the raster file from which to read
    :type path: str
    :param bands: The bands to read
    :type bands: list(band)
    :param x: Read data starting at X pixel - defaults to 0
    :type x: int
    :param y: Read data starting at Y pixel - defaults to 0
    :type y: int
    :param x_size: Number of X pixels to read - default to ALL
    :type x_size: int
    :param y_size: Number of Y pixels to read - defaults to ALL
    :int y_size: int
    :return: dictionary of band/data as numpy array
    :rtype: dict(numpy.ndarray)
    """

    print "#=#=", path, bands

    # out = dict()

    from gdalconst import GA_ReadOnly

    raster = gdal.Open(path, GA_ReadOnly)
    assert raster

    if not x_size:
        x_size = raster.RasterXSize

    if not y_size:
        y_size = raster.RasterYSize

    # for b in bands:
    #
    #     band = raster.GetRasterBand(b.value)
    #     assert band
    #
    #     data = band.ReadAsArray(x, y, x_size, y_size)
    #     out[b] = data
    #
    #     band.FlushCache()
    #     del band

    band = raster.GetRasterBand(1)
    assert band

    data = band.ReadAsArray(x, y, x_size, y_size)
    # out[b] = data

    band.FlushCache()
    del band

    raster.FlushCache()
    del raster

    # return out
    return data


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    WetnessWorkflow().run()
#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


__author__ = "Simon Oldfield"


import luigi
import logging
import numpy
import os
from datacube.api.model import DatasetType, TciBands
from datacube.api.workflow import TileListCsvTask
from datacube.api.workflow.tile import TileTask
from datacube.api.workflow.cell_chunk import Workflow, SummaryTask, CellTask, CellChunkTask
from enum import Enum
import gdal
from datacube.api.utils import get_dataset_metadata, get_mask_pqa, get_mask_wofs, get_dataset_ndv, log_mem
from datacube.api.utils import get_dataset_data_masked, raster_create


_log = logging.getLogger()


class Statistic(Enum):
    __order__ = "COUNT MIN MAX MEAN MEDIAN SUM STANDARD_DEVIATION VARIANCE PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95"

    COUNT = "COUNT"
    COUNT_OBSERVED = "COUNT_OBSERVED"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    MEDIAN = "MEDIAN"
    SUM = "SUM"
    STANDARD_DEVIATION = "STANDARD_DEVIATION"
    VARIANCE = "VARIANCE"
    PERCENTILE_25 = "PERCENTILE_25"
    PERCENTILE_50 = "PERCENTILE_50"
    PERCENTILE_75 = "PERCENTILE_75"
    PERCENTILE_90 = "PERCENTILE_90"
    PERCENTILE_95 = "PERCENTILE_95"


class WetnessWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Wetness In the Landscape - 2015-04-17")

    def create_summary_tasks(self):

        return [WetnessSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                   mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)]


class WetnessSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return WetnessCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class WetnessCellTask(CellTask):

    def create_cell_chunk_task(self, x_offset, y_offset):

        return WetnessCellChunkTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                    satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                    chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                    x_offset=x_offset, y_offset=y_offset)

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = "{satellites}_WETNESS_STATISTICS_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
            satellites=get_satellite_string(self.satellites), x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        # Create output raster - which has len(statistics) bands

        # for each statistic

            # get the band

            # for each chunk file

                # read the chunk
                # write it to the band of the output raster

        tile = self.get_tiles()[0]

        metadata = get_dataset_metadata(tile.datasets[DatasetType.TCI])

        driver = gdal.GetDriverByName("GTiff")
        assert driver

        # Create the output TIF

        raster = driver.Create(self.output().path, metadata.shape[0], metadata.shape[1], len(Statistic), gdal.GDT_Float32)
        assert raster

        # TODO
        raster.SetGeoTransform(metadata.transform)
        raster.SetProjection(metadata.projection)

        raster.SetMetadata(self.generate_raster_metadata())

        import itertools

        for index, statistic in enumerate(Statistic, start=1):

            _log.info("Doing statistic [%s] which is band [%s]", statistic.name, index)

            band = raster.GetRasterBand(index)
            assert band

            # TODO
            band.SetNoDataValue(-999)
            band.SetDescription(statistic.name)

            for x_offset, y_offset in itertools.product(range(0, metadata.shape[0], self.chunk_size_x),
                                                        range(0, metadata.shape[1], self.chunk_size_y)):
                filename = os.path.join(self.output_directory,
                                        self.get_statistic_filename(statistic=statistic,
                                                                    ulx=x_offset, uly=y_offset,
                                                                    lrx=(x_offset + self.chunk_size_x),
                                                                    lry=(y_offset + self.chunk_size_y)))

                _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, statistic.name, filename)

                # read the chunk
                data = numpy.load(filename)

                _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
                _log.info("Writing it to (%d,%d)", x_offset, y_offset)

                # write the chunk to the TIF at the offset
                band.WriteArray(data, x_offset, y_offset)

                band.FlushCache()

            band.ComputeStatistics(True)
            band.FlushCache()

            del band

        raster.FlushCache()
        del raster

        # TODO delete .npy files?

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "WETNESS",
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
            "STATISTICS": " ".join([s.name for s in Statistic])
        }

    def get_statistic_filename(self, statistic, ulx, uly, lrx, lry):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date
        filename = "{satellites}_WETNESS_{statistic}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites), statistic=statistic.name,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max),
            ulx=ulx, uly=uly, lrx=lrx, lry=lry)
        return os.path.join(self.output_directory, filename)


class WetnessCellChunkTask(CellChunkTask):

    def requires(self):

        if self.csv:
            yield TileListCsvTask(x_min=self.x, x_max=self.x, y_min=self.y, y_max=self.y,
                                  acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                  dataset_types=self.get_dataset_types(), path=self.get_tile_csv_filename())

        # yield [self.create_tile_tasks(tile=tile) for tile in self.get_tiles()]

        for tile in self.get_tiles():
            yield self.create_tile_tasks(tile=tile)

    def create_tile_tasks(self, tile):

        return WetnessTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)

    @staticmethod
    def get_dataset_types():

        return [DatasetType.TCI]

    def get_statistic_filename(self, statistic):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date
        filename = "{satellites}_WETNESS_{statistic}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites), statistic=statistic.name,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max),
            ulx=self.x_offset, uly=self.y_offset,
            lrx=(self.x_offset + self.chunk_size_x),
            lry=(self.y_offset + self.chunk_size_y)
        )
        return os.path.join(self.output_directory, filename)

    def output(self):

        return [luigi.LocalTarget(self.get_statistic_filename(statistic)) for statistic in Statistic]

    def run(self):

        stack = list()

        for tile in self.get_tiles():

            # The Tassel Cap dataset is a virtual dataset derived from the NBAR so it's path is actually the NBAR path

            filename = tile.datasets[DatasetType.TCI].path

            filename = map_filename_nbar_to_wetness(filename)

            filename = os.path.join(self.output_directory, filename)

            print "+++", filename

            log_mem("Before get data")

            data = read_dataset_data(filename, bands=[TciBands.WETNESS],
                                     x=self.x_offset, y=self.y_offset,
                                     x_size=self.chunk_size_x, y_size=self.chunk_size_y)

            log_mem("After get data")

            # stack.append(data[TciBands.WETNESS])
            stack.append(data)

            del data

            log_mem("After adding data to stack and deleting it")

        if len(stack) == 0:
            return

        log_mem("Before COUNT")

        # COUNT
        print "COUNT"
        stack_stat = numpy.empty((self.chunk_size_x, self.chunk_size_y), dtype=numpy.float32)
        stack_stat.fill(numpy.shape(stack)[0])
        numpy.save(self.get_statistic_filename(Statistic.COUNT), stack_stat)
        del stack_stat

        log_mem("Before MIN")

        # MIN
        print "MIN"
        stack_stat = numpy.nanmin(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.MIN), stack_stat)
        del stack_stat

        log_mem("Before MAX")

        # MAX
        print "MAX"
        stack_stat = numpy.nanmax(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.MAX), stack_stat)
        del stack_stat

        log_mem("Before MEAN")

        # MEAN
        print "MEAN"
        stack_stat = numpy.nanmean(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.MEAN), stack_stat)
        del stack_stat

        log_mem("Before SUM")

        # SUM
        print "SUM"
        stack_stat = numpy.nansum(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.SUM), stack_stat)
        del stack_stat

        log_mem("Before STD")

        # STANDARD_DEVIATION
        print "STD"
        stack_stat = numpy.nanstd(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.STANDARD_DEVIATION), stack_stat)
        del stack_stat

        log_mem("Before VAR")

        # VARIANCE
        print "VAR"
        stack_stat = numpy.nanvar(stack, axis=0)
        numpy.save(self.get_statistic_filename(Statistic.VARIANCE), stack_stat)
        del stack_stat

        log_mem("Before PERCENTILES")

        # PERCENTILES
        print "PERCENTILES"
        stack_stat = numpy.nanpercentile(stack, [25, 50, 75, 90, 95], axis=0)
        numpy.save(self.get_statistic_filename(Statistic.PERCENTILE_25), stack_stat)
        del stack_stat

        # # PERCENTILE_50
        # print "P50"
        # stack_stat = numpy.nanpercentile(stack, 50, axis=0)
        # numpy.save(self.get_statistic_filename(Statistic.PERCENTILE_50), stack_stat)
        # del stack_stat
        #
        # # PERCENTILE_75
        # print "P75"
        # stack_stat = numpy.nanpercentile(stack, 75, axis=0)
        # numpy.save(self.get_statistic_filename(Statistic.PERCENTILE_75), stack_stat)
        # del stack_stat
        #
        # # PERCENTILE_90
        # print "P90"
        # stack_stat = numpy.nanpercentile(stack, 90, axis=0)
        # numpy.save(self.get_statistic_filename(Statistic.PERCENTILE_90), stack_stat)
        # del stack_stat
        #
        # # PERCENTILE_95
        # print "P95"
        # stack_stat = numpy.nanpercentile(stack, 95, axis=0)
        # numpy.save(self.get_statistic_filename(Statistic.PERCENTILE_95), stack_stat)
        # del stack_stat

        log_mem("Before OBSERVED COUNT")

        # COUNT OBSERVED - note the copy=False is modifying the array so this is done last
        print "COUNT OBSERVED"
        stack_stat = numpy.ma.masked_invalid(stack, copy=False).count(axis=0)
        numpy.save(self.get_statistic_filename(Statistic.COUNT), stack_stat)
        del stack_stat

        log_mem("DONE")


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


class WetnessTileTask(TileTask):

    def output(self):

        filename = self.tile.datasets[DatasetType.TCI].path

        # filename = os.path.basename(filename)
        #
        # filename = filename.replace("_NBAR_", "_WETNESS_")
        # filename = filename.replace(".vrt", ".tif")
        # filename = filename.replace(".tiff", ".tif")

        filename = map_filename_nbar_to_wetness(filename)

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        print "****", self.output().path

        dataset = self.tile.datasets[DatasetType.TCI]

        print "***", dataset.path

        metadata = get_dataset_metadata(dataset)

        mask = None

        # If doing PQA masking then get PQA mask

        if self.mask_pqa_apply and DatasetType.PQ25 in self.tile.datasets:
            mask = get_mask_pqa(self.tile.datasets[DatasetType.PQ25], self.mask_pqa_mask, mask=mask)

        # If doing WOFS masking then get WOFS mask

        if self.mask_wofs_apply and DatasetType.WATER in self.tile.datasets:
            mask = get_mask_wofs(self.tile.datasets[DatasetType.WATER], self.mask_wofs_mask, mask=mask)

        # TODO - no data value and data type
        ndv = get_dataset_ndv(dataset)

        data = get_dataset_data_masked(dataset, mask=mask, ndv=ndv)

        # Create ALL bands raster

        # raster_create(self.output().path, [data[b] for b in dataset.bands],
        #               metadata.transform, metadata.projection, ndv, gdal.GDT_Float32,
        #               dataset_metadata=self.generate_raster_metadata(dataset),
        #               band_ids=[b.name for b in dataset.bands])

        # Create just the WETNESS band raster

        raster_create(self.output().path, [data[TciBands.WETNESS]],
                      metadata.transform, metadata.projection, ndv, gdal.GDT_Float32,
                      dataset_metadata=self.generate_raster_metadata(dataset),
                      band_ids=[TciBands.WETNESS.name])

    def generate_raster_metadata(self, dataset):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": dataset.dataset_type.name,
            "ACQUISITION_DATE": "{acq}".format(acq=self.tile.end_datetime),
            "SATELLITE": dataset.satellite.name,
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""
        }


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    WetnessWorkflow().run()
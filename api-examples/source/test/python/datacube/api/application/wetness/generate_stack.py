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
from datetime import timedelta, datetime
import gdal
import numpy
import osr


__author__ = "Simon Oldfield"


import logging


_log = logging.getLogger()


def daterange(start_date, end_date, step):
    for n in range(0, int((end_date - start_date).days), step):
        yield start_date + timedelta(n)


def main():

    x = 140
    y = -36

    path = "/Users/simon/tmp/cube/output/applications/wetness_with_statistics_2015-04-17/stack/LS_WETNESS_{x:03d}_{y:04d}.tif".format(x=x, y=y)

    acq_dt_min = datetime(2006, 1, 1).date()
    acq_dt_max = datetime(2006, 3, 31).date()

    acq_dt_step = 8

    dates = list(daterange(acq_dt_min, acq_dt_max, acq_dt_step))

    driver = gdal.GetDriverByName("GTiff")
    assert driver

    width = 100
    height = 100

    raster = driver.Create(path, width, height, len(dates), gdal.GDT_Float32, options=["INTERLEAVE=BAND"])
    assert raster

    raster.SetGeoTransform((x, 0.00025, 0.0, y+1, 0.0, -0.00025))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    raster.SetProjection(srs.ExportToWkt())

    md = {
        "X_INDEX": "{x:03d}".format(x=x),
        "Y_INDEX": "{y:04d}".format(y=y),
        "DATASET_TYPE": "WETNESS",
        "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=acq_dt_min, acq_max=acq_dt_max),
        "SATELLITE": "LS57",
        "PIXEL_QUALITY_FILTER": "PQA_MASK_CLEAR",
        "WATER_FILTER": ""
    }

    raster.SetMetadata(md)

    for i, date in enumerate(dates, start=1):
        _log.debug("Writing %s as %d", date, i)

        data = numpy.empty((width, height), dtype=numpy.float32)
        data.fill(i)

        band = raster.GetRasterBand(i)

        band.SetDescription(str(date))
        band.SetNoDataValue(numpy.nan)
        band.WriteArray(data)
        band.ComputeStatistics(True)

        band.FlushCache()
        del band

    raster.FlushCache()
    del raster


if __name__ == "__main__":
    main()

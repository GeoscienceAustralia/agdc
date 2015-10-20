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


import logging
from enum import Enum
import os


_log = logging.getLogger(__name__)


class Satellite(Enum):
    __order__ = "LS5 LS7 LS8"

    LS5 = "LS5"
    LS7 = "LS7"
    LS8 = "LS8"


class Ls5TmBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 THERMAL_INFRAFRED SHORT_WAVE_INFRARED_2"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    THERMAL_INFRAFRED = 6
    SHORT_WAVE_INFRARED_2 = 7


class Ls7EtmBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 THERMAL_INFRAFRED SHORT_WAVE_INFRARED_2 PANACHROMATIC"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    THERMAL_INFRAFRED = 6
    SHORT_WAVE_INFRARED_2 = 7
    PANACHROMATIC = 8


class Ls8OLiTirsBands(Enum):
    __order__ = "COASTAL_AEROSOL BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2 PANACHROMATIC CIRRUS TIRS_1 TIRS_2"

    COASTAL_AEROSOL = 1
    BLUE = 2
    GREEN = 3
    RED = 4
    NEAR_INFRARED = 5
    SHORT_WAVE_INFRARED_1 = 6
    SHORT_WAVE_INFRARED_2 = 7
    PANACHROMATIC = 8
    CIRRUS = 9
    TIRS_1 = 10
    TIRS_2 = 11


class Ls57Arg25Bands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    SHORT_WAVE_INFRARED_2 = 6


class Ls8Arg25Bands(Enum):
    __order__ = "COASTAL_AEROSOL BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"

    COASTAL_AEROSOL = 1
    BLUE = 2
    GREEN = 3
    RED = 4
    NEAR_INFRARED = 5
    SHORT_WAVE_INFRARED_1 = 6
    SHORT_WAVE_INFRARED_2 = 7


class Pq25Bands(Enum):
    __order__ = "PQ"

    PQ = 1


class Fc25Bands(Enum):
    __order__ = "PHOTOSYNTHETIC_VEGETATION NON_PHOTOSYNTHETIC_VEGETATION BARE_SOIL UNMIXING_ERROR"

    PHOTOSYNTHETIC_VEGETATION = 1
    NON_PHOTOSYNTHETIC_VEGETATION = 2
    BARE_SOIL = 3
    UNMIXING_ERROR = 4


class Wofs25Bands(Enum):
    __order__ = "WATER"

    WATER = 1


class NdviBands(Enum):
    __order__ = "NDVI"

    NDVI = 1


class NdwiBands(Enum):
    __order__ = "NDWI"

    NDWI = 1


class MndwiBands(Enum):
    __order__ = "MNDWI"

    MNDWI = 1


class EviBands(Enum):
    __order__ = "EVI"

    EVI = 1


class NbrBands(Enum):
    __order__ = "NBR"

    NBR = 1


# TODO - duplication with TasselCapIndex!!!!

class TciBands(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS FOURTH FIFTH SIXTH"

    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


class DsmBands(Enum):
    __order__ = "ELEVATION SLOPE ASPECT"

    ELEVATION = 1
    SLOPE = 2
    ASPECT = 3


class DatasetType(Enum):
    __order__ = "ARG25 PQ25 FC25 DSM DEM DEM_SMOOTHED DEM_HYDROLOGICALLY_ENFORCED WATER NDVI EVI SAVI TCI NBR"

    ARG25 = "ARG25"
    PQ25 = "PQ25"
    FC25 = "FC25"
    DSM = "DSM"
    DEM = "DEM"
    DEM_SMOOTHED = "DEM_SMOOTHED"
    DEM_HYDROLOGICALLY_ENFORCED = "DEM_HYDROLOGICALLY_ENFORCED"
    WATER = "WATER"
    NDVI = "NDVI"
    EVI = "EVI"
    SAVI = "SAVI"
    TCI = "TCI"
    NBR = "NBR"
    NDWI = "NDWI"
    MNDWI = "MNDWI"


dataset_type_database = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25,
                         DatasetType.WATER,
                         DatasetType.DSM,
                         DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED]
dataset_type_filesystem = []
dataset_type_derived_nbar = [DatasetType.NDVI, DatasetType.EVI, DatasetType.NBR, DatasetType.TCI, DatasetType.NDWI, DatasetType.MNDWI]


class DatasetTile:

    #: :type: Satellite
    satellite = None
    #: :type: DatasetType
    dataset_type = None
    #: :type: str
    path = None
    #: :type: enum
    bands = None

    def __init__(self, satellite_id, type_id, path):
        self.satellite = satellite_id and Satellite[satellite_id] or None
        self.dataset_type = DatasetType[type_id]
        self.path = warp_file_paths(path)

        # TODO ???
        if (self.dataset_type, self.satellite) in BANDS:
            self.bands = BANDS[(self.dataset_type, self.satellite)]
        elif (self.dataset_type, None) in BANDS:
            self.bands = BANDS[(self.dataset_type, None)]

    @staticmethod
    def from_db_array(satellite_id, datasets):

        out = {}

        for dataset in datasets:
            dst = DatasetTile(satellite_id, dataset[0], dataset[1])
            out[dst.dataset_type] = dst

        # TODO DODGINESS until WOFS is ingested into the database

        # Construct a WOFS dataset based on the NBAR dataset
        # If one exists on the filesystem then add it otherwise (None is returned by the make_wofs_dataset) we don't

        dst = make_wofs_dataset(satellite_id, out[DatasetType.ARG25])

        if dst:
            out[DatasetType.WATER] = dst

        return out

    @staticmethod
    def from_string_array(satellite_id, datasets):

        out = {}

        # This is a string of the form {{type,path},{type,path},...}
        # TODO: This is also pretty dodgy!!!
        for entry in [x.strip("{}").split(",") for x in datasets.split("},")]:
            dst = DatasetTile(satellite_id, entry[0], entry[1])
            out[dst.dataset_type] = dst

        return out

    # TODO THIS METHOD IS ONLY HERE UNTIL WOFS EXTENTS ARE INGESTED
    # DO NOT USE!!!

    @staticmethod
    def from_path(path):

        """
        Dodgy as method to construct a Dataset from a filename
        :param filename:
        :return:
        """

        # At the moment I just need this to work for the WOFS extent files
        # which are named like
        #  LS5_TM_WATER_120_-021_2004-09-20T01-40-14.409038.tif
        #  LS7_ETM_WATER_120_-021_2006-06-30T01-45-48.187525.tif

        # And I am writing this at 4pm on a Sunday afternoon so I can't even be bothered doing it properly dodgily
        # That is, using regex'es or something...
        # Throw this away!!!

        import os
        from datacube.api.utils import extract_fields_from_filename

        out = None

        satellite, dataset_type, x, y, acq_dt = extract_fields_from_filename(os.path.basename(path))

        out = DatasetTile(satellite.value, dataset_type.value, path)

        return out


# TODO use Cell in Tile - probably

class Cell:
    #: :type: int
    x = None
    #: :type: int
    y = None
    #: :type: (int, int)
    xy = None

    def __init__(self, x_index, y_index):
        self.x = x_index
        self.y = y_index
        self.xy = (self.x, self.y)  # TODO

    @staticmethod
    def from_csv_record(record):
        return Cell(x_index=int(record["x_index"]), y_index=int(record["y_index"]))

    @staticmethod
    def from_db_record(record):
        return Cell(x_index=record["x_index"], y_index=record["y_index"])


class Tile:
    #: :type: int
    acquisition_id = None
    #: :type: int
    x = None
    #: :type: int
    y = None
    #: :type: (int, int)
    xy = None
    #: :type: datetime
    start_datetime = None
    #: :type: datetime
    end_datetime = None
    #: :type: int
    end_datetime_year = None
    #: :type: int
    end_datetime_month = None
    #: :type: str
    datasets = None

    def __init__(self, acquisition_id, x_index, y_index,
                 start_datetime, end_datetime, end_datetime_year, end_datetime_month, datasets):
        self.acquisition_id = acquisition_id
        self.x = x_index
        self.y = y_index
        self.xy = (self.x, self.y)  # TODO
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.end_datetime_year = end_datetime_year and int(end_datetime_year) or None
        self.end_datetime_month = end_datetime_month and int(end_datetime_month) or None
        self.datasets = datasets

    @staticmethod
    def from_csv_record(record):
        return Tile(
            acquisition_id=long(record["acquisition_id"]),
            x_index=int(record["x_index"]),
            y_index=int(record["y_index"]),
            start_datetime=parse_datetime(record["start_datetime"]),
            end_datetime=parse_datetime(record["end_datetime"]),
            end_datetime_year=record["end_datetime_year"],
            end_datetime_month=record["end_datetime_month"],
            datasets=DatasetTile.from_string_array(record["satellite"], record["datasets"]))

    @staticmethod
    def from_db_record(record):
        return Tile(
            acquisition_id=record["acquisition_id"],
            x_index=record["x_index"],
            y_index=record["y_index"],
            start_datetime=record["start_datetime"],
            end_datetime=record["end_datetime"],
            end_datetime_year=record["end_datetime_year"],
            end_datetime_month=record["end_datetime_month"],
            datasets=DatasetTile.from_db_array(record["satellite"], record["datasets"]))


BANDS = {
    (DatasetType.ARG25, Satellite.LS5): Ls57Arg25Bands,
    (DatasetType.ARG25, Satellite.LS7): Ls57Arg25Bands,
    (DatasetType.ARG25, Satellite.LS8): Ls8Arg25Bands,

    (DatasetType.PQ25, Satellite.LS5): Pq25Bands,
    (DatasetType.PQ25, Satellite.LS7): Pq25Bands,
    (DatasetType.PQ25, Satellite.LS8): Pq25Bands,

    (DatasetType.FC25, Satellite.LS5): Fc25Bands,
    (DatasetType.FC25, Satellite.LS7): Fc25Bands,
    (DatasetType.FC25, Satellite.LS8): Fc25Bands,

    (DatasetType.WATER, Satellite.LS5): Wofs25Bands,
    (DatasetType.WATER, Satellite.LS7): Wofs25Bands,
    (DatasetType.WATER, Satellite.LS8): Wofs25Bands,

    (DatasetType.NDVI, Satellite.LS5): NdviBands,
    (DatasetType.NDVI, Satellite.LS7): NdviBands,
    (DatasetType.NDVI, Satellite.LS8): NdviBands,

    (DatasetType.EVI, Satellite.LS5): EviBands,
    (DatasetType.EVI, Satellite.LS7): EviBands,
    (DatasetType.EVI, Satellite.LS8): EviBands,

    (DatasetType.NBR, Satellite.LS5): NbrBands,
    (DatasetType.NBR, Satellite.LS7): NbrBands,
    (DatasetType.NBR, Satellite.LS8): NbrBands,

    (DatasetType.TCI, Satellite.LS5): TciBands,
    (DatasetType.TCI, Satellite.LS7): TciBands,
    (DatasetType.TCI, Satellite.LS8): TciBands,

    (DatasetType.DSM, None): DsmBands,
    (DatasetType.DEM, None): DsmBands,
    (DatasetType.DEM_SMOOTHED, None): DsmBands,
    (DatasetType.DEM_HYDROLOGICALLY_ENFORCED, None): DsmBands,

    (DatasetType.NDWI, Satellite.LS5): NdwiBands,
    (DatasetType.NDWI, Satellite.LS7): NdwiBands,
    (DatasetType.NDWI, Satellite.LS8): NdwiBands,

    (DatasetType.MNDWI, Satellite.LS5): MndwiBands,
    (DatasetType.MNDWI, Satellite.LS7): MndwiBands,
    (DatasetType.MNDWI, Satellite.LS8): MndwiBands,
}


def get_bands(dataset_type, satellite):

    # Try WITH satellite

    if (dataset_type, satellite) in BANDS:
        return BANDS[(dataset_type, satellite)]

    # Try WITHOUT satellite

    elif (dataset_type, None) in BANDS:
        return BANDS[(dataset_type, None)]

    return None


# NOTE only on dev machine while database paths are incorrect
def warp_file_paths(path):

    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/data/cube/tiles/EPSG4326_1deg_0.00025pixel")  # For cube-dev-01
    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/data/tmp/cube/data/tiles/EPSG4326_1deg_0.00025pixel")  # For innuendo
    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/Users/simon/tmp/datacube/data/input/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel")  # For macbook

    # # My MacBook with data on external USB
    # path = path.replace("/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/Volumes/Seagate Expansion Drive/data/cube/tiles/EPSG4326_1deg_0.00025pixel")  # For macbook
    # path = path.replace("/g/data/u46/wofs/water_f7q/extents", "/Volumes/Seagate Expansion Drive/data/cube/tiles/EPSG4326_1deg_0.00025pixel/wofs_f7q/extents")  # For macbook

    return path


# TODO
def parse_datetime(s):
    from datetime import datetime
    return datetime.strptime(s[:len("YYYY-MM-DD HH:MM:SS")], "%Y-%m-%d %H:%M:%S")
    # return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")


# TODO TEMPORARY UNTIL WOFS IS AVAILABLE AS INGESTED DATA
def make_wofs_dataset(satellite_id, nbar):
    fields = os.path.basename(nbar.path).split("_")

    satellite = fields[0]

    if satellite_id == Satellite.LS8.value:

        # LS8_OLI_TIRS_NBAR_123_-025_2013-04-24T01-46-06.vrt

        if len(fields) == 7:
            sensor = fields[1] + "_" + fields[2]

            x = int(fields[4])
            y = int(fields[5])

            dt = fields[6].replace(".vrt", "").replace(".tif", "")

        # LS8_OLI_NBAR_123_-025_2013-04-24T01-46-06.vrt

        elif len(fields) == 6:
            sensor = fields[1]

            x = int(fields[3])
            y = int(fields[4])

            dt = fields[5].replace(".vrt", "").replace(".tif", "")

    else:
        # LS5_TM_NBAR_123_-025_2005-11-21T01-27-04.570000.tif
        # LS7_ETM_NBAR_123_-025_2005-11-29T01-28-07.511491.tif
        sensor = fields[1]

        x = int(fields[3])
        y = int(fields[4])

        dt = fields[5].replace(".vrt", "").replace(".tif", "")

    # path = "/g/data/u46/wofs/water_f7q/extents/{x:03d}_{y:04d}/{satellite}_{sensor}_WATER_{x:03d}_{y:04d}_{date}.tif".format(x=x, y=y, satellite=satellite, sensor=sensor, date=dt)
    # path = "/g/data/fk4/wofs/water_f7q/extents/{x:03d}_{y:04d}/{satellite}_{sensor}_WATER_{x:03d}_{y:04d}_{date}.tif".format(x=x, y=y, satellite=satellite, sensor=sensor, date=dt)
    path = "/g/data/fk4/wofs/current/extents/{x:03d}_{y:04d}/{satellite}_{sensor}_WATER_{x:03d}_{y:04d}_{date}.tif".format(x=x, y=y, satellite=satellite, sensor=sensor, date=dt)
    # path = "/g/data/u46/sjo/geoserver/wofs_f7q/extents/{x:03d}_{y:04d}/{satellite}_{sensor}_WATER_{x:03d}_{y:04d}_{date}.tif".format(x=x, y=y, satellite=satellite, sensor=sensor, date=dt)

    path = warp_file_paths(path)

    if os.path.isfile(path):
        return DatasetTile(satellite, DatasetType.WATER.value, path)

    return None

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


import logging
from enum import Enum


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
    __order__ = "BS PV NPV ERROR"

    BS = 1
    PV = 2
    NPV = 3
    ERROR = 4


class Wofs25Bands(Enum):
    __order__ = "WATER"

    WATER = 1


class DatasetType(Enum):
    __order__ = "ARG25 PQ25 FC25 DSM WATER"

    ARG25 = "ARG25"
    PQ25 = "PQ25"
    FC25 = "FC25"
    DSM = "DSM"
    WATER = "WATER"


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
        self.satellite = Satellite[satellite_id]
        self.dataset_type = DatasetType[type_id]
        self.path = warp_file_paths(path)
        self.bands = BANDS[(self.dataset_type, self.satellite)]

    @staticmethod
    def from_db_array(satellite_id, datasets):

        out = {}

        for dataset in datasets:
            dst = DatasetTile(satellite_id, dataset[0], dataset[1])
            out[dst.dataset_type] = dst

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
        self.end_datetime_year = int(end_datetime_year)
        self.end_datetime_month = int(end_datetime_month)
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
    (DatasetType.WATER, Satellite.LS8): Wofs25Bands
}


# NOTE only on dev machine while database paths are incorrect
def warp_file_paths(path):
    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/data/cube/tiles/EPSG4326_1deg_0.00025pixel")  # For cube-dev-01
    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/data/tmp/cube/data/tiles/EPSG4326_1deg_0.00025pixel")  # For innuendo
    # return path.replace("/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel", "/Users/simon/tmp/datacube/data/input/g/data1/rs0/tiles/EPSG4326_1deg_0.00025pixel")  # For macbook

    return path # For raijin


# TODO
def parse_datetime(s):
    from datetime import datetime
    return datetime.strptime(s[:len("YYYY-MM-DD HH:MM:SS")], "%Y-%m-%d %H:%M:%S")


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


import argparse
import logging
import os
from datacube.api.model import Satellite, DatasetType
from enum import Enum

__author__ = "Simon Oldfield"


_log = logging.getLogger()


def cloud_qa_mask_arg(s):
    if s in [m.name for m in Ls8CloudMask]:
        return Ls8CloudMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported Cloud QA mask".format(s))


def satellite_arg(s):
    if s in [sat.name for sat in Satellite]:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))


def month_arg(s):
    if s in [month.name for month in Month]:
        return Month[s]
    raise argparse.ArgumentTypeError("{0} is not a supported month".format(s))


def pqa_mask_arg(s):
    if s in [m.name for m in PqaMask]:
        return PqaMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported PQA mask".format(s))


def wofs_mask_arg(s):
    if s in [m.name for m in WofsMask]:
        return WofsMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported WOFS mask".format(s))


def dataset_type_arg(s):
    if s in [t.name for t in DatasetType]:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))


def statistic_arg(s):
    if s in [t.name for t in Statistic]:
        return Statistic[s]
    raise argparse.ArgumentTypeError("{0} is not a supported statistic".format(s))


def season_arg(s):
    if s in [t.name for t in Season]:
        return Season[s]
    raise argparse.ArgumentTypeError("{0} is not a supported season".format(s))


def writeable_dir(prospective_dir):
    if not os.path.exists(prospective_dir):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_dir))

    if not os.path.isdir(prospective_dir):
        raise argparse.ArgumentTypeError("{0} is not a directory".format(prospective_dir))

    if not os.access(prospective_dir, os.W_OK):
        raise argparse.ArgumentTypeError("{0} is not writeable".format(prospective_dir))

    return prospective_dir


def readable_dir(prospective_dir):
    if not os.path.exists(prospective_dir):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_dir))

    if not os.path.isdir(prospective_dir):
        raise argparse.ArgumentTypeError("{0} is not a directory".format(prospective_dir))

    if not os.access(prospective_dir, os.R_OK):
        raise argparse.ArgumentTypeError("{0} is not readable".format(prospective_dir))

    return prospective_dir


def readable_file(prospective_file):
    if not os.path.exists(prospective_file):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_file))

    if not os.path.isfile(prospective_file):
        raise argparse.ArgumentTypeError("{0} is not a file".format(prospective_file))

    if not os.access(prospective_file, os.R_OK):
        raise argparse.ArgumentTypeError("{0} is not readable".format(prospective_file))

    return prospective_file


def date_arg(s):
    try:
        return parse_date(s)

    except ValueError:
        raise argparse.ArgumentTypeError("{0} is not a valid date".format(s))


def date_min_arg(s):
    try:
        return parse_date_min(s)

    except ValueError:
        raise argparse.ArgumentTypeError("{0} is not a valid date".format(s))


def date_max_arg(s):
    try:
        return parse_date_max(s)

    except ValueError:
        raise argparse.ArgumentTypeError("{0} is not a valid date".format(s))


def dummy(path):
    _log.debug("Creating dummy output %s" % path)
    import os

    if not os.path.exists(path):
        open(path, "a").close()


def parse_date(s):
    from datetime import datetime
    return datetime.strptime(s, "%Y-%m-%d").date()


def parse_date_min(s):
    from datetime import datetime

    if s:
        if len(s) == len("YYYY"):
            return datetime.strptime(s, "%Y").date()

        elif len(s) == len("YYYY-MM"):
            return datetime.strptime(s, "%Y-%m").date()

        elif len(s) == len("YYYY-MM-DD"):
            return datetime.strptime(s, "%Y-%m-%d").date()

    return None


def parse_date_max(s):
    from datetime import datetime
    import calendar

    if s:
        if len(s) == len("YYYY"):
            d = datetime.strptime(s, "%Y").date()
            d = d.replace(month=12, day=31)
            return d

        elif len(s) == len("YYYY-MM"):
            d = datetime.strptime(s, "%Y-%m").date()

            first, last = calendar.monthrange(d.year, d.month)
            d = d.replace(day=last)
            return d

        elif len(s) == len("YYYY-MM-DD"):
            d = datetime.strptime(s, "%Y-%m-%d").date()
            return d

    return None


def parse_season_min(s):

    if s:

        i = [int(x) for x in s.split("-")]

        month = Month(i[0])

        day = 1

        if len(i) > 1:
            day = i[1]

        return month, day

    return None


def parse_season_max(s):

    if s:

        i = [int(x) for x in s.split("-")]

        month = Month(i[0])

        day = 31

        if len(i) > 1:
            day = i[1]

        return month, day

    return None


def output_format_arg(s):
    if s in [f.name for f in OutputFormat]:
        return OutputFormat[s]
    raise argparse.ArgumentTypeError("{0} is not a supported output format".format(s))


class Statistic(Enum):
    __order__ = "COUNT COUNT_OBSERVED MIN MAX MEAN SUM STANDARD_DEVIATION VARIANCE PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95"

    COUNT = "COUNT"
    COUNT_OBSERVED = "COUNT_OBSERVED"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    SUM = "SUM"
    STANDARD_DEVIATION = "STANDARD_DEVIATION"
    VARIANCE = "VARIANCE"
    PERCENTILE_25 = "PERCENTILE_25"
    PERCENTILE_50 = "PERCENTILE_50"
    PERCENTILE_75 = "PERCENTILE_75"
    PERCENTILE_90 = "PERCENTILE_90"
    PERCENTILE_95 = "PERCENTILE_95"


PERCENTILE = {
    Statistic.PERCENTILE_25: 25,
    Statistic.PERCENTILE_50: 50,
    Statistic.PERCENTILE_75: 75,
    Statistic.PERCENTILE_90: 90,
    Statistic.PERCENTILE_95: 95
}


class BandListType(Enum):
    __order__ = "EXPLICIT ALL COMMON"

    EXPLICIT = "EXPLICIT"
    ALL = "ALL"
    COMMON = "COMMON"


class TileClass(Enum):
    __order__ = "SINGLE MOSAIC"

    SINGLE = 1
    MOSAIC = 4


class CoordinateReferenceSystem(Enum):
    EPSG_4326 = "EPSG:4326"
    CONUS_ALBERS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"


class TileType(Enum):
    __order__ = "GA USGS"

    GA = 1
    USGS = 6


class ProcessingLevel(Enum):
    __order__ = "ORTHO NBAR PQA FC L1T MAP DSM DEM DEM_S DEM_H"

    ORTHO = 1
    NBAR = 2
    PQA = 3
    FC = 4
    L1T = 5
    MAP = 10
    DSM = 100
    DEM = 110
    DEM_S = 120
    DEM_H = 130
    USGS_SR_BAND = 70
    USGS_SR_ATTR = 71
    USGS_SR_INT16_ATTR = 72
    USGS_CFMASK = 73


# Define PQ mask
#   This represents bits 0-13 set which means:
#       -  0 = band 10 not saturated
#       -  1 = band 20 not saturated
#       -  2 = band 30 not saturated
#       -  3 = band 40 not saturated
#       -  4 = band 50 not saturated
#       -  5 = band 61 not saturated
#       -  6 = band 62 not saturated
#       -  7 = band 70 not saturated
#       -  8 = contiguity ok (i.e. all bands present)
#       -  9 = land (not sea)
#       - 10 = not cloud (ACCA test)
#       - 11 = not cloud (FMASK test)
#       - 12 = not cloud shadow (ACCA test)
#       - 13 = not cloud shadow (FMASK test)

class PqaMask(Enum):
    PQ_MASK_CLEAR = 16383               # bits 0 - 13 set

    PQ_MASK_SATURATION = 255            # bits 0 - 7 set
    PQ_MASK_SATURATION_OPTICAL = 159    # bits 0-4 and 7 set
    PQ_MASK_SATURATION_THERMAL = 96     # bits 5,6 set

    PQ_MASK_CONTIGUITY = 256            # bit 8 set

    PQ_MASK_LAND = 512                  # bit 9 set

    PQ_MASK_CLOUD = 15360               # bits 10-13

    PQ_MASK_CLOUD_ACCA = 1024           # bit 10 set
    PQ_MASK_CLOUD_FMASK = 2048          # bit 11 set

    PQ_MASK_CLOUD_SHADOW_ACCA = 4096    # bit 12 set
    PQ_MASK_CLOUD_SHADOW_FMASK = 8192   # bit 13 set


class WofsMask(Enum):
    DRY = 0
    NO_DATA = 1
    SATURATION_CONTIGUITY = 2
    SEA_WATER = 4
    TERRAIN_SHADOW = 8
    HIGH_SLOPE = 16
    CLOUD_SHADOW = 32
    CLOUD = 64
    WET = 128


class OutputFormat(Enum):
    __order__ = "GEOTIFF ENVI"

    GEOTIFF = "GTiff"
    ENVI = "ENVI"


class Month(Enum):
    __order__ = "JANUARY FEBRUARY MARCH APRIL MAY JUNE JULY AUGUST SEPTEMBER OCTOBER NOVEMBER DECEMBER"

    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12


class Season(Enum):
    __order__ = "SPRING SUMMER AUTUMN WINTER"

    SPRING = "SPRING"
    SUMMER = "SUMMER"
    AUTUMN = "AUTUMN"
    WINTER = "WINTER"


class Quarter(Enum):
    __order__ = "Q1 Q2 Q3 Q4"

    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"


# TODO proper handling of mutiple AEROSOL values!
# Probably change the mask to be 2 attributes - the bits to use to mask and the value expected to match

class Ls8CloudMask(Enum):
    MASK_CIRRUS_CLOUD = 0b00000001
    MASK_CLOUD = 0b00000010
    MASK_ADJACENT_TO_CLOUD = 0b00000100
    MASK_CLOUD_SHADOW = 0b00001000
    MASK_HIGH_AEROSOL = 0b00110000




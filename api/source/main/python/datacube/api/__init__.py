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


import argparse
import logging
import os
from datacube.api.model import Satellite, DatasetType
from datacube.api.utils import PqaMask, WofsMask, OutputFormat, Season, Month
from enum import Enum


_log = logging.getLogger()


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
    __order__ = "COUNT COUNT_OBSERVED MIN MAX MEAN SUM STANDARD_DEVIATION VARIANCE PERCENTILE_5 PERCENTILE_10 PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95"

    COUNT = "COUNT"
    COUNT_OBSERVED = "COUNT_OBSERVED"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    SUM = "SUM"
    STANDARD_DEVIATION = "STANDARD_DEVIATION"
    VARIANCE = "VARIANCE"
    PERCENTILE_5  = "PERCENTILE_5"
    PERCENTILE_10 = "PERCENTILE_10"
    PERCENTILE_25 = "PERCENTILE_25"
    PERCENTILE_50 = "PERCENTILE_50"
    PERCENTILE_75 = "PERCENTILE_75"
    PERCENTILE_90 = "PERCENTILE_90"
    PERCENTILE_95 = "PERCENTILE_95"


PERCENTILE = {
    Statistic.PERCENTILE_5:  5,
    Statistic.PERCENTILE_10: 10,
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






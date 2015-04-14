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


from enum import Enum
import logging
import psycopg2
import psycopg2.extras
import sys
from model import Tile, Cell, DatasetType
import os
from datacube.config import Config


_log = logging.getLogger(__name__)


class TileClass(Enum):
    __order__ = "SINGLE MOSAIC"

    SINGLE = 1
    MOSAIC = 4


TILE_CLASSES = [TileClass.SINGLE, TileClass.MOSAIC]


class TileType(Enum):
    __order__ = "ONE_DEGREE"

    ONE_DEGREE = 1


TILE_TYPE = TileType.ONE_DEGREE


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


class SortType(Enum):
    __order__ = "ASC DESC"

    ASC = "asc"
    DESC = "desc"


def print_tile(tile):
    _log.debug("id=%7d x=%3d y=%3d start=[%s] end=[%s] year=[%4d] month=[%2d] datasets=[%s]",
               tile.acquisition_id, tile.x, tile.y,
               tile.start_datetime, tile.end_datetime,
               tile.end_datetime_year, tile.end_datetime_month,
               ",".join("|".join([ds.type_id, ds.path]) for ds in tile.datasets))


def connect_to_db(config=None):

    connection = cursor = None

    # connect to database

    if not config:
        config = Config(os.path.expandvars("$HOME/.datacube/config"))
        _log.debug(config.to_str())

    connection_string = ""

    if config.get_db_host():
        connection_string += "host={host}".format(host=config.get_db_host())

    if config.get_db_port():
        connection_string += " port={port}".format(port=config.get_db_port())

    connection_string += " dbname={database} user={user} password={password}".format(database=config.get_db_database(),
                                                                                     user=config.get_db_username(),
                                                                                     password=config.get_db_password())

    connection = psycopg2.connect(connection_string)

    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("set search_path to public, {schema}".format(schema="gis, topology, ztmp"))

    return connection, cursor


def to_file_ify_sql(sql):
    return """
    copy (
    {sql}
    ) to STDOUT csv header delimiter ',' escape '"' null '' quote '"'
    """.format(sql=sql)


#
# TODO refactor common code (and SQL) from these methods
# TODO properly implement them
# TODO take some notice of requested dataset types
# TODO support DSM tiles in standard list methods
#

###
# CELLS...
###

# Cells that we DO have

def list_cells(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    Deprecated: Move to using explicit as_list or as_generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type sort: SortType
    :rtype: list[datacube.api.model.Cell]
    """
    return list_cells_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort, config)


def list_cells_as_list(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria AS A REUSABLE LIST rather than as a one-use-generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type sort: SortType
    :rtype: list[datacube.api.model.Cell]
    """
    return list(list_cells_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort, config))


def list_cells_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Cell]
    """
    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql, params = build_list_cells_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort)

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Cell.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_cells_to_file(x, y, satellites, acq_min, acq_max, dataset_types, filename, sort=SortType.ASC, config=None):

    conn = cursor = None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql, params = build_list_cells_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort)

        sql = to_file_ify_sql(sql)

        if filename:
            with open(filename, "w") as f:
                cursor.copy_expert(cursor.mogrify(sql, params), f)
        else:
            cursor.copy_expert(cursor.mogrify(sql, params), sys.stdout)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def build_list_cells_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC):

    sql = """
        SELECT DISTINCT nbar.x_index, nbar.y_index
        FROM acquisition
        JOIN satellite ON satellite.satellite_id=acquisition.satellite_id
        """

    sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_nbar)s
            ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            """

    if DatasetType.PQ25 in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_pqa)s
            ) as pq on
                pq.acquisition_id=acquisition.acquisition_id
                and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
                and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id

        """

    if DatasetType.FC25 in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_fc)s
            ) as fc on
                fc.acquisition_id=acquisition.acquisition_id
                and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DSM in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dsm)s
            ) as dsm on
                    dsm.x_index=nbar.x_index and dsm.y_index=nbar.y_index
                and dsm.tile_type_id=nbar.tile_type_id and dsm.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem)s
            ) as dem on
                    dem.x_index=nbar.x_index and dem.y_index=nbar.y_index
                and dem.tile_type_id=nbar.tile_type_id and dem.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_h)s
            ) as dem_h on
                    dem_h.x_index=nbar.x_index and dem_h.y_index=nbar.y_index
                and dem_h.tile_type_id=nbar.tile_type_id and dem_h.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_SMOOTHED in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_s)s
            ) as dem_s on
                    dem_s.x_index=nbar.x_index and dem_s.y_index=nbar.y_index
                and dem_s.tile_type_id=nbar.tile_type_id and dem_s.tile_class_id=nbar.tile_class_id
        """

    sql += """
        where
            nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
            and satellite.satellite_tag = ANY(%(satellite)s)
            and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
            and end_datetime::date between %(acq_min)s and %(acq_max)s
        """

    sql += """
        order by nbar.x_index {sort}, nbar.y_index {sort}
    """.format(sort=sort.value)

    params = {"tile_type": [TILE_TYPE.value],
              "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
              "satellite": [satellite.value for satellite in satellites],
              "x": x, "y": y,
              "acq_min": acq_min, "acq_max": acq_max,
              "level_nbar": ProcessingLevel.NBAR.value}

    if DatasetType.PQ25 in dataset_types:
        params["level_pqa"] = ProcessingLevel.PQA.value

    if DatasetType.FC25 in dataset_types:
        params["level_fc"] = ProcessingLevel.FC.value

    if DatasetType.DSM in dataset_types:
        params["level_dsm"] = ProcessingLevel.DSM.value

    if DatasetType.DEM in dataset_types:
        params["level_dem"] = ProcessingLevel.DEM.value

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        params["level_dem_h"] = ProcessingLevel.DEM_H.value

    if DatasetType.DEM_SMOOTHED in dataset_types:
        params["level_dem_s"] = ProcessingLevel.DEM_S.value

    return sql, params


# Cells that we DON'T have

# TODO currently hard coded to fractional cover

def list_cells_missing(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    Deprecated: Move to using explicit as_list or as_generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Cell]
    """
    return list_cells_missing_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort, config)


def list_cells_missing_as_list(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria AS A REUSABLE LIST rather than as a one-use-generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Cell]
    """
    return list(list_cells_missing_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort, config))


def list_cells_missing_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql, params = build_list_cells_missing_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort)

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Cell.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_cells_missing_to_file(x, y, satellites, acq_min, acq_max, dataset_types, filename, sort=SortType.ASC, config=None):

    conn = cursor = None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql, params = build_list_cells_missing_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort)

        sql = to_file_ify_sql(sql)

        if filename:
            with open(filename, "w") as f:
                cursor.copy_expert(cursor.mogrify(sql, params), f)
        else:
            cursor.copy_expert(cursor.mogrify(sql, params), sys.stdout)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def build_list_cells_missing_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC):

    sql = """
        select distinct nbar.x_index, nbar.y_index
        from acquisition
        join satellite on satellite.satellite_id=acquisition.satellite_id
        """

    sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_nbar)s
            ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
        """

    if DatasetType.PQ25 in dataset_types:
        sql += """
            left outer join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = %(level_pqa)s
                ) as pqa on
                    pqa.acquisition_id=acquisition.acquisition_id
                    and pqa.x_index=nbar.x_index and pqa.y_index=nbar.y_index
                    and pqa.tile_type_id=nbar.tile_type_id and pqa.tile_class_id=nbar.tile_class_id
            """

    if DatasetType.FC25 in dataset_types:
        sql += """
            left outer join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = %(level_fc)s
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            """

    if DatasetType.DSM in dataset_types:
        sql += """
            left outer join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dsm)s
            ) as dsm on
                    dsm.x_index=nbar.x_index and dsm.y_index=nbar.y_index
                and dsm.tile_type_id=nbar.tile_type_id and dsm.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM in dataset_types:
        sql += """
            left outer join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem)s
            ) as dem on
                    dem.x_index=nbar.x_index and dem.y_index=nbar.y_index
                and dem.tile_type_id=nbar.tile_type_id and dem.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        sql += """
            left outer join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_h)s
            ) as dem_h on
                    dem_h.x_index=nbar.x_index and dem_h.y_index=nbar.y_index
                and dem_h.tile_type_id=nbar.tile_type_id and dem_h.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_SMOOTHED in dataset_types:
        sql += """
            left outer join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_s)s
            ) as dem_s on
                    dem_s.x_index=nbar.x_index and dem_s.y_index=nbar.y_index
                and dem_s.tile_type_id=nbar.tile_type_id and dem_s.tile_class_id=nbar.tile_class_id
        """

    sql += """
        where
            nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
            and satellite.satellite_tag = ANY(%(satellite)s)
            and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
            and end_datetime::date between %(acq_min)s and %(acq_max)s
        """

    if DatasetType.PQ25 in dataset_types:
        sql += """
            and pqa.x_index is null
        """

    if DatasetType.FC25 in dataset_types:
        sql += """
            and fc.x_index is null
        """

    if DatasetType.DSM in dataset_types:
        sql += """
            and dsm.x_index is null
        """

    if DatasetType.DEM in dataset_types:
        sql += """
            and dem.x_index is null
        """

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        sql += """
            and dem_h.x_index is null
        """

    if DatasetType.DEM_SMOOTHED in dataset_types:
        sql += """
            and dem_s.x_index is null
        """

    sql += """
        order by nbar.x_index {sort}, nbar.y_index {sort}
    """.format(sort=sort.value)

    params = {"tile_type": [TILE_TYPE.value], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
              "satellite": [satellite.value for satellite in satellites],
              "x": x, "y": y,
              "acq_min": acq_min, "acq_max": acq_max,
              "level_nbar": ProcessingLevel.NBAR.value}

    if DatasetType.PQ25 in dataset_types:
        params["level_pqa"] = ProcessingLevel.PQA.value

    if DatasetType.FC25 in dataset_types:
        params["level_fc"] = ProcessingLevel.FC.value

    if DatasetType.DSM in dataset_types:
        params["level_dsm"] = ProcessingLevel.DSM.value

    if DatasetType.DEM in dataset_types:
        params["level_dem"] = ProcessingLevel.DEM.value

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        params["level_dem_h"] = ProcessingLevel.DEM_H.value

    if DatasetType.DEM_SMOOTHED in dataset_types:
        params["level_dem_s"] = ProcessingLevel.DEM_S.value

    return sql, params


###
# TILES...
###

# Tiles that we DO have

def list_tiles(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    Deprecated: Move to using explicit as_list or as_generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list_tiles_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort, config)


def list_tiles_as_list(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria AS A REUSABLE LIST rather than as a one-use-generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list(list_tiles_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort))


def list_tiles_as_generator(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type dataset_types: list[datacube.api.model.DatasetType]
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql, params = build_list_tiles_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort)

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Tile.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_tiles_to_file(x, y, satellites, years, dataset_types, filename, sort=SortType.ASC, config=None):

    conn = cursor = None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            copy (
            select
                acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
                extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
                nbar.x_index, nbar.y_index, point(nbar.x_index, nbar.y_index) as xy,
                ARRAY[
                    ['ARG25', nbar.tile_pathname],
                    ['PQ25', pq.tile_pathname],
                    ['FC25', fc.tile_pathname]
                    ] as datasets
            from acquisition
            join satellite on satellite.satellite_id=acquisition.satellite_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 2
                ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 3
                ) as pq on
                    pq.acquisition_id=acquisition.acquisition_id
                    and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
                    and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 4
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            where
                nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and satellite.satellite_tag = ANY(%(satellite)s)
                and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
                and extract(year from end_datetime) = ANY(%(year)s)

            order by end_datetime {sort}, satellite asc
            ) to STDOUT csv header delimiter ',' escape '"' null '' quote '"'
            ;
        """.format(sort=sort.value)

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "satellite": [satellite.value for satellite in satellites],
                  "x": x, "y": y,
                  "year": years}

        if filename:
            with open(filename, "w") as f:
                cursor.copy_expert(cursor.mogrify(sql, params), f)
        else:
            cursor.copy_expert(cursor.mogrify(sql, params), sys.stdout)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def build_list_tiles_sql_and_params(x, y, satellites, acq_min, acq_max, dataset_types, sort=SortType.ASC):

    sql = """
        select
            acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
            extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
            nbar.x_index, nbar.y_index, point(nbar.x_index, nbar.y_index) as xy,
        """

    sql += """
            ARRAY[
    """

    sql += """
            ['ARG25', nbar.tile_pathname]
    """

    if DatasetType.PQ25 in dataset_types:
        sql += """
            ,['PQ25', pqa.tile_pathname]
        """

    if DatasetType.FC25 in dataset_types:
        sql += """
            ,['FC25', fc.tile_pathname]
        """

    if DatasetType.NDVI in dataset_types:
        sql += """
            ,['NDVI', nbar.tile_pathname]
        """

    if DatasetType.EVI in dataset_types:
        sql += """
            ,['EVI', nbar.tile_pathname]
        """

    if DatasetType.NBR in dataset_types:
        sql += """
            ,['NBR', nbar.tile_pathname]
        """

    if DatasetType.TCI in dataset_types:
        sql += """
            ,['TCI', nbar.tile_pathname]
        """

    sql += """
            ] as datasets
    """

    sql += """
        from acquisition
        join satellite on satellite.satellite_id=acquisition.satellite_id
        """

    sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_nbar)s
            ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            """

    if DatasetType.PQ25 in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_pqa)s
            ) as pqa on
                pqa.acquisition_id=acquisition.acquisition_id
                and pqa.x_index=nbar.x_index and pqa.y_index=nbar.y_index
                and pqa.tile_type_id=nbar.tile_type_id and pqa.tile_class_id=nbar.tile_class_id

        """

    if DatasetType.FC25 in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_fc)s
            ) as fc on
                fc.acquisition_id=acquisition.acquisition_id
                and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DSM in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dsm)s
            ) as dsm on
                    dsm.x_index=nbar.x_index and dsm.y_index=nbar.y_index
                and dsm.tile_type_id=nbar.tile_type_id and dsm.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem)s
            ) as dem on
                    dem.x_index=nbar.x_index and dem.y_index=nbar.y_index
                and dem.tile_type_id=nbar.tile_type_id and dem.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_h)s
            ) as dem_h on
                    dem_h.x_index=nbar.x_index and dem_h.y_index=nbar.y_index
                and dem_h.tile_type_id=nbar.tile_type_id and dem_h.tile_class_id=nbar.tile_class_id
        """

    if DatasetType.DEM_SMOOTHED in dataset_types:
        sql += """
        join
            (
            select
                dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
            from tile
            join dataset on dataset.dataset_id=tile.dataset_id
            where dataset.level_id = %(level_dem_s)s
            ) as dem_s on
                    dem_s.x_index=nbar.x_index and dem_s.y_index=nbar.y_index
                and dem_s.tile_type_id=nbar.tile_type_id and dem_s.tile_class_id=nbar.tile_class_id
        """

    sql += """
        where
            nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
            and satellite.satellite_tag = ANY(%(satellite)s)
            and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
            and end_datetime::date between %(acq_min)s and %(acq_max)s
        """

    sql += """
        order by nbar.x_index, nbar.y_index, end_datetime {sort}, satellite asc
    """.format(sort=sort.value)

    params = {"tile_type": [TILE_TYPE.value],
              "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
              "satellite": [satellite.value for satellite in satellites],
              "x": x, "y": y,
              "acq_min": acq_min, "acq_max": acq_max,
              "level_nbar": ProcessingLevel.NBAR.value}

    if DatasetType.PQ25 in dataset_types:
        params["level_pqa"] = ProcessingLevel.PQA.value

    if DatasetType.FC25 in dataset_types:
        params["level_fc"] = ProcessingLevel.FC.value

    if DatasetType.DSM in dataset_types:
        params["level_dsm"] = ProcessingLevel.DSM.value

    if DatasetType.DEM in dataset_types:
        params["level_dem"] = ProcessingLevel.DEM.value

    if DatasetType.DEM_HYDROLOGICALLY_ENFORCED in dataset_types:
        params["level_dem_h"] = ProcessingLevel.DEM_H.value

    if DatasetType.DEM_SMOOTHED in dataset_types:
        params["level_dem_s"] = ProcessingLevel.DEM_S.value

    return sql, params


# Tiles that we DON'T have

def list_tiles_missing(x, y, satellites, acq_min, acq_max, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    Deprecated: Move to using explicit as_list or as_generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list_tiles_missing_as_generator(x, y, satellites, acq_min, acq_max, datasets, sort, config)


def list_tiles_missing_as_list(x, y, satellites, acq_min, acq_max, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria AS A REUSABLE LIST rather than as a one-use-generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list(list_tiles_missing_as_generator(x, y, satellites, acq_min, acq_max, datasets, sort, config))


def list_tiles_missing_as_generator(x, y, satellites, acq_min, acq_max, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    :type x: list[int]
    :type y: list[int]
    :type satellites: list[datacube.api.model.Satellite]
    :type acq_min: datetime.date
    :type acq_max: datetime.date
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            select
                acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
                extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
                NBAR.x_index, NBAR.y_index, point(NBAR.x_index, NBAR.y_index) as xy,
                ARRAY[
                    ['ARG25', NBAR.tile_pathname]
                    ] as datasets
            from acquisition
            join satellite on satellite.satellite_id=acquisition.satellite_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 2
                ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            left outer join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 4
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            where
                nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and satellite.satellite_tag = ANY(%(satellite)s)
                and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
                and end_datetime::date between %(acq_min)s and %(acq_max)s
                and fc.acquisition_id is null

            order by nbar.x_index {sort}, nbar.y_index {sort}
            ;
        """.format(sort=sort.value)

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "satellite": [satellite.value for satellite in satellites],
                  "x": x, "y": y,
                  "acq_min": acq_min, "acq_max": acq_max}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Tile.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_tiles_missing_to_file(x, y, satellites, years, datasets, filename, sort=SortType.ASC, config=None):

    conn = cursor = None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            copy (
            select
                acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
                extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
                NBAR.x_index, NBAR.y_index, point(NBAR.x_index, NBAR.y_index) as xy,
                ARRAY[
                    ['ARG25', NBAR.tile_pathname]
                    ] as datasets
            from acquisition
            join satellite on satellite.satellite_id=acquisition.satellite_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 2
                ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            left outer join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 4
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            where
                nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and satellite.satellite_tag = ANY(%(satellite)s)
                and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
                and end_datetime::date between %(acq_min)s and %(acq_max)s
                and fc.acquisition_id is null

            order by nbar.x_index {sort}, nbar.y_index {sort}
            ) to STDOUT csv header delimiter ',' escape '"' null '' quote '"'
            ;
        """.format(sort=sort.value)

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "satellite": [satellite.value for satellite in satellites],
                  "x": x, "y": y,
                  "year": years}

        if filename:
            with open(filename, "w") as f:
                cursor.copy_expert(cursor.mogrify(sql, params), f)
        else:
            cursor.copy_expert(cursor.mogrify(sql, params), sys.stdout)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


# DEM/DSM tiles - quickie to get DEM/DSM tiles for WOFS - note they have no acquisition information!!!!

def list_tiles_dtm(x, y, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    Deprecated: Move to using explicit as_list or as_generator

    :type x: list[int]
    :type y: list[int]
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list_tiles_dtm_as_generator(x, y, datasets, sort, config)


def list_tiles_dtm_as_list(x, y, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria AS A REUSABLE LIST rather than as a one-use-generator

    :type x: list[int]
    :type y: list[int]
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """
    return list(list_tiles(x, y, datasets, sort, config))


def list_tiles_dtm_as_generator(x, y, datasets, sort=SortType.ASC, config=None):

    """
    Return a list of cells matching the criteria as a SINGLE-USE generator

    :type x: list[int]
    :type y: list[int]
    :type datasets: list[datacube.api.model.DatasetType]
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type sort: SortType
    :rtype: list[datacube.api.model.Tile]
    """

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            select
                null acquisition_id, null satellite, null start_datetime, null end_datetime,
                null end_datetime_year, null end_datetime_month,
                dsm.x_index, dsm.y_index, point(dsm.x_index, dsm.y_index) as xy,
                ARRAY[
                    ['DSM', DSM.tile_pathname],
                    ['DEM', DEM.tile_pathname],
                    ['DEM_HYDROLOGICALLY_ENFORCED', DEM_H.tile_pathname],
                    ['DEM_SMOOTHED', DEM_S.tile_pathname]
                    ] as datasets
            from
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 100
                ) as DSM
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 110
                ) as DEM on
                        DEM.x_index=DSM.x_index and DEM.y_index=DSM.y_index
                    and DEM.tile_type_id=DSM.tile_type_id and DEM.tile_class_id=DSM.tile_class_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 120
                ) as DEM_S on
                        DEM_S.x_index=DSM.x_index and DEM_S.y_index=DSM.y_index
                    and DEM_S.tile_type_id=DSM.tile_type_id and DEM_S.tile_class_id=DSM.tile_class_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 130
                ) as DEM_H on
                        DEM_H.x_index=DSM.x_index and DEM_H.y_index=DSM.y_index
                    and DEM_H.tile_type_id=DSM.tile_type_id and DEM_H.tile_class_id=DSM.tile_class_id
            where
                DSM.tile_type_id = ANY(%(tile_type)s) and DSM.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and DSM.x_index = ANY(%(x)s) and DSM.y_index = ANY(%(y)s)
            ;
        """.format()

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "x": x, "y": y}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Tile.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


###
# Other stuff - mostly incomplete...
###

def visit_tiles(x, y, satellites, years, datasets, func=print_tile, sort=SortType.ASC, config=None):

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            select
                acquisition.acquisition_id, satellite_tag, start_datetime, end_datetime,
                extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
                nbar.x_index, nbar.y_index, point(nbar.x_index, nbar.y_index) as xy,
                ARRAY[
                    ['ARG25', nbar.tile_pathname],
                    ['PQ25', pq.tile_pathname],
                    ['FC25', fc.tile_pathname]
                    ] as datasets
            from acquisition
            join satellite on satellite.satellite_id=acquisition.satellite_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 2
                ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 3
                ) as pq on
                    pq.acquisition_id=acquisition.acquisition_id
                    and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
                    and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 4
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            where
                nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and satellite.satellite_id = ANY(%(satellite)s)
                and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
                and extract(year from end_datetime) = ANY(%(year)s)
            ;
        """

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "satellite": [satellite.value for satellite in satellites],
                  "x": [x], "y": [y],
                  "year": years}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in cursor:
            _log.debug(record)
            func(Tile.from_db_record(record))

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_tiles_wkt(wkt, satellites, years, datasets, sort=SortType.ASC, config=None):

    conn, cursor = None, None

    try:
        # connect to database

        conn, cursor = connect_to_db(config=config)

        sql = """
            select
                acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
                extract(year from end_datetime)::integer as end_datetime_year, extract(month from end_datetime)::integer as end_datetime_month,
                nbar.x_index, nbar.y_index, point(nbar.x_index, nbar.y_index) as xy,
                ARRAY[
                    ['ARG25', nbar.tile_pathname],
                    ['PQ25', pq.tile_pathname],
                    ['FC25', fc.tile_pathname]
                    ] as datasets
            from acquisition
            join satellite on satellite.satellite_id=acquisition.satellite_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 2
                ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 3
                ) as pq on
                    pq.acquisition_id=acquisition.acquisition_id
                    and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
                    and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id
            join
                (
                select
                    dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
                from tile
                join dataset on dataset.dataset_id=tile.dataset_id
                where dataset.level_id = 4
                ) as fc on
                    fc.acquisition_id=acquisition.acquisition_id
                    and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
                    and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
            join tile_footprint on
                tile_footprint.x_index = nbar.x_index
                and tile_footprint.y_index = nbar.y_index
                and tile_footprint.tile_type_id = nbar.tile_type_id

            where
                nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
                and satellite.satellite_tag = ANY(%(satellite)s)
                and st_intersects(tile_footprint.bbox, st_geomfromtext(%(polygon)s, 4326))
                and extract(year from end_datetime) = ANY(%(year)s)

            order by end_datetime asc, satellite asc
            ;
        """

        params = {"tile_type": [1], "tile_class": [tile_class.value for tile_class in TILE_CLASSES],
                  "satellite": [satellite.value for satellite in satellites],
                  "polygon": wkt,
                  "year": years}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        tiles = []

        for record in cursor:
            _log.debug(record)
            tiles.append(Tile.from_db_record(record))

        return tiles

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()
        raise

    finally:

        conn = cursor = None


def list_tiles_wkt_to_file(wkt, years, datasets, format, filename, sort=SortType.ASC, config=None):
    pass


def visit_tiles_wkt(wkt, years, datasets, sort=SortType.ASC, config=None):
    pass


def result_generator(cursor, size=100):

    while True:

        results = cursor.fetchmany(size)

        if not results:
            break

        for result in results:
            yield result



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
import argparse
import psycopg2
import psycopg2.extras
import sys
from model import Tile, Cell


_log = logging.getLogger(__name__)


TILE_CLASSES = [1, 4]   # Change to [1, 4] when database contains overlap mosaic tiles


def main():

    logging.basicConfig(#filename=filename,
                        #level=logging.INFO,
                        level=logging.DEBUG,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(prog=__name__, description="Retrieve a list of matching tiles")

    parser.add_argument("--host", help="DB host", action="store", dest="host", type=str)
    parser.add_argument("--port", help="DB port", action="store", dest="port", type=int)
    parser.add_argument("--database", help="DB user", action="store", dest="database", type=str, default="cube")
    parser.add_argument("--username", help="DB user", action="store", dest="username", type=str, default="nemo")
    parser.add_argument("--password", help="DB password", action="store", dest="password", type=str, default="nemo")

    parser.add_argument("--x", help="X index of tiles", action="store", dest="x", type=int, choices=range(110, 155))
    parser.add_argument("--y", help="Y index of tiles", action="store", dest="y", type=int, choices=range(-45, -10))
    parser.add_argument("--time", help="time range tiles - currently supports a list of years", action="store",
                        dest="times", type=int, nargs="+", choices=range(1987, 2013))
    # parser.add_argument("--time", help="year", action="store", dest="time", type=int, choices=range(1987, 2013))
    parser.add_argument("--dataset", help="datasets", action="store", dest="datasets", type=str, nargs="+",
                        choices=["ARG25", "PQ25", "FC25"], default="ARG25 PQ25 FC25")
    parser.add_argument("--format", help="format", action="store", dest="format", type=str, choices=["csv"])
    parser.add_argument("--filename", help="filename", action="store", dest="filename", type=str, default=None)

    args = parser.parse_args()

    _log.debug("DB host=[%s] port=[%s] user=[%s] password=[%s]", args.host, args.port, args.username, args.password)
    _log.debug("x=[%s] y=[%s] time=[%s] dataset=[%s] format=[%s] filename=[%s]",
               args.x, args.y, args.times, args.datasets, args.format, args.filename)

    args_ok = True

    if not args.x or not args.y:
        _log.error("No X/Y provided")
        args_ok = False

    if not args.times:
        _log.error("No year provided")
        args_ok = False

    if not args_ok:
        parser.print_help()
        return

    list_tiles_to_file(args.x, args.y,
                       args.times, args.datasets,
                       args.format, args.filename,
                       args.database, args.username, args.password, host=args.host, port=args.port)


def print_tile(tile):
    _log.debug("id=%7d x=%3d y=%3d start=[%s] end=[%s] year=[%4d] month=[%2d] datasets=[%s]",
               tile.acquisition_id, tile.x, tile.y,
               tile.start_datetime, tile.end_datetime,
               tile.end_datetime_year, tile.end_datetime_month,
               ",".join("|".join([ds.type_id, ds.path]) for ds in tile.datasets))

#
# TODO refactor common code from these methods
#


def list_cells(x, y, satellites, acq_min, acq_max, datasets, database, user, password, host=None, port=None, sortby="asc"):

    conn, cursor = None, None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

        sql = """
            select distinct nbar.x_index, nbar.y_index
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
                and end_datetime::date between %(acq_min)s and %(acq_max)s

            order by nbar.x_index {sortby}, nbar.y_index {sortby}
            ;
        """.format(sortby=sortby)

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
                  "x": x, "y": y,
                  "acq_min": acq_min, "acq_max": acq_max}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        for record in result_generator(cursor):
            _log.debug(record)
            yield Cell.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()


def list_tiles(x, y, satellites, acq_min, acq_max, datasets, database, user, password, host=None, port=None, sortby="asc"):

    conn, cursor = None, None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

        sql = """
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
                and end_datetime::date between %(acq_min)s and %(acq_max)s

            order by end_datetime {sortby}, satellite asc
            ;
        """.format(sortby=sortby)

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
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


# def list_tiles_by_month(x, y, satellites, months, datasets, database, user, password, host=None, port=None, sortby="asc"):
#
#     conn, cursor = None, None
#
#     try:
#         # connect to database
#
#         connection_string = ""
#
#         if host:
#             connection_string += "host={host}".format(host=host)
#
#         if port:
#             connection_string += " port={port}".format(port=port)
#
#         connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)
#
#         conn = psycopg2.connect(connection_string)
#
#         cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#         cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))
#
#         # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?
#
#         # This query allows the user to specify
#         #   filter results by
#         #       satellite
#         #       x/y range
#         #       time range (use end_datetime)?
#         #
#         #   request which datasets they want
#         #       NBAR
#         #       PQ
#         #       FC
#
#         # It also needs to internally filter to
#         #   tile.tile_type_id in (1)
#         #   tile.tile_class_id in (1,3)
#
#         sql = """
#             select
#                 acquisition.acquisition_id, satellite_tag as satellite, start_datetime, end_datetime,
#                 extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
#                 nbar.x_index, nbar.y_index, point(nbar.x_index, nbar.y_index) as xy,
#                 ARRAY[
#                     ['ARG25', nbar.tile_pathname],
#                     ['PQ25', pq.tile_pathname],
#                     ['FC25', fc.tile_pathname]
#                     ] as datasets
#             from acquisition
#             join satellite on satellite.satellite_id=acquisition.satellite_id
#             join
#                 (
#                 select
#                     dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
#                 from tile
#                 join dataset on dataset.dataset_id=tile.dataset_id
#                 where dataset.level_id = 2
#                 ) as nbar on nbar.acquisition_id=acquisition.acquisition_id
#             join
#                 (
#                 select
#                     dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
#                 from tile
#                 join dataset on dataset.dataset_id=tile.dataset_id
#                 where dataset.level_id = 3
#                 ) as pq on
#                     pq.acquisition_id=acquisition.acquisition_id
#                     and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
#                     and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id
#             join
#                 (
#                 select
#                     dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
#                 from tile
#                 join dataset on dataset.dataset_id=tile.dataset_id
#                 where dataset.level_id = 4
#                 ) as fc on
#                     fc.acquisition_id=acquisition.acquisition_id
#                     and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
#                     and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
#             where
#                 nbar.tile_type_id = ANY(%(tile_type)s) and nbar.tile_class_id = ANY(%(tile_class)s) -- mandatory
#                 and satellite.satellite_tag = ANY(%(satellite)s)
#                 and nbar.x_index = ANY(%(x)s) and nbar.y_index = ANY(%(y)s)
#                 and extract(year from end_datetime) = ANY(%(year)s)
#
#             order by end_datetime {sortby}, satellite asc
#             ;
#         """.format(sortby=sortby)
#
#         params = {"tile_type": [1], "tile_class": TILE_CLASSES,
#                   "satellite": satellites,
#                   "x": x, "y": y,
#                   "year": years}
#
#         _log.debug(cursor.mogrify(sql, params))
#
#         cursor.execute(sql, params)
#
#         # tiles = []
#         #
#         # for record in cursor:
#         #     _log.debug(record)
#         #     tiles.append(Tile.from_db_record(record))
#         #
#         # return tiles
#
#         for record in result_generator(cursor):
#             _log.debug(record)
#             yield Tile.from_db_record(record)
#
#     except Exception as e:
#
#         _log.error("Caught exception %s", e)
#         conn.rollback()


# TODO rename this to be the "standard" list_tiles and the above to be list_tiles_for_year or something

def list_tiles_between_dates(x, y, satellites, start_dt, stop_dt, datasets, database, user, password, host=None, port=None, sortby="asc"):

    conn, cursor = None, None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

        sql = """
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
                and end_datetime between %(start_dt)s and %(stop_dt)s

            order by end_datetime {sortby}, satellite asc
            ;
        """.format(sortby=sortby)

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
                  "x": x, "y": y,
                  "start_dt": start_dt,
                  "stop_dt": stop_dt}

        _log.debug(cursor.mogrify(sql, params))

        cursor.execute(sql, params)

        # tiles = []
        #
        # for record in cursor:
        #     _log.debug(record)
        #     tiles.append(Tile.from_db_record(record))
        #
        # return tiles

        for record in result_generator(cursor):
            _log.debug(record)
            yield Tile.from_db_record(record)

    except Exception as e:

        _log.error("Caught exception %s", e)
        conn.rollback()


def result_generator(cursor, size=100):

    while True:

        results = cursor.fetchmany(size)

        if not results:
            break

        for result in results:
            yield result


def list_tiles_to_file(x, y, satellites, years, datasets, filename, database, user, password, host=None, port=None, sortby="asc"):

    conn = cursor = None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor()
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

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

            order by end_datetime {sortby}, satellite asc
            ) to STDOUT csv header delimiter ',' escape '"' null '' quote '"'
            ;
        """.format(sortby=sortby)

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
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

    finally:

        conn = cursor = None


def list_cells_to_file(x, y, satellites, years, datasets, filename, database, user, password, host=None, port=None, sortby="asc"):

    conn = cursor = None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor()
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

        sql = """
            copy (
            select distinct nbar.x_index, nbar.y_index
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

            order by nbar.x_index {sortby}, nbar.y_index {sortby}
            ) to STDOUT csv header delimiter ',' escape '"' null '' quote '"'
            ;
        """.format(sortby=sortby)

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
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

    finally:

        conn = cursor = None


def visit_tiles(x, y, satellites, years, datasets, database, user, password, host=None, port=None, func=print_tile):

    conn, cursor = None, None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

        # So in terms of SQL first filter back to that tile list

        # select acquisition.acquisition_id, satellite_tag, start_datetime, end_datetime,
        # extract(year from end_datetime) as end_datetime_year, extract(month from end_datetime) as end_datetime_month,
        # nbar.tile_pathname, pq.tile_pathname, fc.tile_pathname
        # from acquisition
        # join satellite on satellite.satellite_id=acquisition.satellite_id
        # join
        # 	(
        # 	select
        # 		dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
        # 	from tile
        # 	join dataset on dataset.dataset_id=tile.dataset_id
        # 	where dataset.level_id = 2
        # 	) as nbar on nbar.acquisition_id=acquisition.acquisition_id
        # join
        # (
        # select
        # 	dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
        # from tile
        # join dataset on dataset.dataset_id=tile.dataset_id
        # where dataset.level_id = 3
        # ) as pq on
        # 	pq.acquisition_id=acquisition.acquisition_id
        # 	and pq.x_index=nbar.x_index and pq.y_index=nbar.y_index
        # 	and pq.tile_type_id=nbar.tile_type_id and pq.tile_class_id=nbar.tile_class_id
        # join
        # (
        # select
        # 	dataset.acquisition_id, tile.dataset_id, tile.x_index, tile.y_index, tile.tile_pathname, tile.tile_type_id, tile.tile_class_id
        # from tile
        # join dataset on dataset.dataset_id=tile.dataset_id
        # where dataset.level_id = 4
        # ) as fc on
        # 	fc.acquisition_id=acquisition.acquisition_id
        # 	and fc.x_index=nbar.x_index and fc.y_index=nbar.y_index
        # 	and fc.tile_type_id=nbar.tile_type_id and fc.tile_class_id=nbar.tile_class_id
        # where
        # nbar.tile_type_id in (1) and nbar.tile_class_id in (1,3) -- mandatory
        # and satellite.satellite_id in (1,2,3)
        # and nbar.x_index in (148,149) and nbar.y_index in (-35, -36)
        # and date_trunc('year', end_datetime) = '2012-01-01'::timestamp
        # ;

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

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": [1, 2, 3],
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


def list_tiles_wkt(wkt, satellites, years, datasets, database, user, password, host=None, port=None):

    conn, cursor = None, None

    try:
        # connect to database

        connection_string = ""

        if host:
            connection_string += "host={host}".format(host=host)

        if port:
            connection_string += " port={port}".format(port=port)

        connection_string += " dbname={database} user={user} password={password}".format(database=database, user=user, password=password)

        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("set search_path to {schema}, public".format(schema="gis, topology, ztmp"))

        # Should the DB model be changed so that DATASET FKs to TILE not TILE FKs to DATASET?

        # This query allows the user to specify
        #   filter results by
        #       satellite
        #       x/y range
        #       time range (use end_datetime)?
        #
        #   request which datasets they want
        #       NBAR
        #       PQ
        #       FC

        # It also needs to internally filter to
        #   tile.tile_type_id in (1)
        #   tile.tile_class_id in (1,3)

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

        params = {"tile_type": [1], "tile_class": TILE_CLASSES,
                  "satellite": satellites,
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


def list_tiles_wkt_to_file(wkt, years, datasets, format, filename, database, user, password, host=None, port=None):
    pass


def visit_tiles_wkt(wkt, years, datasets, database, user, password, host=None, port=None):
    pass


if __name__ == "__main__":
    main()
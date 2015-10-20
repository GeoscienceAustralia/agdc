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
import csv
from fnmatch import fnmatch
import sqlite3

__author__ = "Simon Oldfield"


import logging


_log = logging.getLogger()


# TODO
filename = "LS8_OLI.OVERLAP.FS.txt"


def main():

    conn = None

    try:
        conn = sqlite3.connect(filename + ".db")

        cursor = conn.cursor()

        # cursor.execute("drop table x")
        cursor.execute("create table if not exists x(overlap text unique, north text, south text)")

        # # do the initial populate
        # populate(cursor)
        # conn.commit()

        cursor.execute("select count(*) from x")
        _log.info("There are %d records", cursor.fetchone()[0])

        cursor.execute("select overlap, north, south from x")

        for record in cursor.fetchall():
            # _log.info("overlap=%s north=%s south=%s", record["overlap"][:-10], record["north"][:-10], record["south"][:-10])
            _log.info(record)

    except Exception, e:
        _log.error("Caught exception: %s", e)

    finally:
        if conn:
            conn.close()


def populate(cursor):

    with open(filename, "rb") as f:
        csv_reader = csv.reader(f, delimiter=",")

        for record in csv_reader:
            _log.info("Inserting (if not already there) %s", record[0])

            cursor.execute("insert or ignore into x (overlap) values (?)", (record[0],))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    main()

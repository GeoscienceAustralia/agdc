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

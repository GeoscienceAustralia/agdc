#===============================================================================
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


import ConfigParser
from enum import Enum
import os


class Config:

    class Section(Enum):
        DATABASE = "DATABASE"

    class DatabaseKey(Enum):
        HOST = "host"
        PORT = "port"
        DATABASE = "database"
        USERNAME = "username"
        PASSWORD = "password"
        SCHEMAS = "schemas"

    _config = None

    def __init__(self, path=None):

        import StringIO

        self._config = ConfigParser.SafeConfigParser()

        # Read default config
        self._config.readfp(StringIO.StringIO(self._DEFAULT_CONFIG))

        # Read additional config(s)
        if path and os.path.isfile(path):
            self._config.read(path)

    def _get_string(self, section, key):
        return self._config.get(section.value, key.value)

    def _get_int(self, section, key):
        return int(self._config.get(section.value, key.value))

    def get_db_host(self):
        '''
        Get the DB host

        :return:
        '''
        return self._get_string(Config.Section.DATABASE, Config.DatabaseKey.HOST)

    def get_db_port(self):
        return self._get_int(Config.Section.DATABASE, Config.DatabaseKey.PORT)

    def get_db_database(self):
        return self._get_string(Config.Section.DATABASE, Config.DatabaseKey.DATABASE)

    def get_db_username(self):
        return self._get_string(Config.Section.DATABASE, Config.DatabaseKey.USERNAME)

    def get_db_password(self):
        return self._get_string(Config.Section.DATABASE, Config.DatabaseKey.PASSWORD)

    def get_db_schemas(self):
        return self._get_string(Config.Section.DATABASE, Config.DatabaseKey.SCHEMAS)

    def to_str(self):
        return [(k.value, self._get_string(Config.Section.DATABASE, k)) for k in Config.DatabaseKey]

    # NOTE: This currently points to the datacube "DEV" server
    # Override by providing your own config file - for e.g. in $HOME/.datacube/.config

    _DEFAULT_CONFIG = """
[DATABASE]
host: 130.56.244.225
port: 6432
database: hypercube_v0
username: cube_user
password: GAcube0
schemas: agdc, public, gis, topology
"""


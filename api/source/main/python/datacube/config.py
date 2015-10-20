#===============================================================================
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

    def __init__(self, path="$HOME/.datacube/config"):

        def get_path(p):
            return p and os.path.expandvars(p) or None

        self._config = ConfigParser.SafeConfigParser()

        # Read central default config and/or specified config
        self._config.read([get_path(os.environ["AGDC_API_CONFIG"]),
                           get_path(path)])

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

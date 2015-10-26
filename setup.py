#!/usr/bin/env python

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
#===============================================================================

from setuptools import setup

version = '1.2.1b'

setup(name='agdc',
      version = version,
      packages = [
          'agdc',
          'agdc.ingest',
          'agdc.ingest.modis',
          'agdc.ingest.landsat',
          'agdc.ingest.wofs',
      ],
      package_data = {
                      'agdc': ['agdc_default.conf']
                      },
      scripts=[
          'bin/stacker.sh',
          'bin/bulk_submit_interactive.sh',
          'bin/bulk_submit_pbs.sh'
      ],
      entry_points={
          'console_scripts': [
              'agdc-ingest-landsat = agdc.ingest.landsat:cli',
              'agdc-ingest-modis = agdc.ingest.modis:cli',
              'agdc-ingest-wofs = agdc.ingest.wofs:cli'
          ]
      },
      install_requires=[
                  'eotools == 0.4',
                  'psycopg2 >= 2.5',
                  'gdal',
                  'numexpr',
                  'pyephem',
                  'numpy >= 1.9',
                  'scipy',
                  'python-dateutil',
                  'pytz'
      ],
      url = 'https://github.com/GeoscienceAustralia/ga-datacube',
      author = 'Alex Ip, Matthew Hoyles, Matthew Hardy',
      maintainer = 'Alex Ip, Geoscience Australia',
      maintainer_email = 'alex.ip@ga.gov.au',
      description = 'Australian Geoscience Data Cube (AGDC)',
      long_description = 'Australian Geoscience Data Cube (AGDC). Original Python code developed during the Unlocking the Landsat Archive. (ULA) Project, 2013',
      license = 'Apache License 2.0'
 )

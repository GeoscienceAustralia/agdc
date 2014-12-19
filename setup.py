#!/usr/bin/env python

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
#===============================================================================

from distutils.core import setup

version = '1.0.0'

setup(name='agdc',
      version = version,
      package_dir = {
                     'agdc' : 'src'
                     },
      packages = [
                  'agdc',
                  'agdc.abstract_ingester',
                  'agdc.landsat_ingester'
                  ],
      package_data = {
                      'agdc': ['agdc_default.conf']
                      },
      scripts = ['bin/stacker.sh',
                 'bin/landsat_ingester.sh',
                 'bin/bulk_submit_interactive.sh',
                 'bin/bulk_submit_pbs.sh'
                 ],
      requires = [
                  'EOtools',
                  'psycopg2',
                  'gdal',
                  'numexpr',
                  'scipy',
                  'pytz'
                  ],
      url = 'https://github.com/GeoscienceAustralia/ga-datacube',
      author = 'Alex Ip, Matthew Hoyles, Matthew Hardy',
      maintainer = 'Alex Ip, Geoscience Australia',
      maintainer_email = 'alex.ip@ga.gov.au',
      description = 'Australian Geoscience Data Cube (AGDC)',
      long_description = 'Australian Geoscience Data Cube (AGDC). Original Python code developed during the Unlocking the Landsat Archive. (ULA) Project, 2013',
      license = 'BSD 3'
     )

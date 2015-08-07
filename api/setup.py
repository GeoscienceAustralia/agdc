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


from setuptools import setup


setup(name="agdc-api",
      version="0.1.0-b20150807",
      package_dir={"": "source/main/python", "test": "source/test/python"},
      packages=["datacube", "datacube.api", "datacube.api.tool", "datacube.api.workflow"],
      scripts=[
          # Tools
          "source/main/python/datacube/api/tool/retrieve_aoi_time_series.py",
          "source/main/python/datacube/api/tool/retrieve_dataset.py",
          "source/main/python/datacube/api/tool/retrieve_dataset_stack.py",
          "source/main/python/datacube/api/tool/retrieve_pixel_time_series.py",
          "source/main/python/datacube/api/tool/band_statistics_arg25_validator.py",

          # Workflows
          "source/main/python/datacube/api/workflow/band_stack.py",
          "source/main/python/datacube/api/workflow/band_stack_arg25.py",
          "source/main/python/datacube/api/workflow/band_statistics_arg25.py",
          "source/main/python/datacube/api/workflow/band_statistics_arg25.pbs.sh",
          "source/main/python/datacube/api/workflow/band_statistics_arg25.pbs.submit.sh",
      ],
      author="Geoscience Australia",
      maintainer="Geoscience Australia",
      description="AGDC API",
      license="BSD 3",
      install_requires=[
          "gdal",
          "numpy >= 1.9",
          "scipy",
          "eotools",
          "psycopg2 >= 2.5",
          "enum34",
          "psutil",
          "python-dateutil"
      ]
)

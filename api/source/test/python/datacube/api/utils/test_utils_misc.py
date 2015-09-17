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
import pytest
from datacube.api import DatasetType
from datacube.api.utils import NDV, NAN, INT16_MIN, INT16_MAX, BYTE_MIN, BYTE_MAX, UINT16_MIN, UINT16_MAX
from datacube.api.utils import is_ndv, get_dataset_type_ndv, get_dataset_type_datatype
from gdalconst import GDT_Int16, GDT_Byte, GDT_Float32

_log = logging.getLogger()


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


@pytest.mark.quick
def test_is_ndv():

    assert is_ndv(NDV, NDV)
    
    assert is_ndv(NAN, NAN)
    
    assert is_ndv(INT16_MIN, INT16_MIN)
    assert is_ndv(INT16_MAX, INT16_MAX)
    
    assert is_ndv(UINT16_MIN, UINT16_MIN)
    assert is_ndv(UINT16_MAX, UINT16_MAX)
    
    assert is_ndv(BYTE_MIN, BYTE_MIN)
    assert is_ndv(BYTE_MAX, BYTE_MAX)

    assert not is_ndv(100, NDV)
    
    assert not is_ndv(100, NAN)
    
    assert not is_ndv(100, INT16_MIN)
    assert not is_ndv(100, INT16_MAX)
    
    assert not is_ndv(100, UINT16_MIN)
    assert not is_ndv(100, UINT16_MAX)
    
    assert not is_ndv(100, BYTE_MIN)
    assert not is_ndv(100, BYTE_MAX)

    
@pytest.mark.quick
def test_get_ndv():

    assert is_ndv(get_dataset_type_ndv(DatasetType.ARG25), NDV)
    assert is_ndv(get_dataset_type_ndv(DatasetType.PQ25), UINT16_MAX)
    assert is_ndv(get_dataset_type_ndv(DatasetType.FC25), NDV)
    assert is_ndv(get_dataset_type_ndv(DatasetType.WATER), BYTE_MAX)
    assert is_ndv(get_dataset_type_ndv(DatasetType.NDVI), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.EVI), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.NBR), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.TCI), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.DSM), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.DEM), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.DEM_HYDROLOGICALLY_ENFORCED), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.DEM_SMOOTHED), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.NDWI), NAN)
    assert is_ndv(get_dataset_type_ndv(DatasetType.MNDWI), NAN)


@pytest.mark.quick
def test_get_dataset_data_type():

    assert is_ndv(get_dataset_type_datatype(DatasetType.ARG25), GDT_Int16)
    assert is_ndv(get_dataset_type_datatype(DatasetType.PQ25), GDT_Int16)
    assert is_ndv(get_dataset_type_datatype(DatasetType.FC25), GDT_Int16)
    assert is_ndv(get_dataset_type_datatype(DatasetType.WATER), GDT_Byte)
    assert is_ndv(get_dataset_type_datatype(DatasetType.NDVI), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.EVI), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.NBR), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.TCI), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.DSM), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.DEM), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.DEM_HYDROLOGICALLY_ENFORCED), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.DEM_SMOOTHED), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.NDWI), GDT_Float32)
    assert is_ndv(get_dataset_type_datatype(DatasetType.MNDWI), GDT_Float32)

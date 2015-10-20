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

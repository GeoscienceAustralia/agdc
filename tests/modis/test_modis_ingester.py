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

"""
    test_modis_ingester.py - test class for modis ingester.

    There are 3 modes of operation:

    Mode 0 . Run all tests (1,2,3)
    Mode 1 . Checks Database Metadata against Alex/Jeremy Database creation scripts for MODIS.
            1.1. Check tile_type metadata
            1.2. Check satellite metadata
            1.3. Check sensor metadata
            1.4. Check processing_level metadata
            1.5. Check band metadata
            1.6. Check band_source metadata
            1.7. Check band_lookup_scheme metadata
            1.8. Check band_equivalent metadata
    Mode 2 . Compare ingested data between a test database and the reference base database
            2.1. Test dataset table.
            2.2. Test acquisition table.
            2.3. Test tile table.
            2.4. Test tile_footprint table.
            2.5. Test flattened table.
    Mode 3 . Compare tiles and directory structure between the test tile directory and the reference base tile directory.
            3.1. compares directory structure
                3.1.1. checks files names are identical
                3.1.2. checks directory structure are identical
            3.2. compare tiles files
                3.2.1. Compare file format
                3.2.2. Compare Geo Transform
                3.2.3. Compare Projection
                3.2.4. Compare Number of bands
                3.2.5. Compare number of pixels in the Height dimension
                3.2.6. Compare number of pixels in the Width dimension
                3.2.7. Compare metadata
                3.2.8. Compare bands
                    3.2.8.1. Compare Data Type
                    3.2.8.2. Compare Block Size
                    3.2.8.3. Compare Offset
                    3.2.8.4. Compare Scale
                    3.2.8.5. Compare Mask Flags
                    3.2.8.6. Compare Mask band
                        3.2.8.6.1. Compare Data
                    3.2.8.7. Compare Data.

    Example execution:

    Mode 0:
        python test_modis_ingester.py --test 0 --base_db_string "host='130.56.244.224' port='6432' dbname='hypercube_modis_test_base' user='cube_user' password='GAcube0'" --test_db_string "host='130.56.244.224' port='6432' dbname='hypercube_modis_test' user='cube_user' password='GAcube0'" --base_tile_directory "/g/data/u39/public/data/modis/datacube/mod09-tiled-base" --test_tile_directory "/g/data/u39/public/data/modis/datacube/mod09-tiled-test"

    Mode 1:
        python test_modis_ingester.py --test 1 --test_db_string "host='130.56.244.224' port='6432' dbname='hypercube_modis_test' user='cube_user' password='GAcube0'"

    Mode 2:
        python test_modis_ingester.py --test 2 --base_db_string "host='130.56.244.224' port='6432' dbname='hypercube_modis_test_base' user='cube_user' password='GAcube0'" --test_db_string "host='130.56.244.224' port='6432' dbname='hypercube_modis_test' user='cube_user' password='GAcube0'"

    Mode 3:
        python test_modis_ingester.py --test 3 --base_tile_directory "/g/data/u39/public/data/modis/datacube/mod09-tiled-base" --test_tile_directory "/g/data/u39/public/data/modis/datacube/mod09-tiled-test"

"""

import os
import sys
import logging
import re
import filecmp
from osgeo import gdal
import psycopg2
import sys
import pprint
import argparse
from time import sleep

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class ModisIngesterTest(object):

    def __init__(self):
        self.tiles_processed = 0

    def setTileDirectory(self, base_tile_directory, test_tile_directory):
        self.base_tile_directory = base_tile_directory
        self.test_tile_directory = test_tile_directory

    def setBaseDBString(self, base_database_connection_string):
        self.base_database_connection_string = base_database_connection_string

    def setTestDBString(self, test_database_connection_string):
        self.test_database_connection_string = test_database_connection_string

    def test_directory_structure(self, dir_base, dir_test):
        """compares dir_base and dir_test dictory structure"""

        dirs_cmp = filecmp.dircmp(dir_base, dir_test)
        if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or len(dirs_cmp.funny_files) > 0:
            if len(dirs_cmp.left_only) == 0 and len(dirs_cmp.right_only) == 1 and dirs_cmp.right_only[0] == 'ingest_temp':
                return True
            if len(dirs_cmp.right_only) == 0 and len(dirs_cmp.left_only) == 1 and dirs_cmp.left_only[0] == 'ingest_temp':
                return True
            if len(dirs_cmp.left_only) == 1 and len(dirs_cmp.right_only) == 1 and dirs_cmp.left_only[0] == 'ingest_temp' and dirs_cmp.right_only[0] == 'ingest_temp':
                return True
            return False
        (match, mismatch, errors) =  filecmp.cmpfiles(dir_base, dir_test, dirs_cmp.common_files, shallow=False)
        if len(mismatch) > 0 or len(errors) > 0:
            print mismatch
            return False

        for common_dir in dirs_cmp.common_dirs:
            path_base = os.path.join(dir_base, common_dir)
            path_test = os.path.join(dir_test, common_dir)
            if not self.test_directory_structure(path_base, path_test):
                return False
        return True

    def count_tiles(self, dir_base, dir_test):
        """count tiles common to both dir_base and dir_test dictory"""

        dirs_cmp = filecmp.dircmp(dir_base, dir_test)
        sum = len(dirs_cmp.common_dirs)
        if sum == 0:
            return len(dirs_cmp.common_files)
        else:
            sum = 0
            for common_dir in dirs_cmp.common_dirs:
                path_base = os.path.join(dir_base, common_dir)
                path_test = os.path.join(dir_test, common_dir)
                sum += self.count_tiles(path_base, path_test)
            return sum

    def test_tiles(self, dir_base, dir_test, num_tiles):
        """compares tiles from dir_base and dir_test dictory"""

        self.progress_bar('3.2 Testing tiles ...........................', self.tiles_processed, num_tiles)
        dirs_cmp = filecmp.dircmp(dir_base, dir_test)
        for common_files in dirs_cmp.common_files:
            path_base = os.path.join(dir_base, common_files)
            path_test = os.path.join(dir_test, common_files)
            if not self.test_tile(path_base, path_test):
                return False
            self.tiles_processed += 1
            self.progress_bar('3.2 Testing tiles ...........................', self.tiles_processed, num_tiles)
        for common_dir in dirs_cmp.common_dirs:
            path_base = os.path.join(dir_base, common_dir)
            path_test = os.path.join(dir_test, common_dir)
            if not self.test_tiles(path_base, path_test, num_tiles):
                return False
        return True

    def test_tile(self, tile_base, tile_test):
        """compares test_base tile and tile_test tile"""

        base_file = gdal.Open(tile_base, gdal.GA_ReadOnly)
        test_file = gdal.Open(tile_test, gdal.GA_ReadOnly)

        if base_file.GetDriver().ShortName != test_file.GetDriver().ShortName:
            return False
        if base_file.GetGeoTransform() != test_file.GetGeoTransform():
            return False
        if base_file.GetProjection() != test_file.GetProjection():
            return False
        if base_file.RasterCount != test_file.RasterCount:
            return False
        if base_file.RasterXSize != test_file.RasterXSize:
            return False
        if base_file.RasterYSize != test_file.RasterYSize:
            return False
        if base_file.GetMetadata() != test_file.GetMetadata():
            return False

        for i in range(1, base_file.RasterCount+1):
            base_band = base_file.GetRasterBand(i);
            test_band = test_file.GetRasterBand(i);
            if base_band.DataType != test_band.DataType:
                return False
            if base_band.GetNoDataValue() != test_band.GetNoDataValue():
                return False
            if base_band.GetBlockSize() != test_band.GetBlockSize():
                return False
            if base_band.GetOffset() != test_band.GetOffset():
                return False
            if base_band.GetScale() != test_band.GetScale():
                return False
            if base_band.GetMaskFlags() != test_band.GetMaskFlags():
                return False
            if base_band.GetMaskBand().Checksum() != test_band.GetMaskBand().Checksum():
                return False
            if base_band.Checksum() != test_band.Checksum():
                return False

        return True

    def test_dataset_table(self, base_database_connection_string, test_database_connection_string):
        """compares dataset table from both databases"""
     
        conn_base = psycopg2.connect(base_database_connection_string)
        conn_test = psycopg2.connect(test_database_connection_string)
        cursor_base = conn_base.cursor()
        cursor_test = conn_test.cursor()
        
        cursor_base.execute("SELECT dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text FROM dataset WHERE level_id = 20 OR level_id = 22 ORDER BY level_id, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text")
        cursor_test.execute("SELECT dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text FROM dataset WHERE level_id = 20 OR level_id = 22 ORDER BY level_id, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text")
     
        records_base = cursor_base.fetchall()
        records_test = cursor_test.fetchall()

        if records_base != records_test:
            return False

        return True

    def test_acquisition_table(self, base_database_connection_string, test_database_connection_string):
        """compares acquisition table from both databases"""

        conn_base = psycopg2.connect(base_database_connection_string)
        conn_test = psycopg2.connect(test_database_connection_string)
        cursor_base = conn_base.cursor()
        cursor_test = conn_test.cursor()
        
        cursor_base.execute("SELECT satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover FROM acquisition WHERE satellite_id = 10 AND sensor_id = 10 ORDER BY satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover")
        cursor_test.execute("SELECT satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover FROM acquisition WHERE satellite_id = 10 AND sensor_id = 10 ORDER BY satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover")
     
        records_base = cursor_base.fetchall()
        records_test = cursor_test.fetchall()

        if records_base != records_test:
            return False

        return True

    def test_tile_table(self, base_database_connection_string, test_database_connection_string):
        """compares tile table from both databases"""

        conn_base = psycopg2.connect(base_database_connection_string)
        conn_test = psycopg2.connect(test_database_connection_string)
        cursor_base = conn_base.cursor()
        cursor_test = conn_test.cursor()
        
        cursor_base.execute("SELECT x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status FROM tile WHERE tile_type_id = 3 and tile_class_id = 1 ORDER BY x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status")
        cursor_test.execute("SELECT x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status FROM tile WHERE tile_type_id = 3 and tile_class_id = 1 ORDER BY x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status")
     
        records_base = cursor_base.fetchall()
        records_test = cursor_test.fetchall()
        
        if records_base != records_test:
            return False

        return True

    def test_tile_footprint_table(self, base_database_connection_string, test_database_connection_string):
        """compares tile_footprint table from both databases"""

        conn_base = psycopg2.connect(base_database_connection_string)
        conn_test = psycopg2.connect(test_database_connection_string)
        cursor_base = conn_base.cursor()
        cursor_test = conn_test.cursor()
        
        cursor_base.execute("SELECT x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox FROM tile_footprint WHERE tile_type_id = 3 ORDER BY x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max")
        cursor_test.execute("SELECT x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox FROM tile_footprint WHERE tile_type_id = 3 ORDER BY x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max")
     
        records_base = cursor_base.fetchall()
        records_test = cursor_test.fetchall()
        
        if records_base != records_test:
            return False

        return True

    def test_flattened_tables(self, base_database_connection_string, test_database_connection_string):
        """compares flattened tables from both databases"""

        conn_base = psycopg2.connect(base_database_connection_string)
        conn_test = psycopg2.connect(test_database_connection_string)
        cursor_base = conn_base.cursor()
        cursor_test = conn_test.cursor()
        
        cursor_base.execute("SELECT dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text, satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover, x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status, x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox FROM dataset full join acquisition using(acquisition_id) full join tile using(dataset_id) full join tile_footprint using(x_index, y_index, tile_type_id) ORDER BY dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text, satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover, x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status, x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox")
        cursor_test.execute("SELECT dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text, satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover, x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status, x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox FROM dataset full join acquisition using(acquisition_id) full join tile using(dataset_id) full join tile_footprint using(x_index, y_index, tile_type_id) ORDER BY dataset_path, level_id, dataset_size, datetime_processed, crs, ll_x, ll_y, lr_x, lr_y, ul_x, ul_y, ur_x, ur_y, x_pixels, y_pixels, xml_text, satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime, ll_lon, ll_lat, lr_lon, lr_lat, ul_lon, ul_lat, ur_lon, ur_lat, gcp_count, mtl_text, cloud_cover, x_index, y_index, tile_type_id, tile_class_id, tile_size, tile_status, x_index, y_index, tile_type_id, x_min, y_min, x_max, y_max, bbox")
     
        records_base = cursor_base.fetchall()
        records_test = cursor_test.fetchall()
        
        if records_base != records_test:
            return False

        return True

    def test_tile_type_metadata(self, test_database_connection_string):
        """checks tile_type medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM tile_type WHERE tile_type_id = 2 AND tile_type_name = 'Unprojected WGS84 10-degree at 400 pixels/degree (for MODIS 250m)' AND crs = 'EPSG:4326' AND x_origin = 0 AND y_origin = 0 AND x_size = 10 AND y_size = 10 AND x_pixels = 4000 AND y_pixels = 4000 AND unit = 'degree' AND file_format = 'GTiff' AND file_extension = '.tif' AND tile_directory = 'MODIS' AND format_options = 'COMPRESS=LZW'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM tile_type WHERE tile_type_id = 3 AND tile_type_name = 'Unprojected WGS84 10-degree at 200 pixels/degree (for MODIS 500m)' AND crs = 'EPSG:4326' AND x_origin = 0 AND y_origin = 0 AND x_size = 10 AND y_size = 10 AND x_pixels = 2000 AND y_pixels = 2000 AND unit = 'degree' AND file_format = 'GTiff' AND file_extension = '.tif' AND tile_directory = 'MODIS' AND format_options = 'COMPRESS=LZW'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM tile_type WHERE tile_type_id = 4 AND tile_type_name = 'Unprojected WGS84 10-degree at 100 pixels/degree (for MODIS 1km)' AND crs = 'EPSG:4326' AND x_origin = 0 AND y_origin = 0 AND x_size = 10 AND y_size = 10 AND x_pixels = 1000 AND y_pixels = 1000 AND unit = 'degree' AND file_format = 'GTiff' AND file_extension = '.tif' AND tile_directory = 'MODIS' AND format_options = 'COMPRESS=LZW'")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 3:
                return True
            return False
        except:
            return False

    def test_satellite_metadata(self, test_database_connection_string):
        """checks satellite medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM satellite WHERE satellite_id = 10 AND satellite_name = 'Terra' AND satellite_tag = 'MT' AND semi_major_axis = 7077700 AND altitude = 70500 AND inclination = 1.71389761553675")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 1:
                return True
            return False
        except:
            return False

    def test_sensor_metadata(self, test_database_connection_string):
        """checks sensor medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM sensor WHERE sensor_id = 10 AND satellite_id = 10 AND sensor_name = 'MODIS-Terra' AND description = 'MODIS-Terra Sensor'")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 1:
                return True
            return False
        except:
            return False

    def test_band_type_metadata(self, test_database_connection_string):
        """checks band_type medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM band_type WHERE band_type_id = 1 AND band_type_name = 'reflective'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_type WHERE band_type_id = 5 AND band_type_name = 'emitted'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_type WHERE band_type_id = 31 AND band_type_name = 'RBQ'")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 3:
                return True
            return False
        except:
            return False
    def test_processing_level_metadata(self, test_database_connection_string):
        """checks processing_level medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM processing_level WHERE level_id = 20 AND level_name = 'MOD09' AND nodata_value = -28672 AND resampling_method = 'near' AND level_description = 'MODIS MOD09'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM processing_level WHERE level_id = 21 AND level_name = 'RBQ250' AND nodata_value = 3 AND resampling_method = 'near' AND level_description = '250m Reflectance Band Quality'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM processing_level WHERE level_id = 22 AND level_name = 'RBQ500' AND nodata_value = 3 AND resampling_method = 'near' AND level_description = '500m Reflectance Band Quality'")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM processing_level WHERE level_id = 23 AND level_name = 'RBQ1000' AND nodata_value = 3 AND resampling_method = 'near' AND level_description = '1km Reflectance Band Quality'")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 4:
                return True
            return False
        except:
            return False

    def test_band_metadata(self, test_database_connection_string):
        """checks band medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM band WHERE band_id = 200 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 1' AND band_type_id = 1 AND file_number = 201 AND resolution = 500 AND min_wavelength = 0.62 AND max_wavelength = 0.67 AND satellite_id = 10 AND band_tag = 'MB1' AND band_number = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 201 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 2' AND band_type_id = 1 AND file_number = 202 AND resolution = 500 AND min_wavelength = 0.841 AND max_wavelength = 0.876 AND satellite_id = 10 AND band_tag = 'MB2' AND band_number = 2")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 202 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 3' AND band_type_id = 1 AND file_number = 203 AND resolution = 500 AND min_wavelength = 0.459 AND max_wavelength = 0.479 AND satellite_id = 10 AND band_tag = 'MB3' AND band_number = 3")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 203 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 4' AND band_type_id = 1 AND file_number = 204 AND resolution = 500 AND min_wavelength = 0.545 AND max_wavelength = 0.565 AND satellite_id = 10 AND band_tag = 'MB4' AND band_number = 4")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 204 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 5' AND band_type_id = 1 AND file_number = 205 AND resolution = 500 AND min_wavelength = 1.23 AND max_wavelength = 1.25 AND satellite_id = 10 AND band_tag = 'MB5' AND band_number = 5")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 205 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 6' AND band_type_id = 1 AND file_number = 206 AND resolution = 500 AND min_wavelength = 1.628 AND max_wavelength = 1.652 AND satellite_id = 10 AND band_tag = 'MB6' AND band_number = 6")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 206 AND sensor_id = 10 AND band_name = 'Modis Surface Reflectance Band 7' AND band_type_id = 1 AND file_number = 207 AND resolution = 500 AND min_wavelength = 2.105 AND max_wavelength = 2.155 AND satellite_id = 10 AND band_tag = 'MB7' AND band_number = 7")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band WHERE band_id = 250 AND sensor_id = 10 AND band_name = '500m Reflectance Band Quality' AND band_type_id = 31 AND file_number = 10 AND resolution = 500 AND satellite_id = 10 AND band_tag = 'RBQ500' AND band_number = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 8:
                return True
            return False
        except:
            return False

    def test_band_source_metadata(self, test_database_connection_string):
        """checks band_source medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 200 AND level_id = 20 AND tile_layer = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 201 AND level_id = 20 AND tile_layer = 2")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 202 AND level_id = 20 AND tile_layer = 3")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 203 AND level_id = 20 AND tile_layer = 4")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 204 AND level_id = 20 AND tile_layer = 5")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 205 AND level_id = 20 AND tile_layer = 6")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 206 AND level_id = 20 AND tile_layer = 7")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_source WHERE tile_type_id = 3 AND band_id = 250 AND level_id = 22 AND tile_layer = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 8:
                return True
            return False
        except:
            return False

    def test_band_lookup_scheme_metadata(self, test_database_connection_string):
        """checks band_lookup_scheme medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM band_lookup_scheme WHERE lookup_scheme_id = 10 AND lookup_scheme_name = 'MODIS' AND lookup_scheme_description = 'MODIS bands'")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 1:
                return True
            return False
        except:
            return False

    def test_band_equivalent_metadata(self, test_database_connection_string):
        """checks band_equivalent medatadata in test databases"""

        try:
            conn_test = psycopg2.connect(test_database_connection_string)
            cursor_test = conn_test.cursor()
            
            count = 0
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB1' AND master_band_tag = 'MB1' AND nominal_centre = 0.645 AND nominal_bandwidth = 0.05 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB2' AND master_band_tag = 'MB2' AND nominal_centre = 0.8585 AND nominal_bandwidth = 0.035 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB3' AND master_band_tag = 'MB3' AND nominal_centre = 0.469 AND nominal_bandwidth = 0.02 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB4' AND master_band_tag = 'MB4' AND nominal_centre = 0.555 AND nominal_bandwidth = 0.02 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB5' AND master_band_tag = 'MB5' AND nominal_centre = 1.24 AND nominal_bandwidth = 0.02 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB6' AND master_band_tag = 'MB6' AND nominal_centre = 1.64 AND nominal_bandwidth = 0.024 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1
            cursor_test.execute("SELECT * FROM band_equivalent WHERE lookup_scheme_id = 10 AND master_band_name = 'MB7' AND master_band_tag = 'MB7' AND nominal_centre = 2.13 AND nominal_bandwidth = 0.05 AND centre_tolerance = 0.00025 AND bandwidth_tolerance = 0.00025 AND band_type_id = 1")
            if len(cursor_test.fetchall()) == 1:
                count += 1

            if count == 7:
                return True
            return False
        except:
            return False


    def progress_bar(self, line, ticks, max_ticks):
        """console progress bar"""
        
        per_cent = int(ticks/float(max_ticks)*20)
        sys.stdout.write('\r')
        sys.stdout.write("%s[%-20s] %d/%d %d%% " % (line, '='*per_cent, ticks, max_ticks, ticks*100/max_ticks))
        sys.stdout.flush()

    def run_test_1(self):
        """runs test 1"""

        result = []

        print ""
        print "================================"
        print "= 1. Testing Database Metadata ="
        print "================================"
        print ""
        
        print "1.1 Testing type_type metadata ..............",
        result.append(self.test_tile_type_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.2 Testing satellite metadata ..............",
        result.append(self.test_satellite_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"
        
        print "1.3 Testing sensor metadata .................",
        result.append(self.test_sensor_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.4 Testing band_type metadata ..............",
        result.append(self.test_band_type_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.5 Testing processing_level metadata .......",
        result.append(self.test_processing_level_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.6 Testing band metadata ...................",
        result.append(self.test_band_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.7 Testing band_source metadata ............",
        result.append(self.test_band_source_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.8 Testing band_lookup_scheme metadata .....",
        result.append(self.test_band_lookup_scheme_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "1.9 Testing band_equivalent metadata ........",
        result.append(self.test_band_equivalent_metadata(self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        sys.stdout.flush()
        
        if not False in result:
            print ""
            print "========================"
            print "= 1. All Tests Passsed ="
            print "========================"
            print ""            
            return True
        else:
            return False
            print ""
            print "========================"
            print "= 1. Some Tests Failed ="
            print "========================"
            print ""

    def run_test_2(self):
        """runs test 2"""

        result = []

        print ""
        print "======================="
        print "= 2. Testing Database ="
        print "======================="
        print ""

        print "2.1 Testing dataset table ...................",
        sys.stdout.flush()
        result.append(self.test_dataset_table(self.base_database_connection_string, self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "2.2 Testing acquisition table ...............",
        sys.stdout.flush()
        result.append(self.test_acquisition_table(self.base_database_connection_string, self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "2.3 Testing tile table ......................",
        sys.stdout.flush()
        result.append(self.test_tile_table(self.base_database_connection_string, self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "2.4 Testing tile_footprint table ............",
        sys.stdout.flush()
        result.append(self.test_tile_footprint_table(self.base_database_connection_string, self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "2.5 Testing flattened tables ................",
        sys.stdout.flush()
        result.append(self.test_flattened_tables(self.base_database_connection_string, self.test_database_connection_string))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        sys.stdout.flush()

        if not False in result:
            print ""
            print "========================"
            print "= 2. All Tests Passsed ="
            print "========================"
            print ""            
            return True
        else:
            return False
            print ""
            print "========================"
            print "= 2. Some Tests Failed ="
            print "========================"
            print ""

    def run_test_3(self):
        """runs test 3"""

        result = []

        print ""
        print "========================="
        print "= 3. Testing Tile Files ="
        print "========================="
        print ""

        print "3.1 Testing directory structure .............",
        sys.stdout.flush()
        result.append(self.test_directory_structure(self.base_tile_directory, self.test_tile_directory))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        print "3.2 Testing tiles ...........................",
        sys.stdout.flush()
        num_tiles = self.count_tiles(self.base_tile_directory, self.test_tile_directory)
        result.append(self.test_tiles(self.base_tile_directory, self.test_tile_directory, num_tiles))
        if result[-1]:
            print "Pass"
        else:
            print "Fail"

        sys.stdout.flush()

        if not False in result:
            print ""
            print "========================"
            print "= 3. All Tests Passsed ="
            print "========================"
            print ""            
            return True
        else:
            return False
            print ""
            print "========================"
            print "= 3. Some Tests Failed ="
            print "========================"
            print ""

    def run_all_tests(self):
        """runs all tests"""
        
        result = []

        result.append(self.run_test_1())
        result.append(self.run_test_2())
        result.append(self.run_test_3())

        if not False in result:
            print ""
            print "====================="
            print "= All Tests Passsed ="
            print "====================="
            print ""
        else:
            print ""
            print "====================="
            print "= Some Tests Failed ="
            print "====================="
            print ""

        sys.stdout.flush()

def parse_args():
    """Parse and return command line arguments using argparse."""

    argparser = argparse.ArgumentParser(description='Modis Test Runner.')
    argparser.add_argument('-t', '--test', help='test type (0 = all, 1 = test database medatadata, 2 = test database data, 3 = test tiles.', required = True)
    argparser.add_argument('-bdb', '--base_db_string', help='base database connection string.')
    argparser.add_argument('-tdb', '--test_db_string', help='test database connection string.')
    argparser.add_argument('-btd', '--base_tile_directory', help='base_tile_directory path.')
    argparser.add_argument('-ttd', '--test_tile_directory', help='test_tile_directory path.')
    return argparser.parse_args()

#
# Main program
#

if __name__ == '__main__':

    args = parse_args()
    if args.test == "0":
        if (args.base_tile_directory and args.test_tile_directory and args.base_db_string and args.test_db_string):
            modisIngesterTest = ModisIngesterTest()
            modisIngesterTest.setTileDirectory(args.base_tile_directory, args.test_tile_directory)
            modisIngesterTest.setBaseDBString(args.base_db_string)
            modisIngesterTest.setTestDBString(args.test_db_string)
            modisIngesterTest.run_test_1()
            modisIngesterTest.run_test_2()
            modisIngesterTest.run_test_3()
        else:
            print "Missing base_tile_directory or test_tile_directory or base_db_string or test_db_string parameter(s)"
    if args.test == "1":
        if (args.test_db_string):
            modisIngesterTest = ModisIngesterTest()
            modisIngesterTest.setTestDBString(args.test_db_string)
            modisIngesterTest.run_test_1()
        else:
            print "Missing test_db_string parameter"
    elif args.test == "2":
        if (args.base_db_string and args.test_db_string):
            modisIngesterTest = ModisIngesterTest()
            modisIngesterTest.setBaseDBString(args.base_db_string)
            modisIngesterTest.setTestDBString(args.test_db_string)
            modisIngesterTest.run_test_2()
        else:
            print "Missing base_db_string or test_db_string parameter(s)"            
    elif args.test == "3":
        if (args.base_tile_directory and args.test_tile_directory):
            modisIngesterTest = ModisIngesterTest()
            modisIngesterTest.setTileDirectory(args.base_tile_directory, args.test_tile_directory)
            modisIngesterTest.run_test_3()
        else:
            print "Missing base_tile_directory or test_tile_directory parameter(s)"

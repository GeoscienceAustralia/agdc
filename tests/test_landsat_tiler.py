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

"""Tests for the dbupdater.py module."""

import sys
import os
import subprocess
import unittest
from agdc import dbutil
from agdc import dbcompare
from osgeo import gdal
import numpy
import re

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
# Call this program from the ga-datacube source directory.
# There are three call methods
# 0. python test_landsat_tiler.py 0
# This will put all results into the expected directory.
# 1. python test_landsat_tiler.py 1
# Put all results into the output directory, and compare resulting database,
# with expected database, stopping if databases disagree. If databases agree,
# will proceed to calculating valid pixel counts and difference histograms.
# 2. python test_landsat_tiler.py 2
# As for 1. but continue to histogram stage regardless of database differences.

class TestLandsatTiler(unittest.TestCase):
    """Unit tests for the dbupdater.py script"""
    def process_args(self):
        MODULE = 'new_ingest_benchmark'
        SUITE = 'benchmark'

        self.INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
        self.OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
        self.EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)
        #define three modes of execution
        mode_desc_dict = {0: 'Initialise benchmark data in the expected directory',
                          1: 'Do not do ingestion. Compare existing ingestion ' \
                          'in\n %s\n with benchmark\n %s\n' \
                          %(self.OUTPUT_DIR, self.EXPECTED_DIR),
                          2: 'Compare  from this run with ' \
                          'expected benchmark database exiting if ' \
                          'they are different',
                          3: 'Compare databases and also compare tiles, even if ' \
                          'the databases are different'}
        if len(sys.argv) < 2:
            mode = -1
        else:
            try:
                mode = int(sys.argv[1])
            except ValueError:
                mode = -1
        msg = ''
        if mode not in [0, 1, 2, 3]:
            msg =  'Please specify a mode as follows:\n'
            for mode_num, desc in mode_desc_dict.items():
                msg = msg + 'python test_landsat_tiler.py %d:\t%s\n' %(mode_num, desc)
        return mode, msg

    def setUp(self):
        self.PQA_CONTIGUITY_BIT = 8
        self.test_dbname = None
        self.expected_dbname = None

        self.test_conn = None
        self.expected_conn = None

        self.logfile = None
        self.bands_expected = None
        self.bands_output = None
        self.mode, self.msg = self.process_args()
        if self.mode == 0:
            self.OUTPUT_DIR = self.EXPECTED_DIR
        print 'OUTPUT_DIR =%s' %self.OUTPUT_DIR
        print self.mode
        print self.msg

    def test_landsat_tiler(self):
        """Test the cataloging and tiling of Landsat scences and compare
        resulting database and tile contents with an ingestion benchmark"""
        # This test is intended as an example, and so is extensively
        # commented.
        # Open a log file
        if self.mode not in [0, 1, 2, 3]:
            self.skipTest('Skipping test_landsat_tiler since flag is not in [0, 1, 2, 3]')
        logfile_path = os.path.join(self.OUTPUT_DIR, "test_landsat_tiler.log")
        self.logfile = open(logfile_path, "w")

        #
        # Create the initial database
        #

        # Randomise the name to avoid collisions with other users.
        self.test_dbname = dbutil.random_name("test_tiler")

        # Create the database.
        print 'About to create dbase from %s' \
            %(os.path.join(self.INPUT_DIR, "hypercube_empty.sql"))
        if self.mode != 1:
            dbutil.TESTSERVER.create(self.test_dbname,
                                     self.INPUT_DIR, "hypercube_empty.sql")

        #
        # Run dbupdater on the test database and save the result
        #
        # Create updated datacube_conf file with the new dbname and tile_root
        tile_root = os.path.join(self.OUTPUT_DIR, "tiles")
        configuration_dict = {'dbname': self.test_dbname,
                              'tile_root': tile_root}
        config_file_path = dbutil.update_config_file2(configuration_dict,
                                                     self.INPUT_DIR,
                                                     self.OUTPUT_DIR,
                                                     "test_datacube.conf")
        # Run dbupdater

        ingest_dir = os.path.join(self.INPUT_DIR, 'tiler_testing')
        dbupdater_cmd = ["python",
                         "dbupdater.py",
                         "--debug",
                         "--config=%s" % config_file_path,
                         "--source=%s" % ingest_dir,
                         "--removedblist",
                         "--followsymlinks"]
        if self.mode != 1:
            subprocess.check_call(dbupdater_cmd, stdout=self.logfile,
                                  stderr=subprocess.STDOUT)

        # Run landsat_tiler
        landsat_tiler_cmd = ["python",
                             "landsat_tiler.py",
                             "--config=%s" % config_file_path]
        if self.mode != 1:
            subprocess.check_call(landsat_tiler_cmd, stdout=self.logfile,
                                  stderr=subprocess.STDOUT)
        # Save the updated database
        if self.mode != 1:
            dbutil.TESTSERVER.save(self.test_dbname, self.OUTPUT_DIR,
                                   "tiler_testing.sql")
        #
        # If an expected result exists then load it and compare
        #
        # Check for expected result
        if self.mode > 0 and os.path.isfile(os.path.join(self.EXPECTED_DIR,
                                                         "tiler_testing.sql")):
            print 'starting to check differences'
            #MPHtemp create the output database
            if self.mode == 1:
                self.test_dbname = dbutil.random_name("tiler_testing")
                dbutil.TESTSERVER.create(self.test_dbname,
                                         self.OUTPUT_DIR, "tiler_testing.sql")
            #END MPHtemp

            # Create a randomised name...
            self.expected_dbname = dbutil.random_name("expected_tiler_testing")

            # load the database...
            dbutil.TESTSERVER.create(self.expected_dbname,
                                     self.EXPECTED_DIR, "tiler_testing.sql")
            # create database connections...
            self.test_conn = dbutil.TESTSERVER.connect(self.test_dbname)
            self.expected_conn = \
                dbutil.TESTSERVER.connect(self.expected_dbname)

            # and compare.
            dbases_agree = dbcompare.compare_databases(self.test_conn,
                                               self.expected_conn,
                                               output=self.logfile,
                                               verbosity=3)

            if self.mode == 2:
                #Compare databases and fail test if they differ
                assert dbases_agree, "Databases do not match."

            #Compare data within corresponding files of the EXPECTED_DIR and
            #OUTPUT_DIR. Get list of tile pathnames from EXPECTED and OUTPUT
            #databases' repsective tile tables. There is an assumption here
            #that, within each of the expected and output databases,
            #the tile basename uniquely defines both the tile_type_id and the
            #full tile pathname. However, if the tile type table has put ORTHO
            #bands to be of a new tile_type, then
            #corresponding tiles in expected and output may be of different
            #tile_type. So we need to have self.bands_expected and
            #self.bands_output
            expected_tile_dict, output_tile_dict = \
                self.get_tile_pathnames(self.expected_conn, self.test_conn)
            tiles_expected = set(expected_tile_dict.keys())
            tiles_output = set(output_tile_dict.keys())
            tiles_expected_or_output = tiles_expected | tiles_output

            #Construct band source table as per datacube module
            self.bands_expected = \
                self.construct_bands_source_dict(self.expected_conn)
            self.bands_output =\
                self.construct_bands_source_dict(self.test_conn)
            #file_pattern to parse file name for information
            file_pattern = [r'(?P<sat>\w+)_(?P<sensor>\w+)_',
                            r'(?P<processing_level>\w+)_',
                            r'(?P<xindex>-*\d+)_(?P<yindex>-*\d+)_'
                            r'(?P<year>\d+)-(?P<month>\d+)-'
                            r'(?P<day>\d+)T(?P<hour>\d+)-(?P<minute>\d+)-',
                            r'(?P<second_whole>\d+)\.(?P<second_fraction>\d+)'
                            r'\.(?P<file_extension>.+)']
            pattern = re.compile(''.join(file_pattern))
            #Set up dictionary of pixel counts to be accumulated per
            #(procesing_level, tile_layer) over all tiles
            #0: total_pixel_count_expected
            #1: total_pixel_count_output
            #2: total_pixel_count_both
            #3: total_pixel_count_expected_not_output
            #4: total_pixel_count_output_not_expected
            pixel_count_dict = {}
            #Set up nested dicts of differece counts
            difference_count_dict = {}

            #For each tile in EXPECTED_DIR and OUTPUT_DIR, get pixel counts and
            #difference histograms
            #There are five dictionaries involved:
            ### tile_name_dict             {'sat': LS5, 'sensor': TM, ...}
            ### bands_dict_expected:       those bands from self.bands_expected
            ###                            corresponding to current tile's
            ###                            tile_type_id and (satellite, sensor)
            ### bands_dict_output:         output database's correspondent to
            ###                            bands_dict_expected
            ### level_dict_expected:       those bands from bands_dict_expected
            ###                            for  which the processing level
            ###                            matches that for the current tile
            ### level_dict_output:         output database's correspondent to
            ###                            level_dict_expected
            ### all_levels_info_dict       [level_dict_expected,
            ####                           level_dict_output]
            ###                            for each processing level

            all_levels_info_dict = {}
            for tile_name in tiles_expected_or_output:
                print 'processing tile %s' %tile_name
                tile_type_id_expected = None
                tile_type_id_output = None
                fname_expected = None
                fname_output = None
                #If tile is in either database, extract tile_type and pathname
                if tile_name in tiles_expected:
                    tile_type_id_expected, fname_expected = \
                        expected_tile_dict[tile_name]
                if tile_name in tiles_output:
                    tile_type_id_output, fname_output = \
                        output_tile_dict[tile_name]
                #Extract information from the tile name and select
                #nested dictionary for this tile from bands table,
                #given the (sat, sensor) [or("DERIVED', 'PQA') for PQA],
                #which will be common to expected and output tiles, and the
                #tile_type_id, which may be different for expected and output
                matchobj = re.match(pattern, tile_name)
                tile_name_dict = matchobj.groupdict()
                full_key_expected = \
                    self.get_tiletype_sat_sens_level(tile_type_id_expected,
                                                tile_name_dict)
                full_key_output = \
                    self.get_tiletype_sat_sens_level(tile_type_id_output,
                                                tile_name_dict)
                #Following will raise assertion error if a tile's
                #tile_type_id has changed since benchmark ingestion
                full_key = self.check_equal_or_null(full_key_expected,
                                               full_key_output)
                level_dict_expected = {}
                level_dict_output = {}
                #full_key is (tile_type, sat, sensor, processing_level)
                if full_key in all_levels_info_dict:
                    (level_dict_expected, level_dict_output) = \
                        all_levels_info_dict[full_key]
                if level_dict_expected == {} and full_key_expected != None:
                    level_dict_expected = \
                        self.collect_source_bands(self.bands_expected,
                                                  full_key)
                if level_dict_output == {} and full_key_output != None:
                    level_dict_output = \
                        self.collect_source_bands(self.bands_output,
                                                  full_key)
                if full_key not in all_levels_info_dict:
                    all_levels_info_dict[full_key] = [level_dict_expected,
                                                       level_dict_output]
                if all_levels_info_dict[full_key][0] == {} and \
                        level_dict_expected != {}:
                    all_levels_info_dict[full_key][0] = level_dict_expected
                if all_levels_info_dict[full_key][1] == {} and \
                        level_dict_output != {}:
                    all_levels_info_dict[full_key][1] = level_dict_output

                #Check that the number of bands is as expected, adding
                #singleton dimension if only one band
                ([data_expected, data_output], number_layers) = \
                    self.load_and_check(fname_expected, fname_output,
                                        level_dict_expected,
                                        level_dict_output)
                assert bool(fname_expected) == (data_expected != None) and \
                    bool(fname_output) == (data_output != None), \
                    "data array should exist if and only if fname exists"

                for ilayer in range(number_layers):
                    #Define expected and output band data
                    band_expected, dtype_expected = \
                        self.get_band_data(data_expected, ilayer)
                    band_output, dtype_output = \
                        self.get_band_data(data_output, ilayer)
                    assert (band_expected == None) == (dtype_expected == None)\
                    and (band_output == None) == (dtype_output == None), \
                        "band data should exist if and only if dtype exists"
                    dtype_this = self.check_equal_or_null(dtype_expected,
                                                          dtype_output)
                    #calculate the number of bins required to store the
                    #histogram of differences from this datatype
                    if tile_name_dict['processing_level'] == 'PQA':
                        #possible difference values are 0 through 16,
                        #(number of tests which differ)
                        bin_count = 16 + 1
                    else:
                        #possible difference vals are min through max of dtype
                        bin_count = numpy.iinfo(dtype_this).max - \
                            numpy.iinfo(dtype_this).min + 1
                        assert bin_count < 66000, "datatype is more than 16" \
                            "bits, need to add code to coarsen the" \
                            "histogram bins or use apriori  max and" \
                            "min values of the data"
                    #The histograms are per (level, layer).
                    #Could have one histogram per (sat, sensor, level, layer)
                    #and then, depending on verbosity, aggregate during report.
                    #But for now, just key by (level, layer).
                    result_key = (full_key[3], ilayer + 1)
                    if result_key not in pixel_count_dict:
                        pixel_count_dict[result_key] = numpy.zeros(shape=(5),
                                                            dtype=numpy.uint64)
                        difference_count_dict[result_key] = \
                            numpy.zeros(shape=(bin_count), dtype=numpy.uint64)

                    pixel_count = pixel_count_dict[result_key]
                    difference_count = difference_count_dict[result_key]
                    if tile_name_dict['processing_level'] == 'PQA':
                        if band_expected is None:
                            band_expected = 0
                        if band_output is None:
                            band_output = 0
                        #define index as those pixels with contiguity bit set
                        index_expected = \
                            numpy.bitwise_and(band_expected,
                                              1 << self.PQA_CONTIGUITY_BIT) > 0
                        index_output = \
                            numpy.bitwise_and(band_output,
                                              1 << self.PQA_CONTIGUITY_BIT) > 0
                    else:
                        #For NBAR and ORTHO use nodata_value
                        nodata_value = \
                            level_dict_output[ilayer + 1]['nodata_value']
                        if band_expected is  None:
                            band_expected = nodata_value
                        if band_output is None:
                            band_output = nodata_value
                        index_expected = band_expected != nodata_value
                        index_output = band_output != nodata_value
                    pixel_count[0] += numpy.count_nonzero(index_expected)
                    pixel_count[1] += numpy.count_nonzero(index_output)
                    pixel_count[2] += \
                        numpy.count_nonzero(numpy.logical_and(index_expected,
                                                              index_output))
                    pixel_count[3] += \
                        numpy.count_nonzero(numpy.logical_and
                                            (index_expected, ~index_output))
                    pixel_count[4] += \
                        numpy.count_nonzero(numpy.logical_and
                                            (~index_expected, index_output))
                    #Only want to calculate differences at common pixels
                    index_both = numpy.logical_and(index_expected,
                                                   index_output)
                    if numpy.count_nonzero(index_both) == 0:
                        continue
                    valid_data_expected = band_expected[index_both].ravel()
                    valid_data_output = band_output[index_both].ravel()
                    #Calculate difference histogram and add to running total
                    if tile_name_dict['processing_level'] == 'PQA':
                        difference = \
                            self.count_bitwise_diffs(valid_data_expected,
                                                     valid_data_output)
                    else:
                        difference = abs(valid_data_output.astype(numpy.int64)
                                     - valid_data_expected.astype(numpy.int64))
                    hist, dummy_bin_edges = \
                        numpy.histogram(difference,
                                        numpy.array(range(bin_count + 1),
                                                    dtype=numpy.uint64))
                    difference_count += hist
                    #dereference band data
                    band_expected = None
                    band_output = None
                    difference = None
                    #end of layer loop
                #dereference tile data

                data_expected = None
                data_output = None

            #Output
            #for sat_sen, band_dict in all_bands_dict:
            fp = open(os.path.join(self.OUTPUT_DIR,
                                   'Histogram_output.txt'), 'w')
            fp.writelines('##### COMPARISON OF TILED DATA IN FOLLOWING '\
                              'DIRECTORES\n%s\n%s\n' %(self.EXPECTED_DIR,
                                                       self.OUTPUT_DIR))
            result_keys_processed = []
            for full_key in all_levels_info_dict.keys():
                dummy, dummy, dummy, processing_level = full_key
                top_layer_result_key = (processing_level, 1)
                if top_layer_result_key in result_keys_processed:
                    continue
                fp.writelines('#### Processing Level: %s\n' %processing_level)
                level_dict_expected, level_dict_output = \
                    all_levels_info_dict[full_key]
                assert set(level_dict_expected.keys()) == \
                       set(level_dict_output.keys()), "different key sets"
                number_layers = len(level_dict_output.keys())
                for this_layer in range(1, number_layers + 1):
                    result_key = (processing_level, this_layer)
                    result_keys_processed.append(result_key)
                    fp.writelines('### tile_layer = %d\n' %this_layer)
                    for key, val in level_dict_expected[this_layer].items():
                        if key == 'tile_layer' or key == 'level_name':
                            continue
                        outline = '# %s = %s' %(key, val)
                        if str(level_dict_output[this_layer][key]) != str(val):
                            outline = '%s (%s in output database)' \
                            %(outline, level_dict_output[this_layer][key])
                        fp.writelines('%s\n' %outline)
                    #get key for pixel_count_dict and difference_count_dict
                    #Print counts of pixels with valid data
                    fp.writelines('#Valid data counts\n')
                    pixel_count = pixel_count_dict[result_key]
                    count_desc = ['Expected\t', 'Output\t\t', 'Common\t\t',
                                  'Missing\t\t', 'Extra\t\t']
                    for desc, num in zip(count_desc, pixel_count):
                        fp.writelines('\t\t%s%d\n' %(desc, num))
                    #Print histograms of differences in valid data
                    fp.writelines('#Histogram of differences in valid data\n')
                    difference_count = difference_count_dict[result_key]
                    index_nonzero_bins = difference_count > 0
                    for bin_no in range(len(difference_count)):
                        if index_nonzero_bins[bin_no]:
                            fp.writelines('\t\tDifference of %d: %d\n'
                                          %(bin_no, difference_count[bin_no]))
            fp.close()
        else:
            if self.mode > 0:
                self.skipTest("Expected database save file not found.")
    def tearDown(self):
        # Remove any tempoary databases that have been created.
        if self.test_conn:
            self.test_conn.close()
        if self.expected_conn:
            self.expected_conn.close()

        if self.test_dbname:
            dbutil.TESTSERVER.drop(self.test_dbname)
        if self.expected_dbname:
            dbutil.TESTSERVER.drop(self.expected_dbname)

        # Close the logfile
        if self.logfile:
            self.logfile.close()

    ###########################################################################
    ### Utility methods
    @staticmethod
    def get_tile_pathnames(expected_conn, output_conn):
        """From two different databases, get the tile pathnames from the tile
           table. Return each as a dictionary of
           {basename: (tile_type_id, full path)}"""
        sql = """-- Retrieve list of tile.tile_pathname from each database
                     select tile_type_id, tile_pathname from tile where
                     tile_class_id = 1
              """
        db_cursor = expected_conn.cursor()
        db_cursor.execute(sql)
        expected_dict = {}
        for record in db_cursor:
            expected_dict[os.path.basename(record[1])] = (record[0], record[1])
        db_cursor = output_conn.cursor()
        db_cursor.execute(sql)
        output_dict = {}
        for record in db_cursor:
            output_dict[os.path.basename(record[1])] = (record[0], record[1])
        return (expected_dict, output_dict)

    @staticmethod
    def construct_bands_source_dict(conn):
        """Construct the self.bands object just as in datacube class"""
        sql = """-- Retrieve all band information (including derived bands)
                    select tile_type_id,
                    coalesce(satellite_tag, 'DERIVED') as satellite_tag,
                    coalesce(sensor_name, level_name) as sensor_name,
                    band_id,
                    sensor_id,
                    band_name,
                    band_type_name, 
                    file_number,
                    resolution,
                    min_wavelength,
                    max_wavelength,
                    file_pattern,
                    level_name,
                    tile_layer,
                    band_tag,
                    resampling_method,
                    nodata_value

                    from band ba
                    inner join band_type bt using(band_type_id)
                    inner join band_source bs using (band_id)
                    inner join processing_level pl using(level_id)
                    left join sensor se using(satellite_id, sensor_id)
                    left join satellite sa using(satellite_id)
                    order by tile_type_id,satellite_name, sensor_name,
                    level_name, tile_layer
        """
        db_cursor = conn.cursor()
        db_cursor.execute(sql)
        datacube_bands_dict = {}
        for record in db_cursor:
            # self.bands is keyed by tile_type_id
            band_dict = datacube_bands_dict.get(record[0], {})
            if not band_dict: # New dict needed
                datacube_bands_dict[record[0]] = band_dict

            # sensor_dict is keyed by (satellite_tag, sensor_name)
            sensor_dict = band_dict.get((record[1], record[2]), {})
            if not sensor_dict: # New dict needed
                band_dict[(record[1], record[2])] = sensor_dict
            band_info = {}
            band_info['band_id'] = record[3]
            band_info['band_name'] = record[5]
            band_info['band_type'] = record[6]
            band_info['file_number'] = record[7]
            band_info['resolution'] = record[8]
            band_info['min_wavelength'] = record[9]
            band_info['max_wavelength'] = record[10]
            band_info['file_pattern'] = record[11]
            band_info['level_name'] = record[12]
            band_info['tile_layer'] = record[13]
            band_info['band_tag'] = record[14]
            band_info['resampling_method'] = record[15]
            band_info['nodata_value'] = record[16]

            sensor_dict[record[7]] = band_info
        return datacube_bands_dict

    @staticmethod
    def get_tiletype_sat_sens_level(tile_type, tile_name_dict):
        """return a tuple of (tile_type, sat, sensor, processing_level)"""
        if tile_type == None:
            return None
        sat = tile_name_dict['sat']
        sensor = tile_name_dict['sensor']
        processing_level = tile_name_dict['processing_level']
        if sensor == 'ETM':
            sensor = 'ETM+'
        if processing_level in ['PQA', 'FC']:
            sat = 'DERIVED'
            sensor = processing_level
        return (tile_type, sat, sensor, processing_level)

    @staticmethod
    def collect_source_bands(datacube_bands_dict, full_key):
        """Given the dictionary parsed from a tile basename, return appropriate
        nested dictionary from self.bands dictionary"""
        if full_key == None:
            return {}
        tile_type, sat, sensor, processing_level = full_key
        sat_sensor_dict = datacube_bands_dict[tile_type][(sat, sensor)]
        level_bands_dict = {} #keyed by tile_layer
        for band in sat_sensor_dict.keys():
            if sat_sensor_dict[band]['level_name'] == processing_level:
                key = sat_sensor_dict[band]['tile_layer']
                assert key not in level_bands_dict, "multiple instances of" \
                    "tile layer %d for %s"  %(key, processing_level)
                level_bands_dict[key] = sat_sensor_dict[band]
        return level_bands_dict

    @staticmethod
    def load_and_check(fname1, fname2, dict1, dict2):
        """Check that two tiles agree on their dimensions and crs, and return
        the data arrays in a list"""
        file_info_list = [(fname1, dict1), (fname2, dict2)]
        dimensions = []
        geotrans = []
        projections = []
        data_arrays = []
        gdal.UseExceptions()
        for fname, band_info_dict in file_info_list:
            if fname:
                dset = gdal.Open(fname)
                data = dset.ReadAsArray()
                if len(data.shape) == 2:
                    data = data[None, :]
                dims = data.shape
                assert dims[0] == len(band_info_dict), \
                    "Number of layers read from %s (%d)" \
                    "does not match the number of entries in self.bands (%d)" \
                    %(fname, dims[0], len(band_info_dict))
                dimensions.append(dims)
                geotrans.append(dset.GetGeoTransform())
                projections.append(dset.GetProjection())
                data_arrays.append(data)
                dset = None
                data = None
            if not fname:
                data_arrays.append(None)
        #Check both tiles agree on metadata
        if len(dimensions) == 2:
            assert dimensions[0] == dimensions[1], "Number of dimensions" \
                "disagree for %s and %s" %(fname1, fname2)
            assert len(geotrans) == 2, "Unexpected number of geotransforms"
            assert geotrans[0] == geotrans[1], "geotransforms" \
                "disagree for %s and %s" %(fname1, fname2)
            assert len(projections) == 2, "Unexpected number of projections"
            assert projections[0] == projections[1], "projections disagree" \
                "for %s and %s\n" %(fname1, fname2)
        assert len(data_arrays) == 2, "calls to load_and_check assume that" \
            "two arrays will be returned, with possibility of one being None"
        return (data_arrays, dimensions[0][0])

    @staticmethod
    def get_band_data(data_array, ilayer):
        """From the three-dimensional array extract the appropriate layer and
        also the data type. If data array does not exist then set to a
        singleton nodata value"""
        assert data_array == None or len(data_array.shape) == 3, \
            "get_band_data assumes 3D array"
        band_data, datatype = (None, None)
        if data_array != None:
            band_data = data_array[ilayer, :, :]
            datatype = band_data.dtype
        return (band_data, datatype)

    @staticmethod
    def count_bitwise_diffs(arr1, arr2):
        """Given two flattened arrays, return a same-sized array containing the
        number of bitwise differences"""
        assert arr1.shape == arr2.shape and len(arr1.shape) == 1, \
            "Inconsistent arrays in count_bitwise_diffs"
        assert arr1.dtype == numpy.uint16, "Need uint16 in count_bitwise_diffs"
        assert arr2.dtype == numpy.uint16, "Need uint16 in count_bitwise_diffs"
        diff = numpy.bitwise_xor(arr1, arr2)
        difference_as_bytes = numpy.ndarray(shape=(diff.shape[0], 2),
                                            dtype=numpy.uint8, buffer=diff)
        difference_as_bits = numpy.unpackbits(difference_as_bytes, axis=1)
        difference = numpy.sum(difference_as_bits, axis=1, dtype=numpy.uint8)
        return difference

    @staticmethod
    def check_equal_or_null(a, b):
        """Checks that two objects are the same"""
        ab_list = list((set([a]) | set([b])) - set([None]))
        assert len(ab_list) <= 1, "check_equal_or_null:\n" + a + " and " +  b
        if len(ab_list) == 1:
            return ab_list[0]
        else:
            return None

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestLandsatTiler]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())

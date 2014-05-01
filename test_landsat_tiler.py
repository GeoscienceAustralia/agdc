#!/usr/bin/env python

"""Tests for the dbupdater.py module."""

import sys
import os
import subprocess
import unittest
import dbutil
import dbcompare
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
# This will put all results into the output directory, and compare resulting database,
# with expected database, stopping if the databases disagree. If databases agree,
# will proceed to calculating valid pixel counts and difference histograms.
# 2. python test_landsat_tiler.py 2
# As for 1. but will continue to histogram stage regardless of any database differences.

class TestLandsatTiler(unittest.TestCase):
    """Unit tests for the dbupdater.py script"""

    MODULE = 'landsat_tiler'
    SUITE = 'new_then_old'
    PQA_CONTIGUITY_BIT_INDEX = 8

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)
    #define three modes of execution
    mode_desc_dict={0: 'Initialise the benchmark data in the expected directory',
                   1: 'Compare the output database from this run with the expected benchmark database',
                   2: 'Compare databases and also compare tiles, even if the databases are different'}

    try:
        mode = int(sys.argv[1])
    except:
        mode = -1
    if mode not in [0, 1, 2]:    
        print 'Please specify a mode as follows:'
        for mode, desc in mode_desc_dict.items():
            print 'python test_landsat_tiler.py %d:\t%s\n' %(mode, desc)
        sys.exit()
        
    print 'mode=', mode
    if mode == 0:
        OUTPUT_DIR = EXPECTED_DIR
    
    print 'OUTPUT_DIR =%s' %OUTPUT_DIR
    default_tile_type_id = 1


    def setUp(self):
        self.test_dbname = None
        self.expected_dbname = None

        self.test_conn = None
        self.expected_conn = None

        self.logfile = None

        print 'OUTPUT_DIR =%s' %self.OUTPUT_DIR

    def test_landsat_tiler(self):

        # This test is intended as an example, and so is extensively
        # commented.
        # Open a log file
        logfile_path = os.path.join(self.OUTPUT_DIR, "test_landsat_tiler.log")
        self.logfile = open(logfile_path, "w")

        #
        # Create the initial database
        #

        # Randomise the name to avoid collisions with other users.
        self.test_dbname = dbutil.random_name("test_tiler")

        # Create the database.
        print 'About to create dbase from %s' %(os.path.join(self.INPUT_DIR, "hypercube_empty.sql"))
        dbutil.TESTSERVER.create(self.test_dbname,
                                 self.INPUT_DIR, "hypercube_empty.sql")

        #
        # Run dbupdater on the test database and save the result
        #

        # Create an updated datacube_conf file with the new dbname and a tile output directory
        tile_root = os.path.join(self.OUTPUT_DIR, "tiles")
        configuration_dict = {'dbname': self.test_dbname, 'tile_root': tile_root}
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
        subprocess.check_call(dbupdater_cmd, stdout=self.logfile,
                              stderr=subprocess.STDOUT)

        # Run landsat_tiler
        landsat_tiler_cmd = ["python",
                             "../old_landsat_tiler.py",
                             "--config=%s" % config_file_path]
        subprocess.check_call(landsat_tiler_cmd, stdout=self.logfile,
                              stderr=subprocess.STDOUT)
        #About to call landsat_tiler'
        #LSTiler = landsat_tiler.LandsatTiler()
        #print 'Finished calling landsat_tiler'
        #...
        #...
        #...

        # Save the updated database
        dbutil.TESTSERVER.save(self.test_dbname, self.OUTPUT_DIR,
                               "tiler_testing.sql")
        #return
        #
        # If an expected result exists then load it and compare
        #
        # Check for expected result
        if self.mode > 0 and os.path.isfile(os.path.join(self.EXPECTED_DIR, "tiler_testing.sql")):
            print 'starting to check differences'
            #MPH START temporary code just to compare
            #self.test_dbname = dbutil.random_name("test_tiler")
            #dbutil.TESTSERVER.create(self.test_dbname,
            #                         self.OUTPUT_DIR, "tiler_testing.sql")
            #MPH END temporary code just to compare
            # Create a randomised name...
            self.expected_dbname = dbutil.random_name("expected_tiler_testing")

            # load the database...
            dbutil.TESTSERVER.create(self.expected_dbname,
                                     self.EXPECTED_DIR, "tiler_testing.sql")
          
            # create database connections...
            
            self.test_conn = dbutil.TESTSERVER.connect(self.test_dbname)
            self.expected_conn = dbutil.TESTSERVER.connect(self.expected_dbname)

            # and compare.

            if self.mode == 1:
                #Compare databases and fail test if they differ
                self.assertTrue(dbcompare.compare_databases(self.test_conn,
                                                            self.expected_conn,
                                                            output=self.logfile,
                                                            verbosity=3),
                                "Databases do not match.")
            else:
                #Compare databases and, regardless of result, continue to comparing tiles
                dbcompare.compare_databases(self.test_conn,
                                            self.expected_conn,
                                            output=self.logfile,
                                            verbosity=3)

            #Compare data within corresponding files of the EXPECTED_DIR and OUTPUT_DIR
            #Get list of tile pathnames from EXPECTED and OUTPUT databases' repsective tile tables
            sql = """-- Retrieve list of tile.tile_pathname from each database
select tile_pathname from tile where tile_class_id = 1
"""
            db_cursor = self.expected_conn.cursor()
            db_cursor.execute(sql)
            expected_tile_pathnames_dict = {}
            for record in db_cursor:
                expected_tile_pathnames_dict[os.path.basename(record[0])] = record[0]
            db_cursor = self.test_conn.cursor()
            db_cursor.execute(sql)
            output_tile_pathnames_dict = {}
            for record in db_cursor:
                output_tile_pathnames_dict[os.path.basename(record[0])] = record[0]
            #Construct band source table as per datacube module
            tiles_expected = set(expected_tile_pathnames_dict.keys())
            tiles_output = set(output_tile_pathnames_dict.keys())
            #tiles_expected_not_output = tiles_expected - tiles_output
            #tiles_output_not_expected = tiles_output - tiles_expected
            tiles_expected_and_output = tiles_expected & tiles_output
            tiles_expected_or_output = tiles_expected | tiles_output
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
order by tile_type_id,satellite_name, sensor_name, level_name, tile_layer
""" 
            db_cursor = self.test_conn.cursor()
            db_cursor.execute(sql)
            self.bands = {}
            for record in db_cursor:
                # self.bands is keyed by tile_type_id
                band_dict = self.bands.get(record[0], {})
                if not band_dict: # New dict needed              
                    self.bands[record[0]] = band_dict 
                
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
            
                sensor_dict[record[7]] = band_info # file_number - must be unique for a given satellite/sensor or derived level

            all_bands_dict = self.bands[self.default_tile_type_id]
            #file_pattern to parse file name for information
            file_pattern = ['(?P<sat>\w+)_(?P<sensor>\w+)_(?P<processing_level>\w+)_',
                            '(?P<xindex>-*\d+)_(?P<yindex>-*\d+)_'
                            '(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)T(?P<hour>\d+)-(?P<minute>\d+)-',
                            '(?P<second_whole>\d+)\.(?P<second_fraction>\d+)\.(?P<file_extension>.+)']
            pattern = re.compile(r''.join(file_pattern))
            #Set up dictionary of pixel counts to be accumulated per (sat, sensor, processing_level, tile_layer) over all tiles
            #0: total_pixel_count_expected
            #1: total_pixel_count_output
            #2: total_pixel_count_both
            #3: total_pixel_count_expected_not_output
            #4: total_pixel_count_output_not_expected
            empty_pixel_counts = numpy.zeros(shape=(5),dtype=numpy.uint64)
            pixel_count_dict = {}
            #Set up nested dicts of differece counts
            difference_count_dict = {}

            #For each tile in EXPECTED_DIR and OUTPUT_DIR, get pixel counts and difference histograms
            #There are five dictionaries involved:
            ### tile_name_dict        {'sat': LS5, 'sensor': TM, 'x_index': 120, 'y_index': -30, ... } from parsing tile name
            ### all_bands_dict:       corresponding to tile_type_id == 1 (this will need to be expanded to include
            ###                       other tile types corresponding to different resolutions)
            ### bands_dict:           those bands from all_bands_dict corresponding to current tile's
            ###                       (satellite, sensor)
            ### tile_band_info_dict:  those bands from bands_dict for which the processing level matches
            ###                       that for the current tile
            ### all_levels_info_dict: nested tile_band_info_dict for each processing level, for reporting
            all_levels_info_dict = {}
            for tile_name in tiles_expected_or_output:                
                print 'processing tile %s' %tile_name
                fname_expected = None
                fname_output = None
                
                if tile_name in tiles_expected:
                    fname_expected = expected_tile_pathnames_dict[tile_name]
                if tile_name in tiles_output:
                    fname_output = output_tile_pathnames_dict[tile_name]
                #Extract the necessary information from the tile name and select the appropriate
                #nested dictionary for this tile from bands table
                matchobj = re.match(pattern, tile_name)
                tile_name_dict = matchobj.groupdict()
                if tile_name_dict['sensor'] == 'ETM':
                    tile_name_dict['sensor'] = 'ETM+'
                if tile_name_dict['processing_level'] in ['PQA']:
                    key = ('DERIVED', tile_name_dict['processing_level'])
                else:
                    key = (tile_name_dict['sat'], tile_name_dict['sensor'])
                bands_dict = all_bands_dict[key]

                #Collect the bands from the source table that will be contained in fname_output
                tile_band_info_dict = {} #will be keyed by tile_layer where the band is of the correct processing level

                for band in bands_dict.keys():
                    if bands_dict[band]['level_name'] == tile_name_dict['processing_level']:
                        #for this processing_level, tile_layer is unique
                        key = bands_dict[band]['tile_layer']
                        tile_band_info_dict[key] = bands_dict[band]
                if tile_name_dict['processing_level'] not in all_levels_info_dict:
                    all_levels_info_dict[tile_name_dict['processing_level']] = tile_band_info_dict
                #Check that the number of bands is as expected, adding singleton dimension if only one band
                #expected tile
                if fname_expected:
                    dset_expected = gdal.Open(fname_expected)
                    data_expected = dset_expected.ReadAsArray()
                    if len(data_expected.shape) == 2:
                        data_expected = data_expected[None, :]
                    dimensions_expected = data_expected.shape
                    number_layers = dimensions_expected[0]
                    assert number_layers == len(tile_band_info_dict),"Number of layers read from %s (%d)" \
                                                    "does not match the number of entries in self.bands (%d)" \
                                                    %(fname_expected, number_layers, len(tile_band_info_dict))
                #output tile
                if fname_output:
                    dset_output = gdal.Open(fname_output)
                    data_output = dset_output.ReadAsArray()
                    if len(data_output.shape) == 2:
                        data_output = data_output[None, :]
                    dimensions_output = data_output.shape
                    number_layers = dimensions_output[0]
                    assert number_layers == len(tile_band_info_dict),"Number of layers read from %s (%d)" \
                                                    "does not match the number of entries in self.bands (%d)" \
                                                    %(fname_output, number_layers, len(tile_band_info_dict))

                if fname_expected and fname_output:
                    #check dimensions
                    print fname_expected, fname_output
                    assert dimensions_expected[1] == dimensions_output[1] and dimensions_expected[2] == dimensions_output[2], \
                                                 "For tile %s Number of rows and columns do not agree" %(tile_name)
                    #check geotransforms
                    assert dset_expected.GetGeoTransform() == dset_output.GetGeoTransform(), \
                        "For tile %s, output and expected results have difference geotransforms" %(tile_name)
                    #check projections
                    assert dset_expected.GetProjection() == dset_output.GetProjection(), \
                        "For tile %s, output and expected results have difference projections" %(tile_name)

                for ilayer in range(number_layers):
                    #Define expected and output band data
                    if fname_expected:
                        band_expected = data_expected[ilayer, :, :]
                        dtype_expected = band_expected.dtype
                        dtype_this = dtype_expected
                    else:
                        if tile_band_info_dict[ilayer + 1]['level_name'] == 'PQA':
                            band_expected = 0 #any value with contiguous bit == 0 will do
                        else:
                            band_expected = tile_band_info_dict[ilayer + 1]['nodata_value']

                    if fname_output:
                        band_output = data_output[ilayer, :, :]
                        dtype_output = band_output.dtype
                        dtype_this = dtype_output
                    else:
                        if tile_band_info_dict[ilayer + 1]['level_name'] == 'PQA':
                            band_output = 0 #any value with contiguous bit == 0 will do
                        else:
                            band_output = tile_band_info_dict[ilayer + 1]['nodata_value']

                    if fname_expected and fname_output:
                        assert dtype_expected == dtype_output, 'Different data types for expected and output tile data' %(tile_name)
                    #calculate the number of bins required to store the historam of differences from this datatype
                    if tile_band_info_dict[ilayer + 1]['level_name'] == 'PQA':
                        #possible difference values are 0 through 16, (number of tests which differ)
                        bin_count = 16 + 1
                    else:
                        #possible difference values are min through max of the datatype
                        bin_count = numpy.iinfo(dtype_this).max - numpy.iinfo(dtype_this).min + 1
            
                    #The histograms are per (level, layer)
                    #We could have one histogram per (sat, sensor, level, layer), i.e
                    #key = (tile_name_dict['sat'], tile_name_dict['sensor'], tile_name_dict['processing_level'], '%d' %(ilayer + 1))
                    #and then, depending on verbosity, aggregate during reporting. But for now, just key by (level, layer).
                    key = (tile_name_dict['processing_level'], '%d' %(ilayer + 1))
                    if key not in pixel_count_dict:
                       pixel_count_dict[key] = numpy.zeros(shape=(5), dtype=numpy.uint64)
                       difference_count_dict[key] = numpy.zeros(shape=(bin_count), dtype=numpy.uint64)

                    pixel_count = pixel_count_dict[key]
                    difference_count = difference_count_dict[key]
                    if tile_band_info_dict[ilayer + 1]['level_name'] == 'PQA':
                        #define index as those pixels for which contiguity bit is nonzero
                        index_expected = numpy.bitwise_and(band_expected, 1 << self.PQA_CONTIGUITY_BIT_INDEX) > 0
                        index_output = numpy.bitwise_and(band_output, 1 << self.PQA_CONTIGUITY_BIT_INDEX) > 0
                    else:
                        #For NBAR and ORTHO use nodata_value
                        nodata_value = tile_band_info_dict[ilayer + 1]['nodata_value']
                        index_expected = band_expected != nodata_value
                        index_output = band_output != nodata_value
                    pixel_count[0] += numpy.count_nonzero(index_expected)
                    pixel_count[1] += numpy.count_nonzero(index_output)
                    pixel_count[2] += numpy.count_nonzero(numpy.logical_and(index_expected, index_output))
                    pixel_count[3] += numpy.count_nonzero(numpy.logical_and(index_expected, ~index_output))
                    pixel_count[4] += numpy.count_nonzero(numpy.logical_and(~index_expected, index_output))
                    
                    #Only want to calculate differences at those pixels for which data exists in expected and ouput
                    index_both = numpy.logical_and(index_expected, index_output)
                    if numpy.count_nonzero(index_both) == 0:
                        continue
                    valid_data_expected = band_expected[index_both].ravel()
                    valid_data_output = band_output[index_both].ravel()
                    #Calculate difference histogram and add to running total
                    if tile_band_info_dict[ilayer + 1]['level_name'] == 'PQA':
                        #Count the number of tests on which the two results differ
                        diff = numpy.bitwise_xor(valid_data_expected, valid_data_output)
                        difference_as_bytes = numpy.ndarray(shape=(diff.shape[0], 2), dtype=numpy.uint8, buffer=diff)
                        difference_as_bits = numpy.unpackbits(difference_as_bytes, axis=1)
                        difference = numpy.sum(difference_as_bits, axis=1, dtype=numpy.uint8)
                    else:
                        #for NBAR and ORTHO just use absolute difference
                        difference = abs(valid_data_output.astype(numpy.int64) - valid_data_expected.astype(numpy.int64))
                    hist, bin_edges = numpy.histogram(difference, numpy.array(range(bin_count + 1), dtype=numpy.uint64))
                    difference_count += hist
                    #dereference band data
                    band_expected = None
                    band_output = None
                    difference = None
                    #end of layer loop
                #dereference tile data
                dset_expected = None
                data_expected = None
                dset_output = None
                data_output = None



            #for sat_sen, band_dict in all_bands_dict:                
            fp = open(os.path.join(self.OUTPUT_DIR, 'Histogram_output.txt'), 'w')
            fp.writelines('##### COMPARISON OF TILED DATA IN FOLLOWING DIRECTORES\n%s\n%s\n' %(self.EXPECTED_DIR, self.OUTPUT_DIR))
            for processing_level in ['PQA', 'NBAR', 'ORTHO']:
                fp.writelines('#### Processing Level: %s\n' %processing_level)
                this_level_bands_dict = all_levels_info_dict[processing_level]
                for this_layer in this_level_bands_dict.keys():
                    fp.writelines('### tile_layer = %d\n' %this_layer)
                    for key, val in this_level_bands_dict[this_layer].items():
                        if key == 'tile_layer' or key == 'level_name':
                            continue
                        fp.writelines('# %s = %s\n' %(key, val))
                    #get key for pixel_count_dict and difference_count_dict
                    key = (this_level_bands_dict[this_layer]['level_name'], str(this_layer))
                    #Print counts of pixels with valid data
                    fp.writelines('#Valid data counts\n')
                    pixel_count = pixel_count_dict[key]
                    count_desc = ['Expected\t', 'Output\t\t', 'Common\t\t', 'Missing\t\t','Extra\t\t']
                    for desc, num in zip(count_desc, pixel_count):
                        fp.writelines('\t\t%s%d\n' %(desc, num))
                    #Print histograms of differences in valid data
                    fp.writelines('#Histogram of differences in valid data\n')
                    difference_count = difference_count_dict[key]
                    index_nonzero_bins = difference_count > 0
                    for bin_num in range(len(difference_count)):
                        if index_nonzero_bins[bin_num]:
                            fp.writelines('\t\tDifference of %d: %d\n' %(bin_num, difference_count[bin_num]))                                                                                        
            fp.close()
        else:
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

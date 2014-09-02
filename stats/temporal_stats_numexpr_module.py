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
#     * Neither [copyright holder] nor the names of its contributors may be
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

'''Author: Josh Sixsmith; joshua.sixsmith@ga.gov.au
'''

import os
import sys
import argparse
import numpy
from osgeo import gdal
from datetime import datetime
import numexpr

# adding my home path to get my 'get_tiles' module
sys.path.append('/home/547/jps547')

import get_tiles


'''
    Calculates statistics over the temporal/spectral/z domain of an array 
    containing [z,y,x] data.
    Statistics produced in this order are:
        Sum
        Valid Observations
        Mean
        Variance
        Standard Deviation
        Skewness
        Kurtosis
        Max
        Min
        Median (non-interpolated value)
        Median Index (zero based index)
        1st Quantile (non-interpolated value)
        3rd Quantile (non-interpolated value)
        Geometric Mean    
    if provenance=True then the following two bands are also created:
        Acquisition date/time for Median Observation (seconds since epoch)
        Satellite for Median Observation (5=LS5 7=LS7)

    Median, 1st and 3rd quantiles: For odd length arrays the middle value is
    taken. For even length arrays, the first of the two middle values is taken.
    Either way, no interpolation is used for calculation.
    The median index is the band that the median value came from.

    The numexpr module is used to handle most of the operations. Arrays are 
    processed faster while using less memory.

    This script can be called from the command line with internal tiling 
    handled automatically.
    eg 
    python temporal_stats_numexpr_module.py --infile /to/input/file/location/image_file --outfile /to/output/file/location/stats_output 

    For full command line options call:
    python temporal_stats_numexpr_module.py --help

    The general stats function can also be imported as module, eg:
    from temporal_stats_numexpr_module import temporal_stats

    No data values are also handled, either specified upon input when calling
    the temporal_stats function directly, or from the command line. It is not
    required from the command line, as the script will attempt to find the 
    no data value from the metadata.

'''

NaN     = numpy.float32(numpy.NaN)
    
def line_num_finder(array, string = "", offset=0):
    for i in (numpy.arange(len(array))+offset):
        if string in str(array[i]):
            return i

def main(infile, outfile, file_driver='ENVI', xtile=None, ytile=None, noData=None, as_bip=False, provenance=False):
    
    stats_band_count = 14
    
    if provenance:
        output_band_count   = stats_band_count + 2 # Allow for acquisition_datetime and satellite provenance bands
    else:
        output_band_count   = stats_band_count

    band_names = [
                  'Sum', 
                  'Mean', 
                  'Valid Observations', 
                  'Variance', 
                  'Stddev', 
                  'Skewness', 
                  'Kurtosis', 
                  'Max', 
                  'Min', 
                  'Median (non-interpolated)', 
                  'Index of Median Observation (zero based)', 
                  '1st Quantile (non-interpolated)', 
                  '3rd Quantile (non-interpolated)', 
                  'Geometric Mean',
                  'Date/time for Median Observation (seconds since epoch)',
                  'Satellite for Median Observation (5=LS5 7=LS7)'
                  ][:output_band_count] # Limit the list if last two bands are not required when provenance==False

    inDataset = gdal.Open(infile, gdal.gdalconst.GA_ReadOnly)
    assert inDataset

    samples = inDataset.RasterXSize
    lines   = inDataset.RasterYSize
    band    = inDataset.GetRasterBand(1)

    if (noData == None):
        noData = band.GetNoDataValue()

    band = None

    driver     = gdal.GetDriverByName(file_driver)
    outDataset = driver.Create(outfile, samples, lines, output_band_count, 6)
#    outDataset.SetMetadata(inDataset.GetMetadata())
    outDataset.SetGeoTransform(inDataset.GetGeoTransform())
    outDataset.SetProjection(inDataset.GetProjection())

    outband = []
    for i in range(stats_band_count):
        outband.append(outDataset.GetRasterBand(i+1))
        outband[i].SetNoDataValue(numpy.NaN)

    if (xtile == None):
        xtile = samples
    if (ytile == None):
        ytile = 100


    tiles = get_tiles.get_tile3(samples, lines, xtile, ytile)
    print 'number of tiles: ', len(tiles)

    # set zero point for time and make copies for each section needing times
    tz = datetime.now()
    tz = tz - tz
    tz_tile_all_steps = tz
    tz_tile_read = tz
    tz_tile_stats_process = tz
    tz_tile_write = tz

    for tile in tiles:

        # want total reading, writing, processing times, and averages.
        time_tile_start = datetime.now()

        ystart = int(tile[0])
        yend   = int(tile[1])
        xstart = int(tile[2])
        xend   = int(tile[3])

        xsize = int(xend - xstart)
        ysize = int(yend - ystart)

        time_tile_read_start = datetime.now()

        subset = (inDataset.ReadAsArray(xstart, ystart, xsize, ysize)).astype('float32')
        inDataset.FlushCache()

        time_tile_read_end = datetime.now()
        tz_tile_read += (time_tile_read_end - time_tile_read_start)

        time_tile_stats_process_start = datetime.now()

        # !!! Process the stats !!!
        # calculate the stats over the temporal/spectral domain
        t_stats = temporal_stats(subset, no_data=noData, as_bip=as_bip)

        time_tile_stats_process_end = datetime.now()
        tz_tile_stats_process += (time_tile_stats_process_end - time_tile_stats_process_start)


        time_tile_write_start = datetime.now()
        for i in range(stats_band_count):
            outband[i].WriteArray(t_stats[i], xstart, ystart)
            outband[i].FlushCache()

        time_tile_write_end = datetime.now()
        tz_tile_write += (time_tile_write_end - time_tile_write_start)


        time_tile_end = datetime.now()
        tz_tile_all_steps += (time_tile_end - time_tile_start)
        
    # Populate two provenance bands if required
    if provenance:
        time_provenance_start = datetime.now()
        get_pixel_provenance(inDataset, outDataset)
        time_provenance_end = datetime.now()
        tz_provenance = (time_provenance_end - time_provenance_start)
    
    # Set GDAL band descriptions
    for band_index in range(output_band_count):
        outDataset.GetRasterBand(band_index + 1).SetDescription(band_names[band_index])

    # Write all changes to file
    outDataset.FlushCache()
    outDataset = None

    # Create/repair ENVI header
    if (driver == 'ENVI'): 
        data_ignore_value = 'nan'
        create_envi_hdr(outfile, noData=data_ignore_value, new_bnames=band_names)       
        
    print 'total processing time: ', tz_tile_all_steps
    print 'avg time per tile: ', tz_tile_all_steps/len(tiles)
    print 'total stats processing time: ', tz_tile_stats_process
    print 'avg stats time per tile: ', tz_tile_stats_process/len(tiles)
    print 'total read time: ', tz_tile_read
    print 'avg read time per tile: ', tz_tile_read/len(tiles)
    print 'total write time: ', tz_tile_write
    print 'avg write time per tile: ', tz_tile_write/len(tiles)
    if provenance:
        print 'total time to determine pixel provenance: ', tz_provenance


def temporal_stats(array, no_data=None, as_bip=False):
    '''Calculates statistics over the temporal/spectral/z domain.

    Calculates statistics over the temporal/spectral/z domain of an array
    containing [z,y,x] data.
    Statistics produced in this order are:
    Sum
    Valid Observations
    Mean
    Variance
    Standard Deviation
    Skewness
    Kurtosis
    Max
    Min
    Median (non-interpolated value)
    Median Index (zero based index)
    1st Quantile (non-interpolated value)
    3rd Quantile (non-interpolated value)
    Geometric Mean

    Median, 1st and 3rd quantiles: For odd length arrays the middle value is
    taken. For even length arrays, the first of the two middle values is taken.
    Either way, no interpolation is used for calculation.
    The median index is the band that the median value came from.

    The numexpr module is used to handle most of the operations. Arrays are
    processed faster while using less memory.

    Args:
        array: A numpy array containing [z,y,x] data.
        no_data: The data value to ignore for calculations. Default is None.
        as_bip: If set to True, the array will be transposed to be 
            [Lines,Samples,Bands], and processed as such. The default is to
            process the array as [Bands,Lines,Samples], ie False.

    Returns:
        A numpy float32 array, with NaN representing no Data values.

    Author:
        Josh Sixsmith; joshua.sixsmith@ga.gov.au

    '''

    # assuming a 3D array, [bands,rows,cols]
    dims  = array.shape

    if len(dims) != 3:
        raise Exception('Error. Array dimensions must be 3D, not %i' %len(dims))

    # define what each dimension represents
    bands = dims[0]
    cols  = dims[2]
    rows  = dims[1]

    #number of bands/stats to be output to be returned
    output_band_count = 14

    NaN   = numpy.float32(numpy.NaN)

    if no_data:
        wh = numexpr.evaluate("array == no_data")
        array[wh] = NaN

    if as_bip:
        # a few transpositions will take place, but they are quick to create
        # and are mostly just views into the data, so no copies are made
        # !!!Hopefully!!!
        array_bip = numpy.transpose(array, (1,2,0))

        stats = numpy.zeros((output_band_count, rows, cols), dtype='float32')

        stats[0] = numpy.nansum(array_bip, axis=2) # sum
        vld_obsv = numpy.sum(numpy.isfinite(array_bip), axis=2) # we need an int
        stats[2] = vld_obsv
        stats[1] = numexpr.evaluate("fsum / fvld_obsv", {'fsum':stats[0], 'fvld_obsv':stats[2]}) # mean


        residuals = numexpr.evaluate("array - mean", {'mean':stats[1], 'array':array})
        # variance
        t_emp = numexpr.evaluate("residuals**2").transpose(1,2,0)
        nan_sum = numpy.nansum(t_emp, axis=2)
        stats[3] = numexpr.evaluate("nan_sum / (fvld_obsv - 1)", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})
        stats[4] = numexpr.evaluate("sqrt(var)", {'var':stats[3]}) # stddev
        # skewness
        t_emp = numexpr.evaluate("(residuals / stddev)**3", {'stddev':stats[4], 'residuals':residuals}).transpose(1,2,0)
        nan_sum = numpy.nansum(t_emp, axis=2)
        stats[5] = numexpr.evaluate("nan_sum / fvld_obsv", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})
        # kurtosis
        t_emp = numexpr.evaluate("(residuals / stddev)**4", {'stddev':stats[4], 'residuals':residuals}).transpose(1,2,0)
        nan_sum = numpy.nansum(t_emp, axis=2)
        stats[6] = numexpr.evaluate("(nan_sum / fvld_obsv) - 3", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})

        stats[7] = numpy.nanmax(array_bip, axis=2) # max
        stats[8] = numpy.nanmin(array_bip, axis=2) # min


        # 2nd quantile (aka median)
        # Don't want any interpolated values, so can't use the standard
        # numpy.median() function
        # For odd length arrays, the middle value is taken, for even length
        # arrays, the first of the two middle values is taken

        flat_sort = numpy.sort(array_bip, axis=2).flatten()

        # q2_offset
        q2_idx = numexpr.evaluate("((vld_obsv / 2) + ((vld_obsv % 2) - 1))") # median locations

        # array offsets
        a_off = numpy.arange(cols*rows).reshape((rows,cols))

        # turn the locations into a useable 1D index
        idx_1D = numexpr.evaluate("(q2_idx + a_off * bands)").astype('int')
        stats[9] = flat_sort[idx_1D] # get median values

        # now to find the original index of the median value
        sort_orig_idx = numpy.argsort(array_bip, axis=2).flatten()
        stats[10]  = sort_orig_idx[idx_1D]

        # 1st quantile
        length = numexpr.evaluate("q2_idx + 1")
        q1_idx  = numexpr.evaluate("((length / 2) + ((length % 2) - 1))")
        idx_1D = numexpr.evaluate("(q1_idx + a_off * bands)").astype('int') # 1D index
        stats[11] = flat_sort[idx_1D] # get 1st quantile


        # 3rd quantile
        idx_1D = numexpr.evaluate("((q2_idx + q1_idx) + a_off * bands)").astype('int') # 1D index
        stats[12] = flat_sort[idx_1D] # get 3rd quantile

        # Geometric Mean
        # need to handle nan's so can't use scipy.stats.gmean ## 28/02/2013
        # x**(1./n) gives nth root
        wh = ~(numpy.isfinite(array_bip))
        array_bip[wh] = 1
        stats[13] = numpy.prod(array_bip, axis=2)**(1./vld_obsv)
        #stats[13] = numexpr.evaluate("prod(array_bip, axis=2)")**numexpr.evaluate("(1./vld_obsv)")

        # all nan's will evaluate to 1.0, convert back to NaN 
        wh = numexpr.evaluate("vld_obsv == 0")
        stats[13][wh] = NaN

        # Convert any potential inf values to a NaN for consistancy
        wh = ~(numpy.isfinite(stats))
        stats[wh] = NaN

    else:
        stats = numpy.zeros((output_band_count, rows, cols), dtype='float32')

        stats[0] = numpy.nansum(array, axis=0) # sum
        vld_obsv = numpy.sum(numpy.isfinite(array), axis=0) # we need an int
        stats[2] = vld_obsv
        stats[1] = numexpr.evaluate("fsum / fvld_obsv", {'fsum':stats[0], 'fvld_obsv':stats[2]}) # mean
        residuals = numexpr.evaluate("array - mean", {'mean':stats[1], 'array':array})
        # variance
        t_emp = numexpr.evaluate("residuals**2")
        nan_sum = numpy.nansum(t_emp, axis=0)
        stats[3] = numexpr.evaluate("nan_sum / (fvld_obsv - 1)", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})
        stats[4] = numexpr.evaluate("sqrt(var)", {'var':stats[3]}) # stddev
        # skewness
        t_emp = numexpr.evaluate("(residuals / stddev)**3", {'stddev':stats[4], 'residuals':residuals})
        nan_sum = numpy.nansum(t_emp, axis=0)
        stats[5] = numexpr.evaluate("nan_sum / fvld_obsv", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})
        # kurtosis
        t_emp = numexpr.evaluate("(residuals / stddev)**4", {'stddev':stats[4], 'residuals':residuals})
        nan_sum = numpy.nansum(t_emp, axis=0)
        stats[6] = numexpr.evaluate("(nan_sum / fvld_obsv) - 3", {'fvld_obsv':stats[2], 'nan_sum':nan_sum})

        stats[7] = numpy.nanmax(array, axis=0) # max
        stats[8] = numpy.nanmin(array, axis=0) # min

        # 2nd quantile (aka median)
        # Don't want any interpolated values, so can't use the standard
        # numpy.median() function
        # For odd length arrays, the middle value is taken, for even length
        # arrays, the first of the two middle values is taken

        flat_sort = numpy.sort(array, axis=0).flatten()

        # q2_offset
        q2_idx = numexpr.evaluate("((vld_obsv / 2) + ((vld_obsv % 2) - 1))") # median locations

        # array offsets
        a_off = numpy.arange(cols*rows).reshape((rows,cols))

        # turn the locations into a useable 1D index
        idx_1D = numexpr.evaluate("(q2_idx * cols * rows + a_off)").astype('int')
        stats[9] = flat_sort[idx_1D] # get median values

        # now to find the original index of the median value
        sort_orig_idx = numpy.argsort(array, axis=0).flatten()
        stats[10]  = sort_orig_idx[idx_1D]

        # 1st quantile
        length = numexpr.evaluate("q2_idx + 1")
        q1_idx  = numexpr.evaluate("((length / 2) + ((length % 2) - 1))")
        idx_1D = numexpr.evaluate("(q1_idx * cols * rows + a_off)").astype('int') # 1D index
        stats[11] = flat_sort[idx_1D] # get 1st quantile


        # 3rd quantile
        idx_1D = numexpr.evaluate("((q2_idx + q1_idx) * cols * rows + a_off)").astype('int') # 1D index
        stats[12] = flat_sort[idx_1D] # get 3rd quantile

        # Geometric Mean
        # need to handle nan's so can't use scipy.stats.gmean ## 28/02/2013
        # x**(1./n) gives nth root
        wh = ~(numpy.isfinite(array))
        array[wh] = 1
        stats[13] = numpy.prod(array, axis=0)**(1./vld_obsv)

        # all nan's will evaluate to 1.0, convert back to NaN 
        wh = numexpr.evaluate("vld_obsv == 0")
        stats[13][wh] = NaN

        # Convert any potential inf values to a NaN for consistancy
        wh = ~(numpy.isfinite(stats))
        stats[wh] = NaN

    return stats


    # Now for ENVI style datasets, modify the bandnames
def create_envi_hdr(outfile, noData, new_bnames):
    hdr_fname = os.path.splitext(outfile)[0] + '.hdr'
    # check file exists
    if os.path.exists(hdr_fname):
        # open hdr file for reading
        hdr_open = open(hdr_fname)
        hdr = hdr_open.readlines()
        # close the hdr file
        hdr_open.close()

        fbn = line_num_finder(hdr, 'band names')
        new_hdr = hdr[0:fbn+1]
        f_endbrace = line_num_finder(hdr, '}', offset=fbn)
        bn_stuff = hdr[fbn:f_endbrace+1]

        for bname in new_bnames:
            new_hdr.append(bname + ',\n')
            
        new_hdr[len(new_hdr) - 1].replace(',\n', '}\n')

        # check that f_endbrace is the end of the file
        # if not, then get the extra stuff and append it
        hdr_len = len(hdr)
        if (hdr_len > (f_endbrace +1)):
            extra_hdr = hdr[f_endbrace+1]
            for i in range(len(extra_hdr)):
                new_hdr.append(extra_hdr[i])

        # append the data ignore value
        #data_ignore = 'data ignore value = %i\n' %noData
        data_ignore = 'data ignore value = %s\n' % noData
        new_hdr.append(data_ignore)

        # open the hdr file again for writing
        hdr_open = open(hdr_fname, 'w')

        for line in new_hdr:
            hdr_open.write(line)

        # close the hdr file
        hdr_open.close()

def get_pixel_provenance(input_dataset, output_dataset, median_index_layer=11, datetime_layer = 15, satellite_layer=16):
    
    def seconds_since_epoch(dt):
        return (dt - datetime.utcfromtimestamp(0)).total_seconds()
        
    satellite_id_lookup = {'LS5': 5, 'LS7': 7, 'LS8': 8}   
    
    median_index_array = output_dataset.GetRasterBand(median_index_layer).ReadAsArray()
        
#    log_multiline(logger.debug, median_index_array, 'median_index_array', '\t')
        
    # Create provenance arrays for adding to result dataset
    acquisition_datetime_array = median_index_array.astype(numpy.float32)
    acquisition_datetime_array[:] = NaN
    satellite_array = acquisition_datetime_array.copy()
    
    # Compose two new provenance arrays
    for layer_index in range(input_dataset.RasterCount):
        layer_metadata = input_dataset.GetRasterBand(layer_index + 1).GetMetadata()
        
        # Look up satellite ID
        satellite_id = satellite_id_lookup.get(layer_metadata.get('satellite'))
        
        try:
            start_datetime = datetime.strptime(layer_metadata.get('start_datetime'), '%Y-%m-%dT%H:%M:%S.%f') # e.g: 2010-03-30T23:59:33.942044
        except ValueError:
            start_datetime = None
        
        layer_match_mask = median_index_array == layer_index
        
        # Write values as required
        if satellite_id is not None:
            satellite_array[layer_match_mask] = satellite_id
            
        if start_datetime is not None:
            acquisition_seconds_since_epoch = seconds_since_epoch(start_datetime)
            acquisition_datetime_array[layer_match_mask] = acquisition_seconds_since_epoch    
        
    # Write acquisition times to dataset
    output_band = output_dataset.GetRasterBand(datetime_layer)
    output_band.WriteArray(acquisition_datetime_array)
    output_band.SetNoDataValue(numpy.NaN)
    output_band.FlushCache()
    
    # Write satellite identifiers to dataset
    output_band = output_dataset.GetRasterBand(satellite_layer)
    output_band.WriteArray(satellite_array) 
    output_band.SetNoDataValue(numpy.NaN)
    output_band.FlushCache()
        
    output_dataset.FlushCache()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description='Calculates statistics over the temporal/spectral/z domain.')
    parser.add_argument('--infile', required=True, help='The input file of which to calculate the stats.')
    parser.add_argument('--outfile', required=True, help='The output file name.')
    parser.add_argument('--driver', default='ENVI', help="The file driver type for the output file. See GDAL's list of valid file types. (Defaults to ENVI).")

    parser.add_argument('--xsize', help='The tile size for dimension x.', type=int)
    parser.add_argument('--ysize', help='The tile size for dimension y.', type=int)
    parser.add_argument('--no_data', help='The data value to be ignored.', type=int)
    parser.add_argument('--as_bip', default=False, type=bool, help='If set to True, the array will be transposed to be [Lines,Samples,Bands], and processed in this fashion. The default is to process the array as [Bands,Lines,Samples].')

    parsed_args = parser.parse_args()

    infile  = parsed_args.infile
    outfile = parsed_args.outfile
    driver  = parsed_args.driver
    xtile   = parsed_args.xsize
    ytile   = parsed_args.ysize
    no_data = parsed_args.no_data
    as_bip  = parsed_args.as_bip

    print 'Processing: %s' %infile
    print 'Output: %s' % outfile

    main(infile, outfile, driver, xtile, ytile, no_data, as_bip)


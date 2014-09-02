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

import os
import numpy

# Author: Josh Sixsmith, joshua.sixsmith@ga.gov.au

def edit_envi_hdr(envi_file, noData=None, band_names=None):
    
    def line_num_finder(array, string = "", offset=0):
        for i in (numpy.arange(len(array))+offset):
            if string in str(array[i]):
                return i
    
    hdr_fname = os.path.splitext(envi_file)[0] + '.hdr'
    # check file exists
    if os.path.exists(hdr_fname):
        # open hdr file for reading
        hdr_open = open(hdr_fname)
        hdr = hdr_open.readlines()
        # close the hdr file
        hdr_open.close()

        # find the number of bands. Used to check correct number of bnames
        fnb = line_num_finder(hdr, 'bands')
        sfind = hdr[fnb]
        nb  = int(sfind.split()[2])

        fbn = line_num_finder(hdr, 'band names')
        new_hdr = hdr[0:fbn+1]
        f_endbrace = line_num_finder(hdr, '}', offset=fbn)
        bn_stuff = hdr[fbn:f_endbrace+1]

        if band_names:
            if (len(band_names) != nb):
                raise Exception('Error, band names and number of bands do not match!')
            for i in range(nb): # zero-based index
                if (i == nb - 1):
                    bname = band_names[i] + '}\n'
                else:
                    bname = band_names[i] + ',\n'
                new_hdr.append(bname)
        else:
            band_names = []
            for i in range(1, nb+1): # one-based index
                bname = 'Band %i' % i
                band_names.append(bname)
                
                if (i == nb):
                    bname += '}\n'
                else:
                    bname += ',\n'
            
                new_hdr.append(bname)

        # check that f_endbrace is the end of the file
        # if not, then get the extra stuff and append it
        hdr_len = len(hdr)
        if (hdr_len > (f_endbrace +1)):
            extra_hdr = hdr[f_endbrace+1]
            for i in range(len(extra_hdr)):
                new_hdr.append(extra_hdr[i])

        # append the data ignore value
        if noData:
            data_ignore = 'data ignore value = %s\n' %str(noData)
            new_hdr.append(data_ignore)

        # open the hdr file again for writing
        hdr_open = open(hdr_fname, 'w')

        for line in new_hdr:
            hdr_open.write(line)

        # close the hdr file
        hdr_open.close()


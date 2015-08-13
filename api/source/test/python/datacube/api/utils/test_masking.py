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
import numpy
from pathlib import Path
import timeit


_log = logging.getLogger()


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


flatten = lambda *n: (e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,)))


def log_mem(s=None):

    if s and len(s) > 0:
        _log.debug(s)

    import psutil

    _log.debug("Current memory usage is [%s]", psutil.Process().memory_info())
    _log.debug("Current memory usage is [%d] MB", psutil.Process().memory_info().rss / 1024 / 1024)

    import resource

    _log.debug("Current MAX RSS  usage is [%d] MB", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def get_test_data_path(f=None):

    path = Path(__file__).parent.absolute()

    if f:
        for x in flatten(f):
            path = path.joinpath(x)

    return str(path)


# def test_data_creation():
#
#     stack = []
#
#     for i in range(0, 100):
#
#         data = numpy.random.randint(0, 10000, size=(1000, 1000))
#
#         x = numpy.random.randint(0, 10, size=(1000, 1000))
#         mask = x < 3
#
#         data[mask] = -999
#
#         stack.append(data)
#
#     numpy.save(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"), numpy.array(stack))


# def test_data_creation():
#
#     stack = []
#
#     for i in range(0, 100):
#
#         data = numpy.random.randint(0, 10000, size=(1000, 1000))
#
#         x = numpy.random.randint(0, 10, size=(1000, 1000))
#         mask = x < 3
#
#         data[mask] = -999
#
#         numpy.save(get_test_data_path("data_1000x1000x100_30_percent_nd_{index:03d}.npy".format(index=i)), numpy.array(data))

# def test_data_stacking():
#
#     start_time = timeit.default_timer()
#
#     stack = []
#
#     for i in range(0, 100):
#
#         data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd_{index:03d}.npy".format(index=i)))
#
#         stack.append(data)
#
#     stack = numpy.array(stack, copy=False)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     log_mem("finished")


def test_data_stacking():

    start_time = timeit.default_timer()

    stack = numpy.empty((100, 1000, 1000), dtype=numpy.int16)

    for i in range(0, 100):

        # data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd_{index:03d}.npy".format(index=i)))
        #
        # stack[i] = data

        stack[i] = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd_{index:03d}.npy".format(index=i)))

    elapsed = timeit.default_timer() - start_time

    log_mem("finished")

    _log.info("\nTook %d", elapsed)

    _log.info("\nstack is [%s]\n%s", numpy.shape(stack), stack)



# def test_median_array():
#
#     data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"))
#
#     start_time = timeit.default_timer()
#
#     stat = numpy.median(data, axis=0)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     _log.info("\nmedian is %s", stat)
#
#
# def test_median_masked_array():
#
#     data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"))
#
#     start_time = timeit.default_timer()
#
#     data = numpy.ma.masked_equal(data, -999, copy=False)
#     stat = numpy.ma.median(data, axis=0)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     _log.info("\nmedian is %s", stat)
#
#
# def test_median_masking_array():
#
#     data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"))
#
#     def do_median(data):
#         d = data[data != -999]
#
#         if d.size == 0:
#             return -999
#         else:
#             return numpy.median(a=d)
#
#     start_time = timeit.default_timer()
#
#     stat = numpy.apply_along_axis(do_median, axis=0, arr=data)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     _log.info("\nmedian is %s", stat)
#
#
# def test_percentile_array():
#
#     data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"))
#
#     start_time = timeit.default_timer()
#
#     stat = numpy.percentile(data, axis=0, q=50)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     _log.info("\nmedian is %s", stat)
#
#
# def test_percentile_masking_array():
#
#     data = numpy.load(get_test_data_path("data_1000x1000x100_30_percent_nd.npy"))
#
#     def do_percentile(data):
#         d = data[data != -999]
#
#         if d.size == 0:
#             return -999
#         else:
#             return numpy.percentile(a=d, q=50)
#
#     start_time = timeit.default_timer()
#
#     stat = numpy.apply_along_axis(do_percentile, axis=0, arr=data)
#
#     elapsed = timeit.default_timer() - start_time
#
#     _log.info("\nTook %d", elapsed)
#
#     _log.info("\nP50 is %s", stat)
#
#

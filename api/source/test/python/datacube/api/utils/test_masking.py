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

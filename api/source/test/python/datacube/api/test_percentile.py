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


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

_log = logging.getLogger()


def test_percentile():

    print "Starting..."

    # print "GREEN", GREEN
    #
    # print "GREEN MASKED", numpy.ma.masked_equal(GREEN, -999)
    #
    # d = numpy.array(GREEN)
    #
    # print "GREEN FILTERED", d[d != -999]
    # print "GREEN FILTERED", numpy.percentile(d[d != -999], [25, 50, 75], interpolation="nearest")
    #
    # return

    _log.info("Testing....")

    do_percentile(GREEN, 25)
    do_percentile(GREEN, 50)
    do_percentile(GREEN, 75)


def do_percentile(d, percentile, ndv=-999):

    _log.info("Percentile=%s...", percentile)

    # do_percentile_plain(d, percentile)
    do_percentile_stripped(d, percentile, ndv)
    # do_percentile_nan(d, percentile, ndv)
    # do_percentile_nan_testing(d, percentile, ndv)
    # do_percentile_scipy(d, percentile, ndv)
    do_percentile_testing([d, d], percentile, ndv)


def do_percentile_plain(d, percentile):

    _log.info("Plain percentile...")

    data = numpy.array(d)

    p = numpy.percentile(data, percentile, interpolation="nearest")
    _log.info("p = %d which is index %s", p, numpy.where(data == p))

    # p = do_percentile_python_plain(d, percentile)
    # _log.info("p = %d which is index %s", p, numpy.where(data == p))


def do_percentile_stripped(d, percentile, ndv):

    _log.info("Stripped NDV percentile...")

    data = numpy.array(d)

    p = numpy.percentile(data[data != ndv], percentile, interpolation="nearest")
    _log.info("p = %d which is index %s", p, numpy.where(data == p))

    # p = do_percentile_python_plain(data[data != ndv], percentile)
    # _log.info("p = %d which is index %s", p, numpy.where(data == p))


def do_percentile_nan(d, percentile, ndv):

    _log.info("NAN percentile...")

    data = numpy.ma.masked_equal(d, ndv).astype(numpy.float16).filled(numpy.nan)

    p = numpy.nanpercentile(data, percentile, interpolation="nearest")
    _log.info("p = %d which is index %s", p, numpy.where(data == p))

    # p = do_percentile_python_plain(d, percentile)
    # _log.info("p = %d which is index %s", p, numpy.where(data == p))


# def do_percentile_nan_testing(d, percentile, ndv):
#
#     _log.info("NAN percentile TESTING...")
#
#     # data = numpy.array(d, dtype=numpy.int16)
#     # data = numpy.ma.masked_equal(data, ndv)
#     # data = numpy.ndarray.astype(data, dtype=numpy.float16)
#     # data = data.filled(numpy.nan)
#
#     data = numpy.array(d, dtype=numpy.float16)
#     _log.info(data)
#     data = numpy.ma.masked_equal(data, ndv)
#     data = data.filled(numpy.nan)
#
#     p = numpy.nanpercentile(data, percentile, interpolation="lower")
#     _log.info("p = %d which is index %s", p, numpy.where(data == p))
#
#     # p = do_percentile_python_plain(d, percentile)
#     # _log.info("p = %d which is index %s", p, numpy.where(data == p))


def do_percentile_testing(d, percentile, ndv):
    _log.info("percentile testing...")

    data = numpy.array(d)
    _log.info("d has shape %s", numpy.shape(data))

    p = numpy.apply_along_axis(do_percentile_1d, data, percentile, ndv)
    _log.info("p = %s", p)


def do_percentile_1d(data):
    d = data[data != -999]
    return numpy.percentile(d, [25, 50, 75], interpolation="nearest")

# def do_percentile_scipy(d, percentile, ndv):
#
#     _log.info("SCIPY percentile...")
#
#     data = numpy.ma.masked_equal(d, ndv)
#
#     p = scipy.stats.mstats.scoreatpercentile(data, percentile)
#     _log.info("p = %d which is index %s", p, numpy.where(data == p))

    # p = do_percentile_python_plain(d, percentile)
    # _log.info("p = %d which is index %s", p, numpy.where(data == p))


# def do_percentile_python_plain(d, percentile):
#     size = len(d)
#     return sorted(d)[int(math.ceil((size * percentile))) - 1]


DATA = [
    [[565, 565, 532, 516, 500, 516, 656, 532, 738, 532],
     [549, 598, 565, 532, 697, 614, 656, 500, 516, 516],
     [565, 549, 697, 656, 635, 594, 676, 483, 500, 570],
     [565, 598, 656, 656, 532, 635, 676, 520, 500, 516],
     [565, 582, 532, 549, 532, 516, 500, 516, 532, 532],
     [549, 565, 516, 532, 516, 538, 512, 516, 586, 549],
     [532, 565, 549, 549, 565, 676, 516, 516, 549, 549],
     [635, 635, 656, 553, 549, 553, 553, 516, 590, 532],
     [565, 676, 516, 516, 565, 635, 500, 570, 617, 532],
     [586, 586, 635, 516, 549, 537, 516, 500, 516, 532]],

    [[-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],
     [-999, -999, -999, -999, -999, -999, -999, -999, -999, -999],]
]

def test_fred(x=2, y=2, z=50, ndv=-999):

    print "Testing..."

    data = numpy.load("/tmp/random_{x}_{y}_{z}.npy".format(x=x, y=y, z=z))
    # print numpy.shape(data), data

    # Calculate the percentiles stripping out no data values

    p = numpy.empty((y, x), dtype=numpy.int16)

    from itertools import product

    for y, x in product(range(0, y), range(0, x)):
        d = data[:, x, y]
        # print "data", x, y, numpy.shape(d), d
        # print "stripped data", numpy.shape(d[d != ndv]), d[d != ndv]
        p[x, y] = numpy.percentile(d[d != ndv], 75, axis=0, interpolation="nearest")
        # print "stripped percentiles", p[x, y]

    print "percentiles\n", p

    from datacube.api.utils import calculate_stack_statistic_percentile

    print "calculated percentiles\n", calculate_stack_statistic_percentile(data, 75)

    print data[:, 0, 0]

    print numpy.apply_along_axis(do_percentile_1d, axis=0, arr=data)


# def test_percentile_performance():
    # generate_random_data(x=1000, y=1000, z=1000, ndv=-999)


def generate_random_data(x=2, y=2, z=50, ndv=-999):

    shape = (z, y, x)

    print shape[0], shape[1], shape[2]

    # Generate a stack of random values with no data interspersed

    stack = list()

    for i in range(0, shape[0]):

        if i % 2 == 0:
            data = numpy.empty((y, x), dtype=numpy.int16)
            data.fill(ndv)
        else:
            data = (numpy.random.rand(y, x) * 10000).astype(numpy.int16)

        stack.append(data)

    print numpy.shape(stack)

    numpy.save("random_{x}_{y}_{z}.npy".format(x=x, y=y, z=z), numpy.array(stack))


def plain_old_loop(data):

    print "plain old loop..."

    shape = numpy.shape(data)

    p = numpy.empty(shape[1:], dtype=numpy.int16)

    from itertools import product

    for y, x in product(range(0, shape[1]), range(0, shape[2])):
        d = data[:, x, y]
        p[x, y] = numpy.percentile(d[d != -999], 75, axis=0, interpolation="nearest")

    print "percentiles\n", p


def existing_nan_percentile(data):
    print "existing nan percentile..."

    from datacube.api.utils import calculate_stack_statistic_percentile

    print "calculated percentiles\n", calculate_stack_statistic_percentile(data, [25, 50, 75])


def apply_along_axis(data):
    print "apply along axis..."

    print "calculated percentiles\n", numpy.apply_along_axis(do_percentile_1d, axis=0, arr=data)


if __name__ == "__main__":

    # data = numpy.load("/tmp/random_{x}_{y}_{z}.npy".format(x=1000, y=1000, z=1000))
    data = numpy.load("/tmp/random_{x}_{y}_{z}.npy".format(x=2, y=2, z=50))

    existing_nan_percentile(data)
    # plain_old_loop(data)
    # apply_along_axis(data)



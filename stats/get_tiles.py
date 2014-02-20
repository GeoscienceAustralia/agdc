"""get_tiles.py: Author: Josh Sixsmith; joshua.sixsmith@ga.gov.au"""

import numpy


def get_tile3(samples, lines, xtile=100, ytile=100):
    """Produces a list of tiles with which to break up a large image.

    (samples, lines) is the size of the image as (xsize, ysize),
    (xtile, ytile) are the maximum size of tiles to split it into.

    The output is a list of tuples (ystart, yend, xstart, xend) describing
    the tiles. Note the reversal of x and y coords for output. Note also
    that these tiles are used to break up an image, and are nothing to do
    with the larger tiles used by the data cube.

    """

    assert xtile > 0 and ytile > 0, \
        'get_tile3: the tile size (xtile,ytile) should not be empty'

    ncols = samples
    nrows = lines
    tiles = []
    xstart = numpy.arange(0, ncols, xtile)
    ystart = numpy.arange(0, nrows, ytile)
    for ystep in ystart:
        if ystep + ytile < nrows:
            yend = ystep + ytile
        else:
            yend = nrows
        for xstep in xstart:
            if xstep + xtile < ncols:
                xend = xstep + xtile
            else:
                xend = ncols
            tiles.append((ystep, yend, xstep, xend))
    return tiles

import numpy

#Author: Josh Sixsmith; joshua.sixsmith@ga.gov.au

def get_tile(array, xtile=100,ytile=100):
    st = datetime.datetime.now()
    dims = array.shape
    ncols = dims[1]
    nrows = dims[0]
    if len(dims) >2:
        ncols = dims[2]
        nrows = dims[1]
        dims  = (nrows,ncols)
    xstart = numpy.arange(0,ncols,xtile)
    ystart = numpy.arange(0,nrows,ytile)
    xend   = numpy.zeros(xstart.shape, dtype='int32')
    yend   = numpy.zeros(ystart.shape, dtype='int32')
    i = 0
    for step in xstart:
        if step + xtile < ncols:
            xend[i] = step + xtile
        else:
            xend[i] = ncols
        i += 1
    i = 0
    for step in ystart:
        if step + ytile < nrows:
            yend[i] = step + ytile
        else:
            yend[i] = nrows
        i += 1
    et = datetime.datetime.now()
    print 'get_tile time taken: ', et - st
    return (xstart, xend), (ystart,yend)

def get_tile2(array, xtile=100,ytile=100):
    st = datetime.datetime.now()
    dims = array.shape
    ncols = dims[1]
    nrows = dims[0]
    l = []
    if len(dims) >2:
        ncols = dims[2]
        nrows = dims[1]
        dims  = (nrows,ncols)
    xstart = numpy.arange(0,ncols,xtile)
    ystart = numpy.arange(0,nrows,ytile)
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
            l.append((ystep,yend,xstep, xend))
    et = datetime.datetime.now()
    print 'get_tile2 time taken: ', et - st
    return l 

def get_tile3(samples, lines, xtile=100,ytile=100):
    ncols = samples
    nrows = lines
    tiles = []
    xstart = numpy.arange(0,ncols,xtile)
    ystart = numpy.arange(0,nrows,ytile)
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
            tiles.append((ystep,yend,xstep, xend))
    return tiles


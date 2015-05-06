#!/usr/bin/env python


# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# * Neither Geoscience Australia nor the names of its contributors may be
# used to endorse or promote products derived from this software
# without specific prior written permission.
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


import abc
import argparse
import logging
import sys
from datacube.api.model import Satellite, dataset_type_database, dataset_type_derived_nbar
from datacube.api.utils import PqaMask, WofsMask
from datacube.api import satellite_arg, pqa_mask_arg, wofs_mask_arg, parse_date_min, parse_date_max, readable_file


__author__ = "Simon Oldfield"


_log = logging.getLogger()


class Tool(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.acq_min = None
        self.acq_max = None

        # self.process_min = None
        # self.process_max = None
        #
        # self.ingest_min = None
        # self.ingest_max = None

        self.satellites = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.mask_wofs_apply = None
        self.mask_wofs_mask = None

    @staticmethod
    def get_supported_dataset_types():
        return dataset_type_database + dataset_type_derived_nbar

    def setup_arguments(self):

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1980")
        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2020")

        # parser.add_argument("--process-min", help="Process Date", action="store", dest="process_min", type=str)
        # parser.add_argument("--process-max", help="Process Date", action="store", dest="process_max", type=str)
        #
        # parser.add_argument("--ingest-min", help="Ingest Date", action="store", dest="ingest_min", type=str)
        # parser.add_argument("--ingest-max", help="Ingest Date", action="store", dest="ingest_max", type=str)

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellite",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([s.name for s in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=False)
        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask, default=[PqaMask.PQ_MASK_CLEAR],
                                 metavar=" ".join([s.name for s in PqaMask]))

        self.parser.add_argument("--mask-wofs-apply", help="Apply WOFS mask", action="store_true",
                                 dest="mask_wofs_apply",
                                 default=False)
        self.parser.add_argument("--mask-wofs-mask", help="The WOFS mask to apply", action="store",
                                 dest="mask_wofs_mask",
                                 type=wofs_mask_arg, nargs="+", choices=WofsMask, default=[WofsMask.WET],
                                 metavar=" ".join([s.name for s in WofsMask]))

    def process_arguments(self, args):

        _log.setLevel(args.log_level)

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        # self.process_min = parse_date_min(args.process_min)
        # self.process_max = parse_date_max(args.process_max)
        #
        # self.ingest_min = parse_date_min(args.ingest_min)
        # self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellite

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.mask_wofs_apply = args.mask_wofs_apply
        self.mask_wofs_mask = args.mask_wofs_mask

    def log_arguments(self):

        # process = {process_min} to {process_max}
        # ingest = {ingest_min} to {ingest_max}
        #            process_min=self.process_min, process_max=self.process_max,
        #            ingest_min=self.ingest_min, ingest_max=self.ingest_max,

        _log.info("""
        acq = {acq_min} to {acq_max}
        satellites = {satellites}
        PQA mask = {pqa_mask}
        WOFS mask = {wofs_mask}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   satellites=" ".join([satellite.name for satellite in self.satellites]),
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   wofs_mask=self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""))

    @abc.abstractmethod
    def go(self):
        _log.error("This method should be overridden")

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        self.go()


class CellTool(Tool):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        Tool.__init__(self, name)

        self.x = None
        self.y = None

        self.mask_vector_apply = None
        self.mask_vector_file = None
        self.mask_vector_layer = None
        self.mask_vector_feature = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Tool.setup_arguments(self)

        self.parser.add_argument("--x", help="X grid reference", action="store", dest="x", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="[110 - 155]")
        self.parser.add_argument("--y", help="Y grid reference", action="store", dest="y", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="[-45 - -10]")

        self.parser.add_argument("--mask-vector-apply", help="Apply mask from feature in vector file",
                                 action="store_true", dest="mask_vector_apply", default=False)
        self.parser.add_argument("--mask-vector-file", help="The vector file containing the mask",
                                 action="store", dest="mask_vector_file",
                                 type=readable_file)
        self.parser.add_argument("--mask-vector-layer", help="The vector layer containing the mask",
                                 action="store", dest="mask_vector_layer", type=str)
        self.parser.add_argument("--mask-vector-feature", help="The vector feature containing the mask",
                                 action="store", dest="mask_vector_feature", type=int)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Tool.process_arguments(self, args)

        self.x = args.x
        self.y = args.y

        self.mask_vector_apply = args.mask_vector_apply
        self.mask_vector_file = args.mask_vector_file
        self.mask_vector_layer = args.mask_vector_layer
        self.mask_vector_feature = args.mask_vector_feature

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        Tool.log_arguments(self)

        _log.info("""
        x = {x:03d}
        y = {y:04d}
        VECTOR mask = {vector_mask}
        """.format(x=self.x, y=self.y,
                   vector_mask=self.mask_vector_apply and " ".join([self.mask_vector_file, self.mask_vector_layer, str(self.mask_vector_feature)]) or ""))

    @abc.abstractmethod
    def go(self):
        _log.error("This method should be overridden")


class AoiTool(Tool):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        Tool.__init__(self, name)

        self.vector_file = None
        self.vector_layer = None
        self.vector_fid = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Tool.setup_arguments(self)

        self.parser.add_argument("--vector-file", help="Vector file containing AOI definition(s)", action="store",
                                 dest="vector_file", type=readable_file, required=True)

        self.parser.add_argument("--vector-layer", help="Layer Number in Vector File", action="store",
                                 dest="vector_layer", type=int, default=0)

        self.parser.add_argument("--vector-fid", help="Feature ID in Vector File Layer", action="store",
                                 dest="vector_fid", type=int, default=0)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Tool.process_arguments(self, args)

        self.vector_file = args.vector_file
        self.vector_layer = args.vector_layer
        self.vector_fid = args.vector_fid

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        Tool.log_arguments(self)

        _log.info("""
        vector file = {filename}
        layer ID = {lid}
        feature = {fid}
        """.format(filename=self.vector_file, lid=self.vector_layer, fid=self.vector_fid))

    @abc.abstractmethod
    def go(self):
        _log.error("This method should be overridden")

    # def extract_layer(self):
    #
    #     import ogr
    #     from gdalconst import GA_ReadOnly
    #
    #     vector = ogr.Open(self.vector_file, GA_ReadOnly)
    #     assert vector
    #
    #     layer = vector.GetLayer(self.vector_layer)
    #     assert layer
    #
    #     return layer

    def extract_feature(self):

        import ogr
        from gdalconst import GA_ReadOnly

        vector = ogr.Open(self.vector_file, GA_ReadOnly)
        assert vector

        layer = vector.GetLayer(self.vector_layer)
        assert layer

        feature = layer.GetFeature(self.vector_fid)
        assert feature

        return feature

    def extract_bounds_from_vector(self):

        import math

        feature = self.extract_feature()

        _log.debug("Feature is [%s]", feature)

        ulx, lrx, lry, uly = feature.GetGeometryRef().GetEnvelope()
        _log.debug("envelope = [%s]", (ulx, uly, lrx, lry))

        ulx, lrx, uly, lry = (int(math.floor(ulx)), int(math.floor(lrx)), int(math.floor(uly)), int(math.floor(lry)))

        return ulx, lrx, uly, lry

    def extract_cells_from_vector(self):

        import ogr

        feature = self.extract_feature()

        geom = feature.GetGeometryRef()

        ulx, lrx, uly, lry = self.extract_bounds_from_vector()
        _log.debug("cell bounds are [%s]", (ulx, uly, lrx, lry))

        import itertools

        # return [(x, y) for x, y in itertools.product(range(ulx, lrx), range(lry, uly))]

        cells = list()

        for x, y in itertools.product(range(ulx, lrx + 1), range(lry, uly + 1)):
            _log.debug("Checking if [%03d,%04d] intersects", x, y)

            polygon = ogr.CreateGeometryFromWkt(
                "POLYGON(({ulx} {uly}, {urx} {ury}, {lrx} {lry}, {llx} {lly}, {ulx} {uly}))".format(
                    ulx=x, uly=(y+1), urx=(x+1), ury=(y+1), lrx=(x+1), lry=y, llx=x, lly=y
                ))

            _log.debug("cell polygon is [%s]", polygon.ExportToWkt())

            _log.debug("intersection is [%s]", geom.Intersection(polygon))

            if geom.Intersects(polygon):
                _log.debug("INTERSECTION")
                cells.append((x, y))

            else:
                _log.debug("NO INTERSECTION")

        return cells

    def get_mask_aoi_cell(self, x, y, width=4000, height=4000):

        import gdal
        import osr

        driver = gdal.GetDriverByName("MEM")
        assert driver

        raster = driver.Create("", width, height, 1, gdal.GDT_Byte)
        assert raster

        raster.SetGeoTransform((x, 0.00025, 0.0, y+1, 0.0, -0.00025))

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        raster.SetProjection(srs.ExportToWkt())

        import ogr
        from gdalconst import GA_ReadOnly

        vector = ogr.Open(self.vector_file, GA_ReadOnly)
        assert vector

        layer = vector.GetLayer(self.vector_layer)
        assert layer

        layer.SetAttributeFilter("FID={fid}".format(fid=self.vector_fid))

        gdal.RasterizeLayer(raster, [1], layer, burn_values=[1])

        del layer

        band = raster.GetRasterBand(1)
        assert band

        data = band.ReadAsArray()
        import numpy

        _log.debug("Read [%s] from memory AOI mask dataset", numpy.shape(data))
        return numpy.ma.masked_not_equal(data, 1, copy=False).mask



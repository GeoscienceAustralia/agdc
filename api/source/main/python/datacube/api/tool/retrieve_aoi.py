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
from datacube.api import dataset_type_arg, DatasetType, writeable_dir, output_format_arg, OutputFormat
from datacube.api.query import list_cells_vector_file, list_tiles_vector_file
from datacube.api.tool import AoiTool


_log = logging.getLogger()


class RetrieveAoiTool(AoiTool):

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        AoiTool.__init__(self, name)

        self.dataset_types = None

        self.output_directory = None
        self.overwrite = None
        self.list_only = None

        self.output_format = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        AoiTool.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The type(s) of dataset to retrieve",
                                 action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 nargs="+",
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25, required=True,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--overwrite", help="Over write existing output file", action="store_true",
                                 dest="overwrite", default=False)

        self.parser.add_argument("--list-only",
                                 help="List the datasets that would be retrieved rather than retrieving them",
                                 action="store_true", dest="list_only", default=False)

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        AoiTool.process_arguments(self, args)

        self.dataset_types = args.dataset_type

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        self.output_format = args.output_format

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        AoiTool.log_arguments(self)

        _log.info("""
        datasets to retrieve = {dataset_type}
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        output format = {output_format}
        """.format(dataset_type=" ".join([d.name for d in self.dataset_types]),
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   output_format=self.output_format.name))

    def get_cells(self):

        return list(self.get_cells_from_db())

    def get_cells_from_db(self):

        dataset_types = [d for d in self.dataset_types]

        if self.mask_pqa_apply and DatasetType.PQ25 not in dataset_types:
            dataset_types.append(DatasetType.PQ25)

        if self.mask_wofs_apply and DatasetType.WATER not in dataset_types:
            dataset_types.append(DatasetType.WATER)

        for cell in list_cells_vector_file(vector_file=self.vector_file,
                                           vector_layer=self.vector_layer,
                                           vector_feature=self.vector_feature,
                                           acq_min=self.acq_min, acq_max=self.acq_max,
                                           satellites=[satellite for satellite in self.satellites],
                                           dataset_types=dataset_types):
            yield cell

    def get_tiles(self):

        return list(self.get_tiles_from_db())

    def get_tiles_from_db(self):

        dataset_types = [d for d in self.dataset_types]

        if self.mask_pqa_apply and DatasetType.PQ25 not in dataset_types:
            dataset_types.append(DatasetType.PQ25)

        if self.mask_wofs_apply and DatasetType.WATER not in dataset_types:
            dataset_types.append(DatasetType.WATER)

        for tile in list_tiles_vector_file(vector_file=self.vector_file,
                                           vector_layer=self.vector_layer,
                                           vector_feature=self.vector_feature, acq_min=self.acq_min, acq_max=self.acq_max,
                                           satellites=[satellite for satellite in self.satellites],
                                           dataset_types=dataset_types):
            yield tile

    def go_go_mobile(self):

        # self.extract_bounds_from_vector()

        import ogr
        from gdalconst import GA_ReadOnly

        vector = ogr.Open(self.vector_file, GA_ReadOnly)
        assert vector

        layer = vector.GetLayer(self.vector_layer)
        assert layer

        create_aoi_mask_for_cell(layer, 0, 148, -36)

    def go(self):

        # initially throw a hissy if more than one cell is required

        cells = self.get_cells()

        for cell in cells:
            _log.info("Found cell [%s]", cell.xy)

        tiles = self.get_tiles()

        for tile in tiles:
            _log.info("Found tile [%s]", tile.datasets[DatasetType.ARG25].path)

        # Need the tiles organised more sensibly for the area of interest
        # Want to group as an acquisition of the area of interest
        # The key for the acquisition is date and satellite...ish

        # query for the datasets that cover the area meeting the given criteria (satellite, acq date, ...)
        # order these left to right, top to bottom

        # group them into an "area acquisition" - i.e. if they span passes then datasets from the same satellite within
        # the given time range (e.g. 16 days)

        pass


def create_aoi_mask_for_cell(layer, fid, x, y, width=4000, height=4000):

    import gdal
    import osr

    driver = gdal.GetDriverByName("GTiff")
    assert driver

    raster = driver.Create("/Users/simon/tmp/cube/andreia_nbr/tmp/aoi.tif".format(x=x, y=y), width, height, 1, gdal.GDT_Byte)
    assert raster

    raster.SetGeoTransform((x, 0.00025, 0.0, y+1, 0.0, -0.00025))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    raster.SetProjection(srs.ExportToWkt())

    # feature = layer.GetFeature(fid)
    # assert feature
    #
    # projection = osr.SpatialReference()
    # projection.ImportFromEPSG(4326)
    #
    # geom = feature.GetGeometryRef()
    #
    # # Transform if required
    #
    # if not projection.IsSame(geom.GetSpatialReference()):
    #     geom.TransformTo(projection)

    layer.SetAttributeFilter("FID={fid}".format(fid=fid))

    gdal.RasterizeLayer(raster, [1], layer, burn_values=[1])

    del layer

    band = raster.GetRasterBand(1)
    band.SetNoDataValue(0)
    band.FlushCache()
    del band

    raster.FlushCache()
    del raster


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    RetrieveAoiTool("Retrieve AOI").run()
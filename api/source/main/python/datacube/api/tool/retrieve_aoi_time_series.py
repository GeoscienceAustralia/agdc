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


__author__ = "Simon Oldfield"


import csv
import logging
import os
from datacube.api import dataset_type_arg, writeable_dir
from datacube.api.tool import AoiTool
from datacube.api.utils import log_mem, intersection, get_mask_pqa, get_mask_wofs, get_dataset_data, NDV
from datacube.api.utils import get_dataset_metadata
from datacube.api.model import DatasetType, Satellite, get_bands
from functools import reduce

_log = logging.getLogger()


class RetrieveAoiTimeSeries(AoiTool):

    def __init__(self):

        # Call method on super class
        # super(self.__class__, self).__init__("Retrieve AOI Time Series")
        AoiTool.__init__(self, "Retrieve AOI Time Series")

        self.dataset_type = None
        self.bands = None
        self.output_directory = None
        self.overwrite = None
        self.list_only = None
        self.output_no_data = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        AoiTool.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The types of dataset to retrieve", action="store",
                                 dest="dataset_type", type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

        self.parser.add_argument("--band", help="The band(s) to retrieve", action="store",
                                 dest="bands", type=int, nargs="+")

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir)

        self.parser.add_argument("--overwrite", help="Over write existing output file", action="store_true",
                                 dest="overwrite", default=False)

        self.parser.add_argument("--list-only", help="Just list datasets that would be processed", action="store_true",
                                 dest="list_only", default=False)

        self.parser.add_argument("--hide-no-data", help="Don't output records that are completely no data value(s)",
                                 action="store_false", dest="output_no_data", default=True)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        AoiTool.process_arguments(self, args)

        self.dataset_type = args.dataset_type
        self.bands = args.bands
        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only
        self.output_no_data = args.output_no_data

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        AoiTool.log_arguments(self)

        _log.info("""
        dataset type = {dataset_type}
        bands = {bands}
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        output no data values = {output_no_data}
        """.format(dataset_type=self.dataset_type.name,
                   bands=self.bands and self.bands or "",
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   output_no_data=self.output_no_data,
                   ))

    def go(self):

        import numpy
        from datacube.api.query import list_cells_as_list, list_tiles_as_list
        from datacube.config import Config

        # Verify that all the requested satellites have the same band combinations

        dataset_bands = get_bands(self.dataset_type, self.satellites[0])

        _log.info("dataset bands is [%s]", " ".join([b.name for b in dataset_bands]))

        for satellite in self.satellites:
            if dataset_bands != get_bands(self.dataset_type, satellite):
                _log.error("Satellites [%s] have differing bands", " ".join([satellite.name for satellite in self.satellites]))
                raise Exception("Satellites with different band combinations selected")

        bands = []

        dataset_bands_list = list(dataset_bands)

        if not self.bands:
            bands = dataset_bands_list

        else:
            for b in self.bands:
                bands.append(dataset_bands_list[b - 1])

        _log.info("Using bands [%s]", " ".join(band.name for band in bands))

        x_min, x_max, y_max, y_min = self.extract_bounds_from_vector()
        _log.debug("The bounds are [%s]", (x_min, x_max, y_min, y_max))

        cells_vector = self.extract_cells_from_vector()
        _log.debug("Intersecting cells_vector are [%d] [%s]", len(cells_vector), cells_vector)

        config = Config()
        _log.debug(config.to_str())

        x_list = range(x_min, x_max + 1)
        y_list = range(y_min, y_max + 1)

        _log.debug("x = [%s] y=[%s]", x_list, y_list)

        cells_db = list()

        for cell in list_cells_as_list(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       dataset_types=[self.dataset_type]):
            cells_db.append((cell.x, cell.y))

        _log.debug("Cells from DB are [%d] [%s]", len(cells_db), cells_db)

        cells = intersection(cells_vector, cells_db)
        _log.debug("Combined cells are [%d] [%s]", len(cells), cells)

        for (x, y) in cells:
            _log.info("Processing cell [%3d/%4d]", x, y)

            tiles = list_tiles_as_list(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       dataset_types=[self.dataset_type])

            _log.info("There are [%d] tiles", len(tiles))

            if self.list_only:
                for tile in tiles:
                    _log.info("Would process [%s]", tile.datasets[self.dataset_type].path)
                continue

            # Calculate the mask for the cell

            mask_aoi = self.get_mask_aoi_cell(x, y)

            pixel_count = 4000 * 4000

            pixel_count_aoi = (mask_aoi == False).sum()

            _log.debug("mask_aoi is [%s]\n[%s]", numpy.shape(mask_aoi), mask_aoi)

            metadata = None

            with self.get_output_file() as csv_file:

                csv_writer = csv.writer(csv_file)

                import operator

                header = reduce(operator.add, [["DATE", "INSTRUMENT", "# PIXELS", "# PIXELS IN AOI"]] + [
                    ["%s - # DATA PIXELS" % b.name,
                     "%s - # DATA PIXELS AFTER PQA" % b.name,
                     "%s - # DATA PIXELS AFTER PQA WOFS" % b.name,
                     "%s - # DATA PIXELS AFTER PQA WOFS AOI" % b.name,
                     "%s - MIN" % b.name, "%s - MAX" % b.name, "%s - MEAN" % b.name] for b in bands])

                csv_writer.writerow(header)

                for tile in tiles:

                    _log.info("Processing tile [%s]", tile.datasets[self.dataset_type].path)

                    if self.list_only:
                        continue

                    if not metadata:
                        metadata = get_dataset_metadata(tile.datasets[self.dataset_type])

                    # Apply PQA if specified

                    pqa = None
                    mask_pqa = None

                    if self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets:
                        pqa = tile.datasets[DatasetType.PQ25]
                        mask_pqa = get_mask_pqa(pqa, self.mask_pqa_mask)

                    _log.debug("mask_pqa is [%s]\n[%s]", numpy.shape(mask_pqa), mask_pqa)

                    # Apply WOFS if specified

                    wofs = None
                    mask_wofs = None

                    if self.mask_wofs_apply and DatasetType.WATER in tile.datasets:
                        wofs = tile.datasets[DatasetType.WATER]
                        mask_wofs = get_mask_wofs(wofs, self.mask_wofs_mask)

                    _log.debug("mask_wofs is [%s]\n[%s]", numpy.shape(mask_wofs), mask_wofs)

                    data = get_dataset_data(tile.datasets[self.dataset_type], bands=bands)
                    _log.debug("data is [%s]\n[%s]", numpy.shape(data), data)

                    pixel_count_data = dict()
                    pixel_count_data_pqa = dict()
                    pixel_count_data_pqa_wofs = dict()
                    pixel_count_data_pqa_wofs_aoi = dict()
                    mmin = dict()
                    mmax = dict()
                    mmean = dict()

                    for band in bands:

                        data[band] = numpy.ma.masked_equal(data[band], NDV)
                        _log.debug("masked data is [%s] [%d]\n[%s]", numpy.shape(data), numpy.ma.count(data), data)

                        pixel_count_data[band] = numpy.ma.count(data[band])

                        if pqa:
                            data[band].mask = numpy.ma.mask_or(data[band].mask, mask_pqa)
                            _log.debug("PQA masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa[band] = numpy.ma.count(data[band])

                        if wofs:
                            data[band].mask = numpy.ma.mask_or(data[band].mask, mask_wofs)
                            _log.debug("WOFS masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa_wofs[band] = numpy.ma.count(data[band])

                        data[band].mask = numpy.ma.mask_or(data[band].mask, mask_aoi)
                        _log.debug("AOI masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa_wofs_aoi[band] = numpy.ma.count(data[band])

                        mmin[band] = numpy.ma.min(data[band])
                        mmax[band] = numpy.ma.max(data[band])
                        mmean[band] = numpy.ma.mean(data[band])

                        # Convert the mean to an int...which is actually trickier than you would expect due to masking....

                        if numpy.ma.count(mmean[band]) != 0:
                            mmean[band] = mmean[band].astype(numpy.int16)

                    # Should we output if no data values found?
                    pixel_count_data_pqa_wofs_aoi_all_bands = reduce(operator.add, pixel_count_data_pqa_wofs_aoi.itervalues())
                    if pixel_count_data_pqa_wofs_aoi_all_bands == 0 and not self.output_no_data:
                        _log.info("Skipping dataset with no non-masked data values in ANY band")
                        continue

                    row = reduce(
                        operator.add,
                            [[tile.end_datetime,
                              self.decode_satellite_as_instrument(tile.datasets[self.dataset_type].satellite),
                              pixel_count, pixel_count_aoi]] +

                            [[pixel_count_data[band], pixel_count_data_pqa[band],
                              pixel_count_data_pqa_wofs[band], pixel_count_data_pqa_wofs_aoi[band],
                              mmin[band], mmax[band], mmean[band]] for band in bands])

                    csv_writer.writerow(row)

    @staticmethod
    def decode_satellite_as_instrument(satellite):

        instruments = {Satellite.LS5: "TM", Satellite.LS7: "ETM", Satellite.LS8: "OLI"}

        if satellite in instruments:
            return instruments[satellite]

        return "N/A"

    def get_output_file(self):

        import sys

        if not self.output_directory:
            _log.info("Writing output to standard output")
            return sys.stdout

        # TODO
        # filename = self.get_output_filename(self.dataset_type)
        filename = os.path.join(self.output_directory, "output.csv")

        _log.info("Writing output to %s", filename)

        if os.path.exists(filename) and not self.overwrite:
            _log.error("Output file [%s] exists", filename)
            raise Exception("Output file [%s] already exists" % filename)

        return open(filename, "wb")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    log_mem("Start")

    RetrieveAoiTimeSeries().run()

    log_mem("Finish")

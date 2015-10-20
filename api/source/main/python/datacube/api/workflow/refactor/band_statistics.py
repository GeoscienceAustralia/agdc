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


import argparse
import logging
import sys
from datacube.api import writeable_dir, satellite_arg, Satellite, pqa_mask_arg, PqaMask, dataset_type_arg, DatasetType
from datacube.api import season_arg, output_format_arg, OutputFormat, parse_date_min, parse_date_max
from datacube.api import Season, Month
from datacube.api.model import Ls57Arg25Bands


_log = logging.getLogger()


SEASONS = {
    Season.SUMMER: ((Month.NOVEMBER, 17), (Month.APRIL, 25)),
    Season.AUTUMN: ((Month.FEBRUARY, 16), (Month.JULY, 25)),
    Season.WINTER: ((Month.MAY, 17), (Month.OCTOBER, 25)),
    Season.SPRING: ((Month.AUGUST, 17), (Month.JANUARY, 25))
}


def ls57_arg_band_arg(s):
    if s in [t.name for t in Ls57Arg25Bands]:
        return Ls57Arg25Bands[s]
    raise argparse.ArgumentTypeError("{0} is not a supported LS57 ARG25 band".format(s))


class BandStatisticsWorkflow():

    def __init__(self, name="Arg25 Band Stack Workflow"):

        # Workflow.__init__(self, name="Arg25 Band Stack Workflow")

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.x_min = None
        self.x_max = None

        self.y_min = None
        self.y_max = None

        self.acq_min = None
        self.acq_max = None

        self.epoch = None

        self.seasons = None

        self.satellites = None

        self.output_directory = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.local_scheduler = None
        self.workers = None

        self.dataset_type = None
        self.bands = None

        self.output_format = None

    def setup_arguments(self):

        # # Call method on super class
        # # super(self.__class__, self).setup_arguments()
        # workflow.Workflow.setup_arguments(self)

        # TODO get the combinations of mutually exclusive arguments right

        # TODO months and time slices are sort of mutually exclusive

        self.parser.add_argument("--x", help="Range of X index of tiles", action="store", dest="x", type=int,
                                 choices=range(110, 155 + 1), required=True, nargs=2, metavar="110 ... 155")

        self.parser.add_argument("--y", help="Range of Y index of tiles", action="store", dest="y", type=int,
                                 choices=range(-45, -10 + 1), required=True, nargs=2, metavar="-45 ... -10")

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Range of Acquisition Date", action="store", dest="acq", type=str,
                                 nargs=2, default=["1985", "2014"])

        self.parser.add_argument("--epoch", help="Size of year chunks (e.g. 5 means do in 5 chunks of 5 years)",
                                 action="store", dest="epoch", type=int, default=5)

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([ts.name for ts in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=True)

        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask,
                                 default=[PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD],
                                 metavar=" ".join([ts.name for ts in PqaMask]))

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=True)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=16)

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=[DatasetType.ARG25],  # required=True,
                                 default=DatasetType.ARG25,
                                 metavar=" ".join([dt.name for dt in [DatasetType.ARG25]]))

        self.parser.add_argument("--band", help="The band(s) to process", action="store",
                                 default=Ls57Arg25Bands,  # required=True,
                                 dest="bands", type=ls57_arg_band_arg, nargs="+", metavar=" ".join([b.name for b in Ls57Arg25Bands]))

        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

        self.parser.add_argument("--csv", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=True)



    def process_arguments(self, args):

        # # Call method on super class
        # # super(self.__class__, self).process_arguments(args)
        # workflow.Workflow.process_arguments(self, args)

        self.x_min, self.x_max = args.x
        self.y_min, self.y_max = args.y

        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq[0])
        self.acq_max = parse_date_max(args.acq[1])

        self.satellites = args.satellites

        self.epoch = args.epoch

        self.seasons = args.season

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

        self.dataset_type = args.dataset_type
        self.bands = args.bands

        self.output_format = args.output_format

    def log_arguments(self):

        # # Call method on super class
        # # super(self.__class__, self).log_arguments()
        # workflow.Workflow.log_arguments(self)

        _log.info("""
        x = {x_min:03d} to {x_max:03d}
        y = {y_min:04d} to {y_max:04d}
        """.format(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max))

        _log.info("""
        acq = {acq_min} to {acq_max}
        epoch = {epoch}
        satellites = {satellites}
        output directory = {output_directory}
        PQA mask = {pqa_mask}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   epoch=self.epoch and self.epoch or "",
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands = {bands}
        seasons = {seasons}
        """.format(dataset_type=self.dataset_type.name, bands=" ".join([b.name for b in self.bands]),
                   seasons=" ".join([s.name for s in self.seasons])))

        _log.info("""
        output format = {output_format}
        """.format(output_format=self.output_format.name))

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        # luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    BandStatisticsWorkflow().run()



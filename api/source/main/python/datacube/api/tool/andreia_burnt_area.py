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


import argparse
import logging
import sys
from datacube.api import satellite_arg, Satellite, pqa_mask_arg, PqaMask, readable_file, date_arg
from datetime import timedelta


_log = logging.getLogger()


class AndreiaBurntArea(object):

    def __init__(self, name):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.satellites = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.vector_file = None
        self.vector_layer = None
        self.vector_feature = None

        self.burn_date = None

        self.burn_date_prior_days = None
        self.burn_date_post_days = None

        self.clear_pixels = None

    def setup_arguments(self):

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellite",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS8],
                                 metavar=" ".join([s.name for s in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=False)
        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask, default=[PqaMask.PQ_MASK_CLEAR],
                                 metavar=" ".join([s.name for s in PqaMask]))

        self.parser.add_argument("--vector-file", help="Vector file containing AOI definition(s)", action="store",
                                 dest="vector_file", type=readable_file, required=True)

        self.parser.add_argument("--vector-layer", help="Layer Number in Vector File", action="store",
                                 dest="vector_layer", type=int, default=0)

        self.parser.add_argument("--vector-feature", help="Feature ID in Vector File Layer", action="store",
                                 dest="vector_feature", type=int, default=0)

        self.parser.add_argument("--burn-date", help="The date of the burn (YYY-MM-DD)",
                                 action="store", dest="burn_date",
                                 type=date_arg, required=True)

        self.parser.add_argument("--burn-date-prior-days", help="Maximum number of days BEFORE the burn date to look for an image",
                                 action="store", dest="burn_date_prior_days",
                                 type=int, default=30)

        self.parser.add_argument("--burn-date-post-days", help="Maximum number of days AFTER the burn date to look for an image",
                                 action="store", dest="burn_date_post_days",
                                 type=int, default=60)

        self.parser.add_argument("--clear-pixels", help="Minimum percentage of clear pixels required to use an image",
                                 action="store", dest="clear_pixels",
                                 type=int, default=50, choices=range(1,100+1), metavar="[0 - 100]")

    def process_arguments(self, args):

        _log.setLevel(args.log_level)

        self.satellites = args.satellite

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.vector_file = args.vector_file
        self.vector_layer = args.vector_layer
        self.vector_feature = args.vector_feature

        self.burn_date = args.burn_date

        self.burn_date_prior_days = args.burn_date_prior_days
        self.burn_date_post_days = args.burn_date_post_days

        self.clear_pixels = args.clear_pixels

    def log_arguments(self):

        _log.info("""
        satellites = {satellites}
        PQA mask = {pqa_mask}
        vector file = {filename}
        layer ID = {lid}
        feature = {fid}
        burn date = {burn_date}
        min pre burn date = {min_pre_burn_date},
        max post burn date = {max_post_burn_date}
        min clear pixels = {clear_pixels:2d}%
        """.format(satellites=" ".join([satellite.name for satellite in self.satellites]),
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   filename=self.vector_file, lid=self.vector_layer, fid=self.vector_feature,
                   burn_date=self.burn_date,
                   min_pre_burn_date=self.burn_date - timedelta(days=self.burn_date_prior_days),
                   max_post_burn_date=self.burn_date + timedelta(days=self.burn_date_post_days),
                   clear_pixels=self.clear_pixels))

    def go(self):

        # Find the pre-burn acquisition...
        #  - closest to the burn date
        #  - within the specified number of days of the burn date
        #  - with at least the specified amount of clear pixels in the burn shape

        # Find the 3 post-burn acquisitions...
        #  - closest to the burn date
        #  - within the specified number of days of the burn date
        #  - with at least the specified amount of clear pixels in the burn shape

        # Get NBR dataset for the pre and each of the post

        # Calculate delta NBR (form the pre) for each of the 3

        # That's a good start...after that look at classifying and polyginising and stuff...

        pass

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        self.go()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    AndreiaBurntArea("Andreia Burnt Area").run()
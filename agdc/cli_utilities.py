#!/usr/bin/env python

#===============================================================================
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
#===============================================================================

'''
Command Line Interface Utilities. Created to bridge the gap
between the shell scripts that enable access to the GA-DC and
the underlying python API

@author: Josh Vote
'''
from __future__ import absolute_import
import sys
import argparse
import logging

from datetime import datetime, time

ERR_NO_ERROR = 0
ERR_BAD_PARAMS = 2

from .stacker import Stacker

logger = logging.getLogger(__name__)

class CLI_Utilities(Stacker):

    def __init__(self):
        Stacker.__init__(self) # Call inherited constructor

    def command_get_tile_indexes(self, argsList):
        arg_parser = argparse.ArgumentParser(add_help=False, description='get_tile_indexes is used for finding the set of tile indexes that intersect the specified geometry')
        arg_parser.add_argument('--geometry_srid',dest="geometry_srid", default=4326, help='An EPSG spatial reference system ID as an integer to match geometry_wkt (defaults to 4326)')
        arg_parser.add_argument('--file', nargs="?", dest="file", type=argparse.FileType('w'), default=sys.stdout, help="A file to write the results to (default is to print to stdout)")
        arg_parser.add_argument('geometry_wkt', help='A Spatial Well Known Text string defining an area of interest')

        if (argsList is None):
            arg_parser.print_help()
            return ERR_BAD_PARAMS

        args, others =  arg_parser.parse_known_args(argsList)

        intersectingTiles = self.get_intersecting_tiles(args.geometry_wkt, args.geometry_srid)
        for (x, y, id) in intersectingTiles:
            args.file.write('%s %s\n' % (x, y))

        return ERR_NO_ERROR

    # Prints out the list of file paths for a particular temporal stack
    def command_get_tile_paths(self, argsList):
        arg_parser = argparse.ArgumentParser(add_help=False, description='get_tile_paths is used for returning the set of file paths that match a particular stacker query')
        arg_parser.add_argument('-x', '--x_index', dest='x_index', required=True, help='x-index of tile to be stacked')
        arg_parser.add_argument('-y', '--y_index', dest='y_index', required=True, help='y-index of tile to be stacked')
        arg_parser.add_argument('-s', '--start_date', dest='start_date', required=False, default=None, help='Start Date in dd/mm/yyyy format')
        arg_parser.add_argument('-e', '--end_date', dest='end_date', required=False, default=None, help='End Date in dd/mm/yyyy format')
        arg_parser.add_argument('-a', '--satellite', dest='satellite', required=False, default=None, help='Short Satellite name (e.g. LS5, LS7)')
        arg_parser.add_argument('-n', '--sensor', dest='sensor', required=False, default=None, help='Sensor Name (e.g. TM, ETM+)')
        arg_parser.add_argument('-t', '--tile_type', dest='default_tile_type_id', required=False, default=None, help='Tile type ID of tiles to be stacked')
        arg_parser.add_argument('-p', '--path', dest='path', required=False, default=None, help='WRS path of tiles to be stacked')
        arg_parser.add_argument('-r', '--row', dest='row', required=False, default=None, help='WRS row of tiles to be stacked')
        arg_parser.add_argument('-l', '--processing_level', dest='processing_level', required=False, default=None, help='The processing level to specifically request (eg NBAR, ORTHO) defaults to printing everything')
        arg_parser.add_argument('--file', nargs="?", dest="file", type=argparse.FileType('w'), default=sys.stdout, help="A file to write the results to (default is to print to stdout)")

        if (argsList is None):
            arg_parser.print_help()
            return ERR_BAD_PARAMS

        args, others =  arg_parser.parse_known_args(argsList)

        def date2datetime(input_date, time_offset=time.min):
            if not input_date:
                return None
            return datetime.combine(input_date, time_offset)

        #At this point we just reuse the stacker API for creating a stack of tiles
        stack_info_dict = self.stack_tile(x_index=int(args.x_index),
                                     y_index=int(args.y_index),
                                     stack_output_dir=None,
                                     start_datetime=date2datetime(args.start_date, time.min),
                                     end_datetime=date2datetime(args.end_date, time.max),
                                     satellite=args.satellite, sensor=args.sensor,
                                     tile_type_id=args.default_tile_type_id,
                                     path=args.path,
                                     row=args.row,
                                     create_band_stacks=False,
                                     disregard_incomplete_data=False)

        for processing_level in sorted(stack_info_dict.keys()):
            if args.processing_level is None or args.processing_level == processing_level:
                for start_date_time in stack_info_dict[processing_level]:
                    args.file.write('%s\n' %  stack_info_dict[processing_level][start_date_time]['tile_pathname'])

        return ERR_NO_ERROR



def main():
    # Set top level standard output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    if not logger.level:
        logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    utilities = CLI_Utilities()

    command_names = ["get_tile_indexes", "get_tile_paths", "help"]
    arg_parser = argparse.ArgumentParser(add_help=False, description='Command Line Interface utilities for the GA-DC')
    arg_parser.add_argument('command', help='What command to use?', choices=command_names)

    args, options = arg_parser.parse_known_args()
    command = args.command
    if (command == 'help'):
        assert len(options) > 0, 'Specify a command name after help. Valid command names = ' + str(command_names)
        command = options[0]
        options = None

    if (command == 'get_tile_indexes'):
        sys.exit(utilities.command_get_tile_indexes(options))
    elif (command == 'get_tile_paths'):
        sys.exit(utilities.command_get_tile_paths(options))
    else:
        assert False, 'Unknown command: ' + command


if __name__ == '__main__':
    main()
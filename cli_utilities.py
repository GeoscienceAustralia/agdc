#!/usr/bin/env python
'''
Command Line Interface Utilities. Created to bridge the gap
between the shell scripts that enable access to the GA-DC and
the underlying python API

@author: Josh Vote
'''
import os
import sys
import argparse
import logging

ERR_NO_ERROR = 0
ERR_BAD_PARAMS = 2

from datacube import DataCube
            
# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class CLI_Utilities(DataCube):
        
    def __init__(self):
        DataCube.__init__(self) # Call inherited constructor

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
    

if __name__ == '__main__':
    utilities = CLI_Utilities()
    
    arg_parser = argparse.ArgumentParser(add_help=False, description='Command Line Interface utilities for the GA-DC')
    arg_parser.add_argument('command', help='What command to use?', choices=["get_tile_indexes", "help"])
    
    args, options = arg_parser.parse_known_args()
    command = args.command
    if (command == 'help'):
        assert len(options) > 0, 'Specify a command name after help'
        command = args.options[0]
        options = None
        
    
    if (command == 'get_tile_indexes'):
        sys.exit(utilities.command_get_tile_indexes(options))
    else:
        assert False, 'Unknown command: ' + command
    
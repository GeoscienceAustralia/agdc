'''
Created on 16/12/2014

@author: u76345
'''
import os
import sys
import argparse
import logging
import re
import psycopg2
from osgeo import gdal, osr, gdalconst
from copy import copy
from datetime import datetime, time, timedelta
from scipy import ndimage
import numpy
import shutil
from time import sleep

from EOtools.execute import execute
from EOtools.utils import log_multiline

from agdc import DataCube

PQA_CONTIGUITY = 256 # contiguity = bit 8

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                

class TileRemover(DataCube):
    '''
    TileRemover class with methods to report/flag/delete tiles and associated records for specified dataset names
    '''
    action_dict = {'r': 'report', 'f': 'flag', 'd': 'delete'}
    target_dict = {'d': 'dataset', 'v': 'versions', 'a': 'acquisition'}

    def parse_args(self):
        """Parse the command line arguments.
    
        Returns:
            argparse namespace object
        """
        logger.debug('  Calling parse_args()')
    
        _arg_parser = argparse.ArgumentParser('remove_tiles')
        
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(self.agdc_root, 'agdc_default.conf'),
            help='TileRemover configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('-r', '--dryrun', dest='dryrun',
            default=False, action='store_const', const=True,
            help='Dry-run mode flag - nothing changed')
        
        _arg_parser.add_argument('-t', '--target', dest='target',
            default='acquisition',
            help='Specify either Dataset, (all) Versions or (all datasets in) [A]cquisition to report/flag/delete')
        
        _arg_parser.add_argument('-a', '--action', dest='action',
            default='report',
            help='Specify whether to [R]eport, Flag or Delete records/files')
    
        _arg_parser.add_argument('-n', '--name', dest='dataset_name',
            default=None,
            help='Comma-separated list of dataset names to report/flag/delete')
        _arg_parser.add_argument('-l', '--list', dest='dataset_list',
            default=None,
            help='File containing a list of dataset names to report/flag/delete')
       
    
        args, _unknown_args = _arg_parser.parse_known_args()
        return args
        
    def get_field_names(self, table_name, excluded_field_list=[]):
        ''' Return a list containing all field names for the specified table'''
        sql = """select column_name from information_schema.columns where table_name='""" + table_name + """';"""
        log_multiline(logger.debug, sql, 'SQL', '\t')
        self.db_cursor.execute(sql)
            
        field_list = [record[0] for record in self.db_cursor if record[0] not in excluded_field_list]
        log_multiline(logger.debug, field_list, table_name + ' field list', '\t')
        return field_list
    
    def get_satellite_dict(self):
        ''' Return a dict of satellite tags keyed by satellite_id'''
        sql = """select satellite_id, satellite_tag from satellite;"""
        log_multiline(logger.debug, sql, 'SQL', '\t')
        self.db_cursor.execute(sql)
            
        satellite_dict = dict([(record[0], record[1]) for record in self.db_cursor])
        log_multiline(logger.debug, satellite_dict, ' satellite_dict', '\t')
        return satellite_dict
    
    def __init__(self, source_datacube=None, default_tile_type_id=1):
        '''
        Constructor for TileRemover class
        '''
        self.dataset_records = {}
        self.acquisition_records = {}
        self.all_tile_records = {}
        self.tile_records_to_delete = {}
        self.tile_records_to_update = {}

        if source_datacube:
            # Copy values from source_datacube and then override command line args
            self.__dict__ = copy(source_datacube.__dict__)
            
            args = self.parse_args()
            # Set instance attributes for every value in command line arguments file
            for attribute_name in args.__dict__.keys():
                attribute_value = args.__dict__[attribute_name]
                self.__setattr__(attribute_name, attribute_value)

        else:
            DataCube.__init__(self) # Call inherited constructor
            
        if self.debug:
            console_handler.setLevel(logging.DEBUG)
            
        if self.action and type(self.action) == str:
            self.action = TileRemover.action_dict.get(self.action[0].lower()) or 'report'
        else:
            self.action = 'report'
            
        if self.target and type(self.target) == str:
            self.target = TileRemover.target_dict.get(self.target[0].lower()) or 'acquisition'
        else:
            self.target = 'acquisition'
            
        if self.dataset_name: # Dataset list specified at command line
            self.dataset_name_list = self.dataset_name.split(',')
        elif self.dataset_list: # Dataset list file specified
            dataset_list_file = open(self.dataset_list, 'r')
            self.dataset_name_list = [dataset_name.replace('\n', '') for dataset_name in dataset_list_file.readlines()]            
            dataset_list_file.close()
        else:
            raise Exception('No dataset IDs or dataset name list file specified')
        
        assert self.dataset_name_list, 'No dataset names specified'
        self.dataset_name_list = sorted(self.dataset_name_list)
        
        # Only need one cursor - create it here
        self.db_cursor = self.db_connection.cursor()
        
        # Populate field name lists for later use
        self.dataset_field_list = self.get_field_names('dataset', ['xml_text'])
        self.acquisition_field_list = self.get_field_names('acquisition', ['mtl_text'])
        self.tile_field_list = self.get_field_names('tile')
        
        self.satellite_dict = self.get_satellite_dict()
        
        log_multiline(logger.debug, self.__dict__, 'self.__dict__', '\t')
            
    def get_dataset_records(self, dataset_name_list):
        '''Return a nested dict containing all dataset record info for datasets matching specified names keyed by dataset_id'''
        
        dataset_records = {}
        for dataset_name in dataset_name_list:
            if self.target == 'dataset': # Only return exact matches
                match_pattern = '.*/' + dataset_name + '$'
            else: # Return all versions
                #
                match_pattern = '.*/' + re.sub('_(\d){1,3}$', '', dataset_name) + '(_(\d){1,3})*$'
                
            if self.target == 'acquisition':
                sql = """-- Find all datasets derived from acquisition of specified dataset name
select
    """ + \
',\n    '.join(self.dataset_field_list) + \
"""
from dataset
join (
    select distinct acquisition_id from dataset where dataset_path ~ '""" + match_pattern + """'
    ) a using(acquisition_id);"""
            else:
                sql = """-- Find datasets matching provided name
select
    """ + \
',\n    '.join(self.dataset_field_list) + \
"""
from dataset where dataset_path ~ '""" + match_pattern + """';"""
        
            log_multiline(logger.debug, sql, 'SQL', '\t')
            self.db_cursor.execute(sql)
            
            for record in self.db_cursor:
                dataset_records[record[0]] = dict(zip(self.dataset_field_list, record))
            
        log_multiline(logger.debug, dataset_records, 'dataset_records', '\t')
        return dataset_records
    
        
    def get_acquisition_records(self, dataset_records):
        sql = """-- Find all acquisition records for specified datasets
select
    """ + \
',\n    '.join(self.acquisition_field_list) + \
"""
from acquisition where acquisition_id in %(acquisition_id_tuple)s"""
        params = {'acquisition_id_tuple': tuple(sorted(set([dataset_record['acquisition_id'] for dataset_record in dataset_records.values()])))}
        
        log_multiline(logger.debug, self.db_cursor.mogrify(sql, params), 'SQL', '\t')
        self.db_cursor.execute(sql, params)
        
        acquisition_records = {}
        for record in self.db_cursor:
            acquisition_records[record[0]] = dict(zip(self.acquisition_field_list, record))
            
        log_multiline(logger.debug, acquisition_records, 'acquisition_records', '\t')
        
        return acquisition_records
    
    
    def get_tile_records(self, dataset_records):         
        sql = """-- Find tiles and any overlap tiles including those for other datasets
select
    """ + \
',\n    '.join(self.tile_field_list) + \
"""
from tile where dataset_id in %(dataset_id_tuple)s
union
SELECT DISTINCT
    """ + \
',\n    '.join(['o.' + tile_field for tile_field in self.tile_field_list]) + \
"""
FROM tile t
JOIN dataset d USING (dataset_id)
JOIN acquisition a USING (acquisition_id)
JOIN tile o ON
    o.x_index = t.x_index AND
    o.y_index = t.y_index AND
    o.tile_type_id = t.tile_type_id
JOIN dataset od ON
    od.dataset_id = o.dataset_id AND
    od.level_id = d.level_id
JOIN acquisition oa ON
    oa.acquisition_id = od.acquisition_id AND
    oa.satellite_id = a.satellite_id
WHERE
    d.dataset_id in %(dataset_id_tuple)s
    AND (
        (oa.start_datetime BETWEEN
         a.start_datetime - (a.end_datetime - a.start_datetime) / 2.0 AND
         a.end_datetime + (a.end_datetime - a.start_datetime) / 2.0)
     OR
        (oa.end_datetime BETWEEN
         a.start_datetime - (a.end_datetime - a.start_datetime) / 2.0 AND
         a.end_datetime + (a.end_datetime - a.start_datetime) / 2.0)
    );"""
        params = {'dataset_id_tuple': tuple(sorted(set([dataset_record['dataset_id'] for dataset_record in dataset_records.values()])))}
        
        log_multiline(logger.debug, self.db_cursor.mogrify(sql, params), 'SQL', '\t')
        self.db_cursor.execute(sql, params)
        
        tile_records = {}
        for record in self.db_cursor:
            tile_records[record[0]] = dict(zip(self.tile_field_list, record))
            
        log_multiline(logger.debug, tile_records, 'tile_records', '\t')
        
        return tile_records
        
    def get_records(self):
        self.dataset_records = self.get_dataset_records(self.dataset_name_list)
        assert self.dataset_records, 'No matching dataset records found'
        
        self.acquisition_records = self.get_acquisition_records(self.dataset_records)
        self.all_tile_records = self.get_tile_records(self.dataset_records)
 
        # Non-overlapped and mosaic tiles as well as overlapped source tiles from nominated datasets
        self.tile_records_to_delete = dict([(tile_id, self.all_tile_records[tile_id]) for tile_id in self.all_tile_records.keys() if \
                                 (self.all_tile_records[tile_id]['tile_class_id'] in (1, 4, 1001, 1004) or 
                                 (self.all_tile_records[tile_id]['tile_class_id'] in (3, 1003) and 
                                      self.all_tile_records[tile_id]['dataset_id'] in [dataset_record['dataset_id'] for dataset_record in self.dataset_records.values()]))
                                 ])
        
        # Overlapped source tiles NOT from nominated datasets
        self.tile_records_to_update = dict([(tile_id, self.all_tile_records[tile_id]) for tile_id in self.all_tile_records.keys() if \
                                 (self.all_tile_records[tile_id]['tile_class_id'] in (3, 1003) and 
                                      self.all_tile_records[tile_id]['dataset_id'] not in [dataset_record['dataset_id'] for dataset_record in self.dataset_records.values()])
                                 ])
        
    def print_records(self):
        print 'Acquisitions:'
        for satellite, path, row, date in sorted([(self.satellite_dict[acquisition_record['satellite_id']], acquisition_record['x_ref'], acquisition_record['y_ref'], acquisition_record['end_datetime'].strftime('%Y%m%d')) for acquisition_record in self.acquisition_records.values()]):
            print '\tsatellite=%s, path=%d, row=%d, date=%s' % (satellite, path, row, date)
        
        print 'Datasets:'
        for dataset_path in sorted([dataset_record['dataset_path'] for dataset_record in self.dataset_records.values()]):
            print '\t%s' % dataset_path
        
        print 'Tiles to be deleted:'
        # Show non-overlapped and mosaic tiles as well as overlapped source tiles from nominated datasets
        for tile_path in sorted([tile_record['tile_pathname'] for tile_record in self.tile_records_to_delete.values()]):
            print '\t%s' % tile_path
        
        print 'Tiles to be updated:'
        # Show overlapped source tiles NOT from nominated datasets
        for tile_path in sorted([tile_record['tile_pathname'] for tile_record in self.tile_records_to_update.values()]):
            print '\t%s' % tile_path
   
        
    def flag_records(self):
        params = {'tiles_to_be_deleted_tuple': tuple(sorted(self.tile_records_to_delete.keys())),
                  'tiles_to_be_updated_tuple': tuple(sorted(self.tile_records_to_update.keys()))
                  }

        if (params['tiles_to_be_deleted_tuple'] 
            or params['tiles_to_be_updated_tuple']
            ):
        
            sql = ("""-- Change tile class of non-overlapping tiles or overlap source tiles from nominated datasets
update tile
set tile_class_id = tile_class_id + 1000
where tile_class_id < 1000
and tile_id in %(tiles_to_be_deleted_tuple)s;
""" if params['tiles_to_be_deleted_tuple'] else '') + \
("""    
-- Change tile class of overlap source tiles NOT from nominated datasets
update tile
set tile_class_id = 1 -- Change 3->1
where tile_class_id = 3
and tile_id in %(tiles_to_be_updated_tuple)s;
""" if params['tiles_to_be_updated_tuple'] else '')
    
            log_multiline(logger.debug, self.db_cursor.mogrify(sql, params), 'SQL', '\t')
            
            if self.dryrun:
                print '\nDRY RUN ONLY!'
                print 'Tile-flagging SQL:'
                print self.db_cursor.mogrify(sql, params)
                print
            else:
                self.db_cursor.execute(sql, params)
                print 'Records updated successfully'
        else:
            print 'No tiles to delete or modify'
    
    def delete_records(self):
        params = {'tiles_to_be_deleted_tuple': tuple(sorted(self.tile_records_to_delete.keys())),
                  'tiles_to_be_updated_tuple': tuple(sorted(self.tile_records_to_update.keys())),
                  'dataset_tuple': tuple(sorted(self.dataset_records.keys())),
                  'acquisition_tuple': tuple(sorted(self.acquisition_records.keys()))
                  }
        
        if (params['tiles_to_be_deleted_tuple'] 
            or params['tiles_to_be_updated_tuple']
            or params['dataset_tuple']
            or params['acquisition_tuple']
            ):
        
            sql = ("""-- Delete non-overlapping tiles or overlap source tiles from nominated datasets
delete from tile
where tile_id in %(tiles_to_be_deleted_tuple)s;
""" if params['tiles_to_be_deleted_tuple'] else '') + \
("""    
-- Change tile class of overlap source tiles NOT from nominated datasets
update tile
set tile_class_id = 1
where tile_class_id = 3
and tile_id in %(tiles_to_be_updated_tuple)s;
""" if params['tiles_to_be_updated_tuple'] else '') + \
("""
-- Delete datasets
delete from dataset
where dataset_id in %(dataset_tuple)s
and not exists (
    select tile_id
    from tile
    where dataset_id in %(dataset_tuple)s""" + \
("""
    and tile_id not in %(tiles_to_be_deleted_tuple)s
""" if params['tiles_to_be_deleted_tuple'] else '') + \
"""
    );
""" if params['dataset_tuple'] else '') + \
("""
-- Delete acquisitions not shared by other not-nominated datasets
delete from acquisition
where acquisition_id in %(acquisition_tuple)s
and not exists (
    select dataset_id 
    from dataset
    where acquisition_id in %(acquisition_tuple)s
    and dataset_id not in %(dataset_tuple)s
    );
""" if params['dataset_tuple'] else '')    
            log_multiline(logger.debug, self.db_cursor.mogrify(sql, params), 'SQL', '\t')
            
            if self.dryrun:
                print '\nDRY RUN ONLY!'
                print 'Record-deleting SQL:'
                print self.db_cursor.mogrify(sql, params)
                print
                print 'Tile files which would be deleted:'
                for tile_pathname in sorted([tile_record['tile_pathname'] for tile_record in self.tile_records_to_delete.values()]):
                    print '\t%s' % tile_pathname
                print
            else:
                self.db_cursor.execute(sql, params)
                print 'Records deleted/updated successfully'
                self.remove_files(sorted([tile_record['tile_pathname'] for tile_record in self.tile_records_to_delete.values()]))
                print 'Tile files removed successfully'
        else:
            print 'No tiles, datasets or acquisitions to delete or modify'
    
    
def main():
    '''
    Main routine
    '''
    tile_remover = TileRemover()
    
    tile_remover.get_records()
    tile_remover.print_records()
    
    if tile_remover.action == 'flag':
        tile_remover.flag_records()
    elif tile_remover.action == 'delete':
        tile_remover.delete_records()
    
    
if __name__ == '__main__':
    main()
        

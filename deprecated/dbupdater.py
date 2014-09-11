#!/usr/bin/env python

#===============================================================================
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
#===============================================================================

'''
Created on 05/10/2012

@author: Alex Ip
'''
import os
import sys
import argparse
import logging
import re
import shelve
import psycopg2
import time
from copy import copy

from EOtools.execute import execute
from EOtools.utils import log_multiline

import update_dataset_record
import update_pqa_dataset_record
import update_fc_dataset_record

from agdc import DataCube

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
class DBUpdater(DataCube):
    def parse_args(self):
        """Parse the command line arguments.
    
        Returns:
            argparse namespace object
        """
        logger.debug('  Calling parse_args()')
    
        _arg_parser = argparse.ArgumentParser('dbupdater')
        
        # N.B: modtran_root is a direct overrides of config entries
        # and its variable name must be prefixed with "_" to allow lookup in conf file
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(self.agdc_root, 'agdc_default.conf'),
            help='DBUpdater configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('--source', dest='source_dir',
            required=True,
            help='Source root directory containing datasets')
        _arg_parser.add_argument('--refresh', dest='refresh',
            default=False, action='store_const', const=True,
            help='Refresh mode flag to force updating of existing records')
        _arg_parser.add_argument('--purge', dest='purge',
            default=False, action='store_const', const=True,
            help='Purge mode flag to force removal of nonexistent dataset records')
        _arg_parser.add_argument('--removedblist', dest='remove_existing_dblist',
            default=False, action='store_const', const=True,
            help='Delete any pre-existing dataset list from disk')
        _arg_parser.add_argument('--followsymlinks', dest='follow_symbolic_links',
            default=False, action='store_const', const=True,
            help='Follow symbolic links when finding datasets to ingest')
        return _arg_parser.parse_args()
        
    def __init__(self, source_datacube=None, tile_type_id=1):
        """Constructor
        Arguments:
            source_datacube: Optional DataCube object whose connection and data will be shared
            tile_type_id: Optional tile_type_id value (defaults to 1)
        """
        
        if source_datacube:
            # Copy values from source_datacube and then override command line args
            self.__dict__ = copy(source_datacube.__dict__)
            
            args = self.parse_args()
            # Set instance attributes for every value in command line arguments file
            for attribute_name in args.__dict__.keys():
                attribute_value = args.__dict__[attribute_name]
                self.__setattr__(attribute_name, attribute_value)

        else:
            DataCube.__init__(self); # Call inherited constructor        
        self.temp_dir = os.path.join(self.temp_dir, re.sub('^/', '', os.path.abspath(self.source_dir)))
        self.create_directory(self.temp_dir)
        logger.debug('self.temp_dir = %s', self.temp_dir)
            
        if self.debug:
            console_handler.setLevel(logging.DEBUG)
            
    def update_records(self):
        
        def purge_scenes(db_cursor, dataset_root):
            logger.info('Purging all nonexistent datasets in directory "%s"', dataset_root)
            sql = """-- Retrieve all dataset paths
select dataset_id, dataset_path
from dataset
where position(%(dataset_root)s in dataset_path) = 1
order by dataset_path;
"""    
            params = {'dataset_root': dataset_root}
    
            log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')
    
            db_cursor.execute(sql, params)
            
            db_cursor2 = self.db_connection.cursor()
            for row in db_cursor:
                if not os.path.isdir(os.path.join(row[1], 'scene01')):
                    logger.info('Removing dataset record for nonexistent directory "%s"', row[1])
                    sql = """-- Removing %(bad_dataset)s
delete from tile where dataset_id = %(dataset_id)s;
delete from dataset where dataset_id = %(dataset_id)s;

"""    
                    params = {'dataset_id': row[0],
                              'bad_dataset': row[1]}
    
                    log_multiline(logger.debug, db_cursor2.mogrify(sql, params), 'SQL', '\t')
    
                    try:
                        db_cursor2.execute(sql, params)
                        self.db_connection.commit()
                    except Exception, e:
                        logger.warning('Delete operation failed for "%s": %s', sql, e.message)
                        self.db_connection.rollback()

            logger.info('Scene purging completed for %s', dataset_root) 
            
        dataset_list_file = os.path.join(self.temp_dir, 'dataset.list')
        if self.remove_existing_dblist:
            try:
                os.remove(dataset_list_file)
            except:
                pass
        db_cursor = self.db_connection.cursor()
    
        if self.purge:
            # Remove rows for nonexistent files
            purge_scenes(db_cursor, self.source_dir) 

        # Wait for locked file to become unlocked
        unlock_retries = 0
        while os.path.exists(dataset_list_file) and self.check_object_locked(dataset_list_file):
            unlock_retries += 1
            assert unlock_retries > DBUpdater.MAX_RETRIES, 'Timed out waiting for list file %s to be unlocked' % dataset_list_file
            logger.debug('Waiting for locked list file %s to become unlocked',  dataset_list_file)
            time.sleep(DBUpdater.LOCK_WAIT)
            
        if os.path.exists(dataset_list_file):
            logger.info('Loading existing list file %s',  dataset_list_file)
            shelf = shelve.open(dataset_list_file)
            dataset_list = shelf['dataset_list']
            shelf.close()
        else:        
            self.lock_object(dataset_list_file)
            shelf = shelve.open(dataset_list_file)
            logger.info('Creating new list file %s',  dataset_list_file)
            
            # Create master list of datasets
            logger.info('Searching for datasets in %s', self.source_dir)
            if self.follow_symbolic_links:
                command = "find -L %s -name 'scene01' | sort" % self.source_dir
            else:
                command = "find %s -name 'scene01' | sort" % self.source_dir
            logger.debug('executing "%s"', command)
            result = execute(command)
            assert not result['returncode'], '"%s" failed: %s' % (command, result['stderr'])
    
            dataset_list = [os.path.abspath(re.sub('/scene01$', '', scenedir)) for scenedir in result['stdout'].split('\n') if scenedir]
    
            # Save dataset dict for other instances to use
            logger.debug('Saving new dataset list file %s',  dataset_list_file)
#            assert not os.path.getsize(dataset_list_file), 'File %s has already been written to'
            shelf['dataset_list'] = dataset_list
            shelf.close()
            self.unlock_object(dataset_list_file)
            
#            log_multiline(logger.debug, dataset_list, 'dataset_list')

        print 'Not here'
        for dataset_dir in dataset_list:
            if not os.path.isdir(os.path.join(dataset_dir, 'scene01')):
                logger.warning('Skipping nonexistent dataset %s', dataset_dir)
                continue
            
            try:
                if re.search('PQ', dataset_dir):
                    update_pqa_dataset_record.update_dataset_record(dataset_dir, db_cursor, self.refresh, self.debug)
                elif re.search('FC', dataset_dir):
                    update_fc_dataset_record.update_dataset_record(dataset_dir, db_cursor, self.refresh, self.debug)
                else:
                    update_dataset_record.update_dataset_record(dataset_dir, db_cursor, self.refresh, self.debug)
                self.db_connection.commit()
            except Exception, e:
                logger.warning('Database operation failed for %s: %s', dataset_dir, e.message)
                self.db_connection.rollback()
        
#        self.db_connection.close()        
        logger.info('Database updating completed for %s', self.source_dir)
        
                       
if __name__ == '__main__':
    dbupdater = DBUpdater()
    
    dbupdater.update_records()

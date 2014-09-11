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
Script to check whether tile files are valid GDAL datasets

Created on 07/03/2013

@author: u76345
'''
import os
import logging
from osgeo import gdal
from EOtools.utils import log_multiline
from EOtools.execute import execute
import sys
from time import sleep
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
                
class IntegrityChecker(DataCube):
    '''
    classdocs
    '''


    def check_files(self, path_prefix=None, level_name_tuple=None, tile_type_id=1):
        '''
        Function to iterate through all tile records and return a list of invalid paths
        '''
        query_cursor = self.db_connection.cursor()
        check_cursor = self.db_connection.cursor()
        update_cursor = self.db_connection.cursor()
        
        query_sql = """-- Retrieve all tile details for specified tile range
select
  tile_id,
  tile_pathname
from tile
"""

        if level_name_tuple:
            query_sql += """  inner join dataset using(dataset_id)
  inner join acquisition using(acquisition_id)
  inner join processing_level using(level_id)
"""

        query_sql += """where tile_type_id = %(tile_type_id)s
  and tile_class_id = 1 -- Non-empty tile
  and tile_status is null -- Not checked yet        
"""
        
        if level_name_tuple:
                query_sql += """  and level_name in %(level_name_list)s
"""   

        if path_prefix:
                query_sql += """  and tile_pathname like %(path_prefix)s || '%%'
"""   

        query_sql += """order by x_index, y_index, start_datetime
limit 1000 -- Keep the query small and refresh it frequently
"""   

        query_params = {'tile_type_id': tile_type_id,
                  'path_prefix': path_prefix,
                  'level_name_list': level_name_tuple
              }
                      
        log_multiline(logger.debug, query_cursor.mogrify(query_sql, query_params), 'SQL', '\t')
            
        while True:
            while not self.lock_object('integrity check query'):
                sleep(10)
                
            try:
                query_cursor.execute(query_sql, query_params)
            finally:
                self.unlock_object('integrity check query')
                
            if not query_cursor: # Nothing else to process
                break
            
            for record in query_cursor:
                tile_id = record[0]
                tile_pathname = record[1]
                
                check_sql="""-- Check whether tile_status has already been assigned (quick)
select tile_id
from tile
where tile_id = %(tile_id)s
  and tile_type_id = %(tile_type_id)s
  and tile_class_id = 1 -- Non-empty tile
  and tile_status is null -- Not checked yet        
"""
                check_params = {'tile_id': tile_id,
                                'tile_type_id': tile_type_id
                                }
                
                log_multiline(logger.debug, check_cursor.mogrify(check_sql, check_params), 'SQL', '\t')
                check_cursor.execute(check_sql, check_params)
                
                if not check_cursor:
                    continue # Already processed - skip it
                
                if self.lock_object(tile_pathname):
                    tile_status = 0 # Assume OK
                    try:
                        if not os.path.exists(tile_pathname):
                            tile_status = 1 # Doesn't exist
                        else:
                            dataset = gdal.Open(tile_pathname)
                            if dataset:
                                try:
                                    array = dataset.GetRasterBand(dataset.RasterCount).ReadAsArray()
                                    # Everything should be OK at this point
                                except Exception, e:
                                    logger.debug('Tile read failed: ', e.message)
                                    tile_status = 3 # Can't read
                            else:
                                tile_status = 2 # Can't open
                                
                        logger.info('%s status = %d', tile_pathname, tile_status) 
                        
                        update_sql = """update tile 
    set tile_status = %(tile_status)s              
    where tile_id = %(tile_id)s
    """
                        update_params = {'tile_status': tile_status,
                                         'tile_id': tile_id
                                         }
                        log_multiline(logger.debug, update_cursor.mogrify(update_sql, update_params), 'SQL', '\t')
                        update_cursor.execute(update_sql, update_params)
                        self.db_connection.commit()
                    except Exception, e:
                        logger.error(e.message)
                        self.db_connection.rollback()  
                    finally:
                        self.unlock_object(tile_pathname)          


if __name__ == '__main__':
    integrity_checker = IntegrityChecker()
    
    if integrity_checker.debug:
        console_handler.setLevel(logging.DEBUG)
    
    integrity_checker.check_files(level_name_tuple=('NBAR',))
    
        

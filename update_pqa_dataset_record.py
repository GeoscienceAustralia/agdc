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
Sub-module to catalogue PQ dataset (unpackaged) - called by dbupdater.py

N.B: This functionality is now provided by landsat_ingester.py

Created on 05/10/2012

@author: Alex Ip
'''
import os
import sys
import logging
from osgeo import gdal, osr, gdalconst
import re
import psycopg2
from datetime import datetime
from pytz import timezone
from glob import glob

from EOtools import execute
from EOtools.utils import log_multiline


# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)
                
def update_dataset_record(dataset_dir, db_cursor, refresh=True, debug=False):
    if debug:
        console_handler.setLevel(logging.DEBUG)
        
    logger.debug('update_dataset_record(dataset_dir=%s, db_cursor=%s, refresh=%s, debug=%s) called', dataset_dir, db_cursor, refresh, debug)
    
    def get_directory_size(directory):
        command = "du -sk %s | cut -f1" % directory
        logger.debug('executing "%s"', command)
        result = execute(command)
        assert not result['returncode'], '"%s" failed: %s' % (command, result['stderr'])
        
        logger.debug('stdout = %s', result['stdout'])

        return int(result['stdout'])
    
    dataset_dir = os.path.abspath(dataset_dir)
    
    m = re.match('.*(LS\d)_(\w*)_(PQ)_.+_(\d{3})_(\d{3})_(\d{4})(\d{2})(\d{2})$', dataset_dir)
    satellite_tag = m.groups()[0]
    sensor_name = m.groups()[1]
    processing_level = m.groups()[2]
    path = int(m.groups()[3])
    row = int(m.groups()[4])
    date_string = m.groups()[5] + '-' + m.groups()[6] + '-' + m.groups()[7]
    
    dataset_size = get_directory_size(dataset_dir)
    
    datafile = glob(os.path.join(dataset_dir, 'scene01', 'L*.tif'))
    assert datafile, 'No PQA datafile found in %s' % dataset_dir
    datafile = datafile[0]
    # Convert local time to UTC and strip timestamp
    file_mtime = datetime.fromtimestamp(os.path.getmtime(datafile))
    file_mtime = file_mtime.replace(tzinfo=timezone('Australia/ACT'))
    file_mtime = file_mtime.astimezone(timezone('UTC'))
    file_mtime = file_mtime.replace(tzinfo=None)
    
    sql = """-- Get scene values from existing NBAR dataset record
select
  coalesce(pqa.dataset_id, nextval('dataset_id_seq')) as dataset_id,
  acquisition_id, 
  %(dataset_path)s as dataset_path, 
  coalesce(pqa.level_id, (select level_id from processing_level where upper(level_name) like upper(%(level_name)s) || '%%')) as level_id,
  cast(%(datetime_processed)s as timestamp without time zone) as datetime_processed,
  %(dataset_size)s as dataset_size,
  nbar.crs,
  nbar.ll_x,
  nbar.ll_y,
  nbar.lr_x,
  nbar.lr_y,
  nbar.ul_x,
  nbar.ul_y,
  nbar.ur_x,
  nbar.ur_y,
  nbar.x_pixels,
  nbar.y_pixels,
  pqa.dataset_id as pqa_dataset_id
from (select * from acquisition
  where satellite_id = (select satellite_id from satellite where upper(satellite_tag) = upper(%(satellite_tag)s))
    and sensor_id = (select sensor_id from sensor inner join satellite using(satellite_id) 
      where upper(satellite_tag) = upper(%(satellite_tag)s) and upper(sensor_name) like upper(%(sensor_name)s) || '%%')
    and x_ref = %(x_ref)s
    and y_ref = %(y_ref)s
    and start_datetime between cast(%(date_string)s || ' 00:00:00' as timestamp without time zone)
      and cast(%(date_string)s || ' 23:59:59.999' as timestamp without time zone)
  ) acquisition
inner join (select * from dataset where level_id = 2) nbar using(acquisition_id)
left join (select * from dataset where level_id = 3
  and dataset_path = %(dataset_path)s) pqa using (acquisition_id)
"""
    
    params = {'satellite_tag': satellite_tag,
        'sensor_name': sensor_name, 
        'x_ref': path, 
        'y_ref': row, 
        'dataset_path': dataset_dir,
        'level_name': processing_level,
        'datetime_processed': file_mtime,
        'dataset_size': dataset_size,
        'date_string': date_string
        }
    
    log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')    
    db_cursor.execute(sql, params)
    
    result = db_cursor.fetchone()
    assert result, 'NBAR dataset not found for PQA dataset %s' % dataset_dir
    
    try:
        xml_path = glob(os.path.join(dataset_dir, 'metadata.xml'))[0]
        xml_file = open(xml_path)
        xml_text = xml_file.read()
        xml_file.close() 
    except IndexError: # No XML file exists
        logger.debug('No metadata.xml file found')
        xml_text = None
    
    params = {'dataset_id': result[0], 
        'acquisition_id': result[1], 
        'dataset_path': result[2],
        'level_id': result[3],
        'datetime_processed': result[4],
        'dataset_size': result[5],
        'crs': result[6],
        'll_x': result[7],
        'll_y': result[8],
        'lr_x': result[9],
        'lr_y': result[10],
        'ul_x': result[11],
        'ul_y': result[12],
        'ur_x': result[13],
        'ur_y': result[14],
        'x_pixels': result[15],
        'y_pixels': result[16],
        'pqa_dataset_id': result[17],
        'xml_text': xml_text
        }
    
    if params['pqa_dataset_id']: # PQA record already exists
        if refresh:
            logger.info('Updating existing record for %s', dataset_dir)
            sql = """-- Update any values in dataset record not used to find record
update dataset 
set
  datetime_processed = %(datetime_processed)s,
  dataset_size = %(dataset_size)s,
  crs = %(crs)s,
  ll_x = %(ll_x)s,
  ll_y = %(ll_y)s,
  lr_x = %(lr_x)s,
  lr_y = %(lr_y)s,
  ul_x = %(ul_x)s,
  ul_y = %(ul_y)s,
  ur_x = %(ur_x)s,
  ur_y = %(ur_y)s,
  x_pixels = %(x_pixels)s,
  y_pixels = %(y_pixels)s,
  xml_text = %(xml_text)s
where dataset_id = %(dataset_id)s
"""
        else:
            logger.info('Skipping existing record for %s', dataset_dir)
            return
    else: # Record doesn't already exist - insert it
        logger.info('Creating new record for %s', dataset_dir)
        sql = """-- Create new dataset record - acquisition record should already exist for nbar dataset
insert into dataset(
  dataset_id, 
  acquisition_id, 
  dataset_path, 
  level_id,
  datetime_processed,
  dataset_size,
  crs,
  ll_x,
  ll_y,
  lr_x,
  lr_y,
  ul_x,
  ul_y,
  ur_x,
  ur_y,
  x_pixels,
  y_pixels,
  xml_text
  )
values (
  %(dataset_id)s,
  %(acquisition_id)s,
  %(dataset_path)s,
  %(level_id)s,
  %(datetime_processed)s,
  %(dataset_size)s,
  %(crs)s,
  %(ll_x)s,
  %(ll_y)s,
  %(lr_x)s,
  %(lr_y)s,
  %(ul_x)s,
  %(ul_y)s,
  %(ur_x)s,
  %(ur_y)s,
  %(x_pixels)s,
  %(y_pixels)s,
  %(xml_text)s
)
"""        
    log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')    
    db_cursor.execute(sql, params)

    

    
    
     
    

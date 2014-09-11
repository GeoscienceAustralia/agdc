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
Sub-module to catalogue NBAR dataset - called by dbupdater.py

N.B: This functionality is now provided by landsat_ingester.py

Created on 05/10/2012

@author: Alex Ip
'''
import os
import sys
from glob import glob
import re
import logging
from osgeo import gdal, osr, gdalconst
from scipy import ndimage
import numpy
import numpy.ma as ma
import psycopg2

from EOtools.DatasetDrivers import SceneDataset
from EOtools.execute import execute
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
                
DTYPE_MAP = {
    # numpy dtype: GDAL dtype
    numpy.dtype('uint8'):   gdal.GDT_Byte,
    numpy.dtype('uint16'):  gdal.GDT_UInt16,
    numpy.dtype('int16'):   gdal.GDT_Int16,
    numpy.dtype('float32'): gdal.GDT_Float32,
    numpy.dtype('float64'): gdal.GDT_Float64
}

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
    
    dataset = SceneDataset(default_metadata_required=False, utm_fix=True)
    assert dataset.Open(dataset_dir), 'Unable to open %s' % dataset_dir
    
    dataset_size = get_directory_size(dataset_dir)
    
    gcp_count = None
    mtl_text = None
    if dataset.processor_level.upper() in ['ORTHO', 'L1T', 'MAP']:  
        logger.debug('Dataset %s is Level 1', dataset_dir)      
        try:
            gcp_path = glob(os.path.join(dataset_dir, 'scene01', '*_GCP.txt'))[0]
            
            gcp_file = open(gcp_path)
            # Count the number of lines consisting of 8 numbers with the first number being positive
            gcp_count = len([line for line in gcp_file.readlines() if re.match('\d+(\s+-?\d+\.?\d*){7}', line)])
            gcp_file.close()    
        except IndexError: # No GCP file exists
            logger.debug('No GCP.txt file found')
        
        try:
            mtl_path = glob(os.path.join(dataset_dir, 'scene01', '*_MTL.txt'))[0]
            
            mtl_file = open(mtl_path)
            mtl_text = mtl_file.read()
            mtl_file.close()                
        except IndexError: # No MTL file exists
            logger.debug('No MTL.txt file found')
        
    try:
        xml_path = glob(os.path.join(dataset_dir, 'metadata.xml'))[0]
        xml_file = open(xml_path)
        xml_text = xml_file.read()
        xml_file.close() 
    except IndexError: # No XML file exists
        logger.debug('No metadata.xml file found')
        xml_text = None
    
                   
    sql = """-- Find dataset_id and acquisition_id for given path
select dataset_id, acquisition_id
from dataset 
inner join acquisition using(acquisition_id)
where dataset_path = %s
"""
    db_cursor.execute(sql, (dataset_dir,))
    result = db_cursor.fetchone()
    if result: # Record already exists
        if refresh:
            logger.info('Updating existing record for %s', dataset_dir)
            dataset_id = result[0]
            acquisition_id = result[1]
            
            sql = """
insert into processing_level(level_id, level_name)
select nextval('level_id_seq'), upper(%(level_name)s)
where not exists (select level_id from processing_level where level_name = upper(%(level_name)s));

-- Update existing acquisition record if required
update acquisition
  set gcp_count = %(gcp_count)s
  where acquisition_id = %(acquisition_id)s
  and %(gcp_count)s is not null;
        
update acquisition
  set mtl_text = %(mtl_text)s
  where acquisition_id = %(acquisition_id)s
  and %(mtl_text)s is not null;
        
update acquisition
  set cloud_cover = %(cloud_cover)s
  where acquisition_id = %(acquisition_id)s
  and %(cloud_cover)s is not null;
        
update dataset 
  set level_id = (select level_id from processing_level where upper(level_name) = upper(%(processing_level)s)),
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
    else: # Record doesn't already exist
        logger.info('Creating new record for %s', dataset_dir)
        dataset_id = None       
        acquisition_id = None       
                
        sql = """-- Create new processing level record if needed
insert into processing_level(level_id, level_name)
select nextval('level_id_seq'), upper(%(level_name)s)
where not exists (select level_id from processing_level where level_name = upper(%(level_name)s));
        
-- Create new acquisition record if needed
insert into acquisition(
  acquisition_id,
  satellite_id, 
  sensor_id, 
  x_ref, 
  y_ref, 
  start_datetime, 
  end_datetime, 
  ll_lon,
  ll_lat,
  lr_lon,
  lr_lat,
  ul_lon,
  ul_lat,
  ur_lon,
  ur_lat"""
  
        if gcp_count is not None:
            sql += """,
  gcp_count"""
    
        if mtl_text is not None:
            sql += """,
  mtl_text"""
    
        sql += """
  )
select
  nextval('acquisition_id_seq'),
  (select satellite_id from satellite where upper(satellite_tag) = upper(%(satellite_tag)s)),
  (select sensor_id from sensor inner join satellite using(satellite_id) 
    where upper(satellite_tag) = upper(%(satellite_tag)s) and upper(sensor_name) = upper(%(sensor_name)s)),
  %(x_ref)s,
  %(y_ref)s,
  %(start_datetime)s,
  %(end_datetime)s,
  %(ll_lon)s,
  %(ll_lat)s,
  %(lr_lon)s,
  %(lr_lat)s,
  %(ul_lon)s,
  %(ul_lat)s,
  %(ur_lon)s,
  %(ur_lat)s"""
  
        if gcp_count is not None:
            sql += """,
  %(gcp_count)s"""
    
        if mtl_text is not None:
            sql += """,
  %(mtl_text)s"""
    
        sql += """
where not exists
  (select acquisition_id 
    from acquisition 
    where satellite_id = (select satellite_id
      from satellite 
      where upper(satellite_tag) = upper(%(satellite_tag)s)
      )
      and sensor_id = (select sensor_id 
        from sensor 
        inner join satellite using(satellite_id) 
        where upper(satellite_tag) = upper(%(satellite_tag)s)
          and upper(sensor_name) = upper(%(sensor_name)s)
      ) 
    and x_ref = %(x_ref)s 
    and y_ref = %(y_ref)s 
    and start_datetime = %(start_datetime)s 
    and end_datetime = %(end_datetime)s
    );

-- Create new dataset record
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
select
  nextval('dataset_id_seq') as dataset_id,
  (select acquisition_id 
    from acquisition 
    where satellite_id = (select satellite_id from satellite where upper(satellite_tag) = upper(%(satellite_tag)s))
      and sensor_id = (select sensor_id from sensor inner join satellite using(satellite_id) 
        where upper(satellite_tag) = upper(%(satellite_tag)s)
          and upper(sensor_name) = upper(%(sensor_name)s)) 
      and x_ref = %(x_ref)s 
      and y_ref = %(y_ref)s 
      and start_datetime = %(start_datetime)s 
      and end_datetime = %(end_datetime)s
    ) as acquisition_id,
  %(dataset_path)s,
  (select level_id from processing_level where upper(level_name) = upper(%(processing_level)s)),
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
where not exists
  (select dataset_id
  from dataset
  where dataset_path = %(dataset_path)s
  )

;
"""
    # same params for insert or update
    params = {'acquisition_id': acquisition_id,
        'dataset_id': dataset_id, 
        'satellite_tag': dataset.satellite.TAG, 
        'sensor_name': dataset.satellite.sensor, 
        'x_ref': dataset.path_number, 
        'y_ref': dataset.row_number, 
        'start_datetime': dataset.scene_start_datetime, 
        'end_datetime': dataset.scene_end_datetime, 
        'dataset_path': dataset_dir,
        'processing_level': dataset.processor_level,
        'datetime_processed': dataset.completion_datetime,
        'dataset_size': dataset_size,
        'level_name': dataset.processor_level.upper(),
        'll_lon': dataset.ll_lon,
        'll_lat': dataset.ll_lat,
        'lr_lon': dataset.lr_lon,
        'lr_lat': dataset.lr_lat,
        'ul_lon': dataset.ul_lon,
        'ul_lat': dataset.ul_lat,
        'ur_lon': dataset.ur_lon,
        'ur_lat': dataset.ur_lat,
        'crs': dataset.GetProjection(),
        'll_x': dataset.ll_x,
        'll_y': dataset.ll_y,
        'lr_x': dataset.lr_x,
        'lr_y': dataset.lr_y,
        'ul_x': dataset.ul_x,
        'ul_y': dataset.ul_y,
        'ur_x': dataset.ur_x,
        'ur_y': dataset.ur_y,
        'x_pixels': dataset.image_pixels,
        'y_pixels': dataset.image_lines,
        'gcp_count': gcp_count,
        'mtl_text': mtl_text,
        'cloud_cover': dataset.cloud_cover_percentage,
        'xml_text': xml_text
        }
    
    log_multiline(logger.debug, db_cursor.mogrify(sql, params), 'SQL', '\t')    
    db_cursor.execute(sql, params)
    
    

    
    
     
    

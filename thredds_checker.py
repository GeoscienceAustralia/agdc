'''
Created on 25/09/2013

@author: u76345
'''

import xml.dom.minidom
import argparse
from datetime import datetime
import logging, os, re, copy

from datacube import DataCube
from ULA3.utils import log_multiline

#===============================================================================
# # Set top level standard output 
# console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(logging.INFO)
# console_formatter = logging.Formatter('%(message)s')
# console_handler.setFormatter(console_formatter)
#===============================================================================

logger = logging.getLogger('datacube.' + __name__)

class ThreddsChecker(DataCube):
    '''
    classdocs
    '''
    def parse_args(self):
        """Parse the command line arguments.
    
        Returns:
            argparse namespace object
        """
        logger.debug('  Calling parse_args()')
    
        _arg_parser = argparse.ArgumentParser('stacker')
        
        # N.B: modtran_root is a direct overrides of config entries
        # and its variable name must be prefixed with "_" to allow lookup in conf file
        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
            help='Stacker configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('-s', '--start_date', dest='start_date',
            required=False, default=None,
            help='Start Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-e', '--end_date', dest='end_date',
            required=False, default=None,
            help='End Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-a', '--satellite', dest='satellite',
            required=False, default=None,
            help='Short Satellite name (e.g. LS5, LS7)')
        _arg_parser.add_argument('-n', '--sensor', dest='sensor',
            required=False, default=None,
            help='Sensor Name (e.g. TM, ETM+)')
    
        return _arg_parser.parse_args()
        
    def __init__(self, source_datacube=None, default_tile_type_id=1):
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
            DataCube.__init__(self) # Call inherited constructor
            
        # Attempt to parse dates from command line arguments or config file
        try:
            self.start_date = datetime.strptime(self.start_date, '%Y%m%d').date()
        except:
            try:
                self.start_date = datetime.strptime(self.start_date, '%d/%m/%Y').date()
            except:
                try:
                    self.start_date = datetime.strptime(self.start_date, '%Y-%m-%d').date()
                except:
                    self.start_date= None      
                          
        try:
            self.end_date = datetime.strptime(self.end_date, '%Y%m%d').date()
        except:
            try:
                self.end_date = datetime.strptime(self.end_date, '%d/%m/%Y').date()
            except:
                try:
                    self.end_date = datetime.strptime(self.end_date, '%Y-%m-%d').date()
                except:
                    self.end_date= None            
          

        # Other variables set from config file only - not used
        try:
            self.min_path = int(self.min_path) 
        except:
            self.min_path = None
        try:
            self.max_path = int(self.max_path) 
        except:
            self.max_path = None
        try:
            self.min_row = int(self.min_row) 
        except:
            self.min_row = None
        try:
            self.max_row = int(self.max_row) 
        except:
            self.max_row = None
            
        self.thredds_root = '/projects/v27/EOS_delivery/LANDSAT'
    
    def check(self, kml_filename=None, wrs_shapefile='WRS-2_bound_world.kml'):
        '''
        check a KML file
        '''
        self.db_cursor = self.db_connection.cursor()
        
        sql = """-- Find all NBAR acquisitions
select satellite_name as satellite, sensor_name as sensor, 
x_ref as path, y_ref as row, 
start_datetime, end_datetime,
dataset_path,
ll_lon, ll_lat,
lr_lon, lr_lat,
ul_lon, ul_lat,
ur_lon, ur_lat,
cloud_cover::integer, gcp_count::integer
from 
    (
    select *
    from dataset
    where level_id = 2 -- NBAR
    ) dataset
inner join acquisition a using(acquisition_id)
inner join satellite using(satellite_id)
inner join sensor using(satellite_id, sensor_id)

where (%(start_date)s is null or end_datetime::date >= %(start_date)s)
  and (%(end_date)s is null or end_datetime::date <= %(end_date)s)
  and (%(satellite)s is null or satellite_tag = %(satellite)s)
  and (%(sensor)s is null or sensor_name = %(sensor)s)

order by end_datetime
;
"""
        params = {
                  'start_date': self.start_date,
                  'end_date': self.end_date,
                  'satellite': self.satellite,
                  'sensor': self.sensor
                  }
        
        log_multiline(logger.debug, self.db_cursor.mogrify(sql, params), 'SQL', '\t')
        self.db_cursor.execute(sql, params)
        
        field_list = ['satellite',
                      'sensor', 
                      'path',
                      'row', 
                      'start_datetime', 
                      'end_datetime',
                      'dataset_path',
                      'll_lon',
                      'll_lat',
                      'lr_lon',
                      'lr_lat',
                      'ul_lon',
                      'ul_lat',
                      'ur_lon',
                      'ur_lat',
                      'cloud_cover', 
                      'gcp_count'
                      ]
        
        for record in self.db_cursor:
            
            acquisition_info = {}
            for field_index in range(len(field_list)):
                acquisition_info[field_list[field_index]] = record[field_index]
                
            acquisition_info['year'] = acquisition_info['end_datetime'].year    
            acquisition_info['month'] = acquisition_info['end_datetime'].month   
            acquisition_info['dataset_name'] = re.search('[^/]+$', acquisition_info['dataset_path']).group(0) 
            
            log_multiline(logger.debug, acquisition_info, 'acquisition_info', '\t')
            
            thredds_dataset = '%s/%04d/%02d/%s_BX.nc' % (self.thredds_root, acquisition_info['year'], acquisition_info['month'], acquisition_info['dataset_name'])
            #===================================================================
            # if os.path.exists(thredds_dataset):
            #     print '%s exists' % (acquisition_info['dataset_name'])
            # else:
            #     print '%s does not exist' % (acquisition_info['dataset_name'])
            #===================================================================
            if not os.path.exists(thredds_dataset):
                print acquisition_info['dataset_path']   
        
def main():
        
    tc = ThreddsChecker()
    tc.check()
    
if __name__ == '__main__':
    main() 
    

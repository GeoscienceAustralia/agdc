'''
Created on 09/09/2014

@author: u76345
'''
import sys, logging
from EOtools.utils import log_multiline
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
                
class BandLookup(object):
    '''
    CLass BandLookup manages band equivalence
    '''

    _band_lookup_dict = {} # Class lookup dict - populated once from query
        
    # Sets of allowable values - will be populated from query
    _lookup_scheme_name_set = set()
    _satellite_tag_set = set()
    _sensor_name_set = set()
    _band_tag_set = set()
    
    def __init__(self, data_cube, lookup_scheme_name=None, satellite_tag=None, sensor_name=None):
        '''
        Constructor
        '''
        assert type(data_cube) == DataCube, 'data_cube parameter must be of type DataCube'
        assert not lookup_scheme_name or type(lookup_scheme_name) == str, 'lookup_scheme_name parameter must be of type str'
        assert not satellite_tag or type(satellite_tag) == str, 'satellite_tag parameter must be of type str'
        assert not sensor_name or type(sensor_name) == str, 'sensor_name parameter must be of type str'
        
        # Set instance values if provided as constructor parameters
        self.lookup_scheme_name = lookup_scheme_name
        self.satellite_tag = satellite_tag
        self.sensor_name = sensor_name
        
        self.db_connection = data_cube.db_connection
        db_cursor = self.db_connection.cursor()
        
        if not BandLookup._band_lookup_dict: # Check whether class lookup dict has been populated
        
            sql = """-- Retrieve all band equivalence information
 SELECT distinct
    band_lookup_scheme.lookup_scheme_name,
    coalesce(satellite.satellite_tag, 'DERIVED') as satellite_tag,
    coalesce(sensor_name, level_name) as sensor_name,
    band_equivalent.master_band_tag,
    band_source.tile_layer,
    band_equivalent.nominal_centre,
    band_equivalent.nominal_bandwidth,
    band_equivalent.centre_tolerance,
    band_equivalent.bandwidth_tolerance,
    COALESCE(band_adjustment.adjustment_offset, 0.0) AS adjustment_offset,
    COALESCE(band_adjustment.adjustment_multiplier, 1.0) AS adjustment_multiplier,
    band_lookup_scheme.lookup_scheme_id,
    band.satellite_id,
    band.sensor_id,
    band.band_id,
    band_lookup_scheme.lookup_scheme_description,
    band_equivalent.master_band_name,
    band.min_wavelength,
    band.max_wavelength
   FROM band
   JOIN band_type using(band_type_id)
   JOIN band_source using (band_id)
   JOIN processing_level using(level_id)
   JOIN band_equivalent ON abs((band.max_wavelength::numeric + band.min_wavelength::numeric) / 2.0 - band_equivalent.nominal_centre) <= band_equivalent.centre_tolerance AND abs(band.max_wavelength::numeric - band.min_wavelength::numeric - band_equivalent.nominal_bandwidth) <= band_equivalent.bandwidth_tolerance
   JOIN band_lookup_scheme USING (lookup_scheme_id)
   LEFT JOIN band_adjustment USING (lookup_scheme_id, band_id)
   LEFT JOIN sensor using(satellite_id, sensor_id)
   LEFT JOIN satellite using(satellite_id)   
   ORDER BY 1,2,3,5,6
""" 
            log_multiline(logger.debug, sql, 'SQL', '\t')
            db_cursor.execute(sql)
            
            for record in db_cursor:
                # Add values to their respective sets of allowable values
                BandLookup._lookup_scheme_name_set |= {record[0]}
                BandLookup._satellite_tag_set |= {record[1]}
                BandLookup._sensor_name_set |= {record[2]}
                BandLookup._band_tag_set |= {record[3]}
                
                # Create dict items keyed by tuple of (lookup_scheme_name, satellite_tag, sensor_name, master_band_tag)
                BandLookup._band_lookup_dict[(record[0], record[1], record[2], record[3])] = {
                                                                                             'tile_layer': record[4],
                                                                                             'nominal_centre': record[5],
                                                                                             'nominal_bandwidth': record[6],
                                                                                             'centre_tolerance': record[7],
                                                                                             'bandwidth_tolerance': record[8],
                                                                                             'adjustment_offset': record[9],
                                                                                             'adjustment_multiplier': record[10],
                                                                                             'lookup_scheme_id': record[11],
                                                                                             'satellite_id': record[12],
                                                                                             'sensor_id': record[13],
                                                                                             'band_id': record[14],
                                                                                             'lookup_scheme_description': record[15],
                                                                                             'master_band_name': record[16],
                                                                                             'min_wavelength': record[17],
                                                                                             'max_wavelength': record[18]
                                                                                  }
    def get_band_dict(self, band_tag):
        '''
        Returns band dict for provided band_tag using pre-set lookup_scheme_name, satellite_tag & sensor_name
        Returns None if not found
        '''
        assert self.lookup_scheme_name, 'lookup_scheme_name not set'
        assert self.satellite_tag, 'satellite_tag not set'
        assert self.sensor_name, 'sensor_name not set'
        assert band_tag in BandLookup._band_tag_set, 'band_tag not recognised'
        
        band_dict = BandLookup._band_lookup_dict.get((self.lookup_scheme_name,
                                                 self.satellite_tag,
                                                 self.sensor_name,
                                                 band_tag
                                                 ))
        return band_dict
 
    def get_tile_layer(self, band_tag):
        '''
        Returns tile layer integer for provided band_tag using pre-set lookup_scheme_name, satellite_tag & sensor_name.
        Returns None if not found
        '''
        band_dict = self.get_band_dict(band_tag)
        if band_dict:
            return band_dict['tile_layer']
        else:
            return None
                
    def get_adjustments(self, band_tag):
        '''
        Returns adjustment factors for provided band_tag using pre-set lookup_scheme_name, satellite_tag & sensor_name.
        Returns None if not found
        '''
        band_dict = self.get_band_dict(band_tag)
        if band_dict:
            return {'adjustment_offset': band_dict['adjustment_offset'],
                    'adjustment_multiplier': band_dict['adjustment_multiplier']
                    }
        else:
            return None
                
    @property
    def lookup_scheme_name_set(self):
        """Set of allowable lookup_scheme_name values"""
        return BandLookup._lookup_scheme_name_set
                
    @property
    def satellite_tag_set(self):
        """Set of allowable satellite_tag values"""
        return BandLookup._satellite_tag_set
                
    @property
    def sensor_name_set(self):
        """Set of allowable sensor_name values"""
        return BandLookup._sensor_name_set
                
    @property
    def band_tag_set(self):
        """Set of allowable band_tag values"""
        return BandLookup._band_tag_set

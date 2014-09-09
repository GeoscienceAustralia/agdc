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
    _lookup_schemes = {} # Dict containing the descriptions of all available lookup schemes
        
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
    band_equivalent.nominal_centre::float,
    band_equivalent.nominal_bandwidth::float,
    band_equivalent.centre_tolerance::float,
    band_equivalent.bandwidth_tolerance::float,
    COALESCE(band_adjustment.adjustment_offset, 0.0)::float AS adjustment_offset,
    COALESCE(band_adjustment.adjustment_multiplier, 1.0)::float AS adjustment_multiplier,
    band_lookup_scheme.lookup_scheme_id,
    band.satellite_id,
    band.sensor_id,
    band.band_id,
    band_equivalent.master_band_name,
    band.min_wavelength::float,
    band.max_wavelength::float,
    band_lookup_scheme.lookup_scheme_description
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
                # Create nested dict with levels keyed by lookup_scheme_name, satellite_tag, sensor_name, band_tag
                lookup_scheme_dict = BandLookup._band_lookup_dict.get(record[0])
                if lookup_scheme_dict is None:
                    lookup_scheme_dict = {}
                    BandLookup._band_lookup_dict[record[0]] = lookup_scheme_dict
                    BandLookup._lookup_schemes[record[0]] = record[18] # Set lookup scheme description
                    
                satellite_tag_dict = lookup_scheme_dict.get(record[1])
                if satellite_tag_dict is None:
                    satellite_tag_dict = {}
                    lookup_scheme_dict[record[1]] = satellite_tag_dict
                    
                sensor_name_dict = satellite_tag_dict.get(record[2])
                if sensor_name_dict is None:
                    sensor_name_dict = {}
                    satellite_tag_dict[record[2]] = sensor_name_dict
                    
                assert sensor_name_dict.get(record[3]) is None, 'Duplicated band_tag record'

                sensor_name_dict[record[3]] = {
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
                                 'master_band_name': record[15],
                                 'min_wavelength': record[16],
                                 'max_wavelength': record[17]
                                 }
                    
    def _get_sensor_name_dict(self):
        '''
        Returns sensor_name_dict for pre-set lookup_scheme_name, satellite_tag & sensor_name
        Returns None if not found
        '''
        assert self.lookup_scheme_name, 'lookup_scheme_name not set'
        assert self.satellite_tag, 'satellite_tag not set'
        assert self.sensor_name, 'sensor_name not set'
        
        try:
            sensor_name_dict = BandLookup._band_lookup_dict[self.lookup_scheme_name][self.satellite_tag][self.sensor_name]
        except KeyError:
            sensor_name_dict = {}
            
        return sensor_name_dict
                
    @property
    def lookup_schemes(self):
        '''
        Returns a dict of lookup_scheme descriptions keyed by lookup_scheme_name
        '''
        return dict(BandLookup._lookup_schemes)

    @property
    def bands(self):
        '''
        Returns a list of band tags for the current lookup_scheme_name, satellite_tag & sensor_name sorted by centre wavelength
        '''
        sensor_name_dict = self._get_sensor_name_dict()
        return sorted([band_tag for band_tag in sensor_name_dict.keys()], 
                      key=lambda band_tag: sensor_name_dict[band_tag]['nominal_centre'])

    @property
    def band_info(self):
        '''
        Returns a nested dict keyed by band tag containing all info for each band for the current lookup_scheme_name, satellite_tag & sensor_name 
        '''
        sensor_name_dict = self._get_sensor_name_dict()
        return dict(sensor_name_dict)

    @property
    def band_no(self):
        '''
        Returns a dict keyed by band tag containing the integer band number for each band_tag for the current lookup_scheme_name, satellite_tag & sensor_name 
        '''
        sensor_name_dict = self._get_sensor_name_dict()
        return {band_tag: sensor_name_dict[band_tag]['tile_layer'] for band_tag in sensor_name_dict}

    @property
    def adjustment_offset(self):
        '''
        Returns a dict keyed by band tag containing the floating point adjustment offset for each band_tag for the current lookup_scheme_name, satellite_tag & sensor_name 
        '''
        sensor_name_dict = self._get_sensor_name_dict()
        return {band_tag: sensor_name_dict[band_tag]['adjustment_offset'] for band_tag in sensor_name_dict}

    @property
    def adjustment_multiplier(self):
        '''
        Returns a dict keyed by band tag containing the floating point adjustment multiplier for each band_tag for the current lookup_scheme_name, satellite_tag & sensor_name 
        '''
        sensor_name_dict = self._get_sensor_name_dict()
        return {band_tag: sensor_name_dict[band_tag]['adjustment_multiplier'] for band_tag in sensor_name_dict}


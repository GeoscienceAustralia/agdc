'''
Created on 09/09/2014

@author: u76345
'''
import sys, logging
from EOtools.utils import log_multiline
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
                
class BandLookup(object):
    '''
    Class BandLookup manages band equivalence with band_tag lookups for a given set of
    lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name values
    '''

    _band_lookup_dict = {} # Class lookup dict - populated once from query
    _lookup_schemes = {} # Dict containing the descriptions of all available lookup schemes
        
    def __init__(self, 
                 data_cube, 
                 lookup_scheme_name=None, 
                 tile_type_id=1, # Should this be None?
                 satellite_tag=None, 
                 sensor_name=None,
                 level_name=None):
        '''
        Constructor for BandLookup class
        Parameters (can all be set later with the exception of data_cube):
             data_cube: Parent data_cube (or descendant) object 
             lookup_scheme_name: lookup scheme name. Needs to be a member of self.lookup_schemes 
             tile_type_id: Tile Type identifier. Defaults to 1 - should this be None?
             satellite_tag: Short name of satellite 
             sensor_name: Name of sensor
             level_name: Processing level name
        '''
        assert isinstance(data_cube, DataCube), 'data_cube parameter must be of type DataCube'
        assert not lookup_scheme_name or type(lookup_scheme_name) == str, 'lookup_scheme_name parameter must be of type str'
        assert not tile_type_id or type(tile_type_id) in (long, int), 'tile_type_id parameter must be of type long or int'
        assert not satellite_tag or type(satellite_tag) == str, 'satellite_tag parameter must be of type str'
        assert not sensor_name or type(sensor_name) == str, 'sensor_name parameter must be of type str'
        assert not level_name or type(level_name) == str, 'level_name parameter must be of type str'
        
        if data_cube.debug:
            console_handler.setLevel(logging.DEBUG)

        # Set instance values if provided as constructor parameters
        self.lookup_scheme_name = lookup_scheme_name
        self.tile_type_id = tile_type_id
        self.satellite_tag = satellite_tag
        self.sensor_name = sensor_name
        self.level_name = level_name
        
        self.db_connection = data_cube.db_connection
        db_cursor = self.db_connection.cursor()
        
        if not BandLookup._band_lookup_dict: # Check whether class lookup dict has been populated
        
            sql = """-- Retrieve all band equivalence information
 SELECT
    band_lookup_scheme.lookup_scheme_name,
    band_source.tile_type_id,
    coalesce(satellite.satellite_tag, 'DERIVED') as satellite_tag,
    coalesce(sensor_name, level_name) as sensor_name,
    processing_level.level_name,
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
    band_type_name,
    band.min_wavelength::float,
    band.max_wavelength::float,
    band_lookup_scheme.lookup_scheme_description
   FROM band
   JOIN band_type using(band_type_id)
   JOIN band_source using (band_id)
   JOIN processing_level using(level_id)
   JOIN band_equivalent ON band_equivalent.band_type_id = band.band_type_id
     and abs((band.max_wavelength::numeric + band.min_wavelength::numeric) / 2.0 - band_equivalent.nominal_centre) <= band_equivalent.centre_tolerance 
     AND abs(band.max_wavelength::numeric - band.min_wavelength::numeric - band_equivalent.nominal_bandwidth) <= band_equivalent.bandwidth_tolerance
   JOIN band_lookup_scheme USING (lookup_scheme_id)
   LEFT JOIN band_adjustment USING (lookup_scheme_id, band_id)
   LEFT JOIN sensor using(satellite_id, sensor_id)
   LEFT JOIN satellite using(satellite_id)
   ORDER BY 1,2,3,4,5,7
""" 
            log_multiline(logger.debug, sql, 'SQL', '\t')
            db_cursor.execute(sql)
            
            for record in db_cursor:
                # Create nested dict with levels keyed by:
                # lookup_scheme_name, tile_type_id, satellite_tag, sensor_name, level_name, band_tag
                lookup_scheme_dict = BandLookup._band_lookup_dict.get(record[0])
                if lookup_scheme_dict is None:
                    lookup_scheme_dict = {}
                    BandLookup._band_lookup_dict[record[0]] = lookup_scheme_dict
                    BandLookup._lookup_schemes[record[0]] = record[21] # Set lookup scheme description
                    
                tile_type_id_dict = lookup_scheme_dict.get(record[1])
                if tile_type_id_dict is None:
                    tile_type_id_dict = {}
                    lookup_scheme_dict[record[1]] = tile_type_id_dict
                    
                satellite_tag_dict = tile_type_id_dict.get(record[2])
                if satellite_tag_dict is None:
                    satellite_tag_dict = {}
                    tile_type_id_dict[record[2]] = satellite_tag_dict
                    
                sensor_name_dict = satellite_tag_dict.get(record[3])
                if sensor_name_dict is None:
                    sensor_name_dict = {}
                    satellite_tag_dict[record[3]] = sensor_name_dict
                    
                level_name_dict = sensor_name_dict.get(record[4])
                if level_name_dict is None:
                    level_name_dict = {}
                    sensor_name_dict[record[4]] = level_name_dict
                    
                assert level_name_dict.get(record[5]) is None, 'Duplicated band_tag record'

                level_name_dict[record[5]] = {
                                 'tile_layer': record[6],
                                 'nominal_centre': record[7],
                                 'nominal_bandwidth': record[8],
                                 'centre_tolerance': record[9],
                                 'bandwidth_tolerance': record[10],
                                 'adjustment_offset': record[11],
                                 'adjustment_multiplier': record[12],
                                 'lookup_scheme_id': record[13],
                                 'satellite_id': record[14],
                                 'sensor_id': record[15],
                                 'band_id': record[16],
                                 'master_band_name': record[17],
                                 'band_type_name': record[18],
                                 'min_wavelength': record[19],
                                 'max_wavelength': record[20]
                                 }
                
            log_multiline(logger.debug, BandLookup._band_lookup_dict, 'BandLookup._band_lookup_dict', '\t')
                    
    def _get_level_name_dict(self):
        '''
        Returns level_name_dict for pre-set lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name
        Returns None if not found
        '''
        assert self.lookup_scheme_name, 'lookup_scheme_name not set'
        assert self.tile_type_id, 'tile_type_id not set'
        assert self.satellite_tag, 'satellite_tag not set'
        assert self.sensor_name, 'sensor_name not set'
        assert self.level_name, 'level_name not set'
        
        try:
            level_name_dict = BandLookup._band_lookup_dict[self.lookup_scheme_name][self.tile_type_id][self.satellite_tag][self.sensor_name][self.level_name]
        except KeyError:
            level_name_dict = {}
            
        return level_name_dict
                
    @property
    def lookup_schemes(self):
        '''
        Returns a dict of available lookup_scheme descriptions keyed by lookup_scheme_name
        '''
        return dict(BandLookup._lookup_schemes)

    @property
    def bands(self):
        '''
        Returns a list of band tags for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name sorted by centre wavelength
        '''
        level_name_dict = self._get_level_name_dict()
        return sorted([band_tag for band_tag in level_name_dict.keys()], 
                      key=lambda band_tag: level_name_dict[band_tag]['nominal_centre'])

    @property
    def band_info(self):
        '''
        Returns a nested dict keyed by band tag containing all info for each band for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name 
        '''
        level_name_dict = self._get_level_name_dict()
        return dict(level_name_dict)

    @property
    def band_no(self):
        '''
        Returns a dict keyed by band tag containing the one-based integer band number for each band_tag for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name
        '''
        level_name_dict = self._get_level_name_dict()
        return {band_tag: level_name_dict[band_tag]['tile_layer'] for band_tag in level_name_dict}

    @property
    def band_index(self):
        '''
        Returns a dict keyed by band tag containing the zero-based integer band number for each band_tag for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name
        '''
        level_name_dict = self._get_level_name_dict()
        return {band_tag: level_name_dict[band_tag]['tile_layer'] - 1 for band_tag in level_name_dict}

    @property
    def adjustment_offset(self):
        '''
        Returns a dict keyed by band tag containing the floating point adjustment offset for each band_tag for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name
        '''
        level_name_dict = self._get_level_name_dict()
        return {band_tag: level_name_dict[band_tag]['adjustment_offset'] for band_tag in level_name_dict}

    @property
    def adjustment_multiplier(self):
        '''
        Returns a dict keyed by band tag containing the floating point adjustment multiplier for each band_tag for the current lookup_scheme_name, tile_type_id, satellite_tag, sensor_name & level_name
        '''
        level_name_dict = self._get_level_name_dict()
        return {band_tag: level_name_dict[band_tag]['adjustment_multiplier'] for band_tag in level_name_dict}

    @property
    def band_lookup_dict(self):
        """
        Returns a copy of the class value _band_lookup_dict
        """
        return dict(BandLookup._band_lookup_dict)
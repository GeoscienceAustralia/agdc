#!/usr/bin/env python
"""Utility functions for parsing and conversion of dataset ids.

Initial support for old (without ground station id) and new variants of 
L1T, NBAR, PQ and FC names following the GA spec.

"""

import os.path
import re
import datetime


# EXAMPLE PATHS
# L1T  : /g/data/v10/L1/2008-12/LS7_ETM_OTH_P51_GALPGS01-011_077_092_20081229
# NBAR : /g/data/v10/nbar-product/2007-01/LS7_ETM_NBAR_P54_GANBAR01-002_100_077_20070111
# PQ   : /g/data/v10/PQ/2008-12/LS7_ETM_PQ_P55_GAPQ01-002_115_079_20081224
# FC   : /g/data/v10/PQ/2008-12/LS7_ETM_FC_P54_GAFC01-002_115_079_20081224


L1T = 'L1T'
NBAR = 'NBAR'
PQ = 'PQ'
FC = 'FC'

NAME_INTERNALS = {
    L1T  : { 'TAG': 'OTH',  'LEVEL': 'P51', 'LABEL': 'GALPGS01' },
    NBAR : { 'TAG': 'NBAR', 'LEVEL': 'P54', 'LABEL': 'GANBAR01' },
    PQ   : { 'TAG': 'PQ',   'LEVEL': 'P55', 'LABEL': 'GAPQ01' },
    FC   : { 'TAG': 'FC',   'LEVEL': 'P54', 'LABEL': 'GAFC01' },
}

RE_SATELLITE =  '(?P<satellite>\w+)'
RE_SENSOR    =  '(?P<sensor>\w+)'
RE_TAG       =  '(?P<tag>\w+)'
RE_PROCLEVEL =  '(?P<plevel>\w+)'
RE_SPEC_STN  =  '(?P<spec>\w+)-(?P<station>\d{3})'
RE_SPEC      =  '(?P<spec>\w+)'
RE_PATH      =  '(?P<path>\d{3})'
RE_ROW       =  '(?P<row>\d{3})'
RE_DATE      =  '(?P<date>\d{8})'


# Patterns to match any dataset name string

NAME_PATTERNS = {
    'NEW': '_'.join([RE_SATELLITE, RE_SENSOR, RE_TAG, RE_PROCLEVEL, RE_SPEC_STN, RE_PATH, RE_ROW, RE_DATE]),
    'OLD': '_'.join([RE_SATELLITE, RE_SENSOR, RE_TAG, RE_PROCLEVEL, RE_SPEC    , RE_PATH, RE_ROW, RE_DATE]),
}

# Regex name components, except TAG entry for string subst.
# NOT USED -- commented out

#REGEX_COMPONENTS = {
#    'NEW': [ RE_SATELLITE, RE_SENSOR, '%(TAG)s', RE_PROCLEVEL, RE_SPEC_STN, RE_PATH, RE_ROW, RE_DATE ],
#    'OLD': [ RE_SATELLITE, RE_SENSOR, '%(TAG)s', RE_PROCLEVEL, RE_SPEC    , RE_PATH, RE_ROW, RE_DATE ],
#}

# Format statement components, except TAG, LEVEL, LABEL entries for string subst.
# '@@foo$$' will be replaced with '%(foo)s' when compiled into a format statment.

T_SATELLITE = '@@satellite$$'
T_SENSOR    = '@@sensor$$'
T_STN       = '@@station$$'
T_PATH      = '@@path$$'
T_ROW       = '@@row$$'
T_DATE      = '@@date$$'

FORMAT_COMPONENTS = {
    'NEW': [
        T_SATELLITE, T_SENSOR, 
        '%(TAG)s',                  # str
        '%(LEVEL)s',                # str
        '%(LABEL)s' + '-' + T_STN,  # str
        T_PATH, T_ROW, T_DATE
    ],
    'OLD': [
        T_SATELLITE, T_SENSOR, 
        '%(TAG)s',              # str
        '%(LEVEL)s',            # str
        '%(LABEL)s',            # str
        T_PATH, T_ROW, T_DATE
    ],
}



def match(path=None, variant='NEW'):
    __, _name = os.path.split(path)
    return re.search(NAME_PATTERNS[variant], _name)


def get_component(path, key=None, variant='NEW'):
    m = match(path, variant)
    try:
        return m.groupdict()[key]
    except AttributeError:
        # no match, m=None
        return None
    except KeyError:
        return None


def get_date(path):
    ds = get_component(path, key='date')
    assert len(ds) == 8
    y = int(ds[0:4])
    m = int(ds[4:6])
    d = int(ds[6:8])
    return datetime.date(y, m, d)


def _replace_format_tokens(s):
    return s.replace('@@', '%(').replace('$$', ')s')


def convert(path, target_key, variant='NEW'):
    m = match(path)
    if m:
        fmt = '_'.join(FORMAT_COMPONENTS[variant]) % NAME_INTERNALS[target_key]
        return _replace_format_tokens(fmt) % m.groupdict()
    return None

     



if __name__ == '__main__':

    import unittest

    class DatasetIDConversionTest(unittest.TestCase):

        def setUp(self):
            self.test_paths = {
                L1T  : '/foo/bar/LS7_ETM_OTH_P51_GALPGS01-002_100_077_20070111',
                NBAR : '/foo/bar/LS7_ETM_NBAR_P54_GANBAR01-002_100_077_20070111',
                PQ   : '/foo/bar/LS7_ETM_PQ_P55_GAPQ01-002_100_077_20070111',
                FC   : '/foo/bar/LS7_ETM_FC_P54_GAFC01-002_100_077_20070111',
            }

        def test_match_func(self):
            for k, path in self.test_paths.iteritems():
                self.assertNotEquals(match(path), None)
                self.assertNotEquals(match(os.path.split(path)[1]), None)

        def test_match_regex(self):
            for k, path in self.test_paths.iteritems():
                m = match(path)
                self.assertNotEquals(m, None)
                md = m.groupdict()
                self.assertEquals(md['satellite'], 'LS7')
                self.assertEquals(md['sensor'], 'ETM')
                self.assertEquals(md['station'], '002')
                self.assertEquals(md['path'], '100')
                self.assertEquals(md['row'], '077')
                self.assertEquals(md['date'], '20070111')

        def test_token_replacement(self):
            self.assertEqual(_replace_format_tokens('@@foo$$ $$@@'), '%(foo)s )s%(')

        def _test_conversion(self, src, target):
            _d, _name = os.path.split(self.test_paths[target])
            self.assertEquals(convert(self.test_paths[src], target), _name)
            sname = os.path.split(self.test_paths[src])[1]
            self.assertEquals(convert(sname, target), _name)

        def test_L1T_to_NBAR(self):
            self._test_conversion(L1T, NBAR)

        def test_NBAR_to_PQ(self):
            self._test_conversion(NBAR, PQ)

        def test_NBAR_to_L1T(self):
            self._test_conversion(NBAR, L1T)

        def test_L1T_to_PQ(self):
            self._test_conversion(L1T, PQ)

        def test_PQ_to_PQ(self):
            self._test_conversion(PQ, PQ)

        def test_L1T_to_L1T(self):
            self._test_conversion(L1T, L1T)

        def test_NBAR_to_NBAR(self):
            self._test_conversion(NBAR, NBAR)

        def test_NBAR_to_FC(self):
            self._test_conversion(NBAR, FC)

        def test_x_L1T_no_station(self):
            path = '/g/data/v10/L1/2008-12/LS7_ETM_OTH_P51_GALPGS01_077_092_20081229'
            self.assertEquals(match(path), None)
            self.assertEquals(get_component(path, 'station'), None)
            self.assertNotEquals(match(path, variant='OLD'), None)
            self.assertEquals(get_component(path, 'station', variant='OLD'), None)


    unittest.main()



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
Script to generate KML file of nominal scene footprints for each NBAR scene in DB

Created on 25/09/2013

@author: u76345
'''

import xml.dom.minidom
import argparse
from datetime import datetime
import logging, os, re, copy

from agdc import DataCube
from EOtools.utils import log_multiline

#===============================================================================
# # Set top level standard output 
# console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(logging.INFO)
# console_formatter = logging.Formatter('%(message)s')
# console_handler.setFormatter(console_formatter)
#===============================================================================

logger = logging.getLogger('datacube.' + __name__)

class SceneKMLGenerator(DataCube):
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
            default=os.path.join(self.agdc_root, 'agdc_default.conf'),
            help='Stacker configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')
        _arg_parser.add_argument('-o', '--output', dest='output_file',
            required=False, default=1,
            help='Output file path')
        _arg_parser.add_argument('-s', '--start_date', dest='start_date',
            required=False, default=None,
            help='Start Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-e', '--end_date', dest='end_date',
            required=False, default=None,
            help='End Date in dd/mm/yyyy format')
        _arg_parser.add_argument('-a', '--satellite', dest='satellite',
            required=False, default=None,
            help='Short Satellite name (e.g. LS5, LS7)')
        _arg_parser.add_argument('-t', '--thumbnail', dest='thumbnail_size',
            required=False, default=512,
            help='Thumbnail side length in pixels')
        _arg_parser.add_argument('-n', '--sensor', dest='sensor',
            required=False, default=None,
            help='Sensor Name (e.g. TM, ETM+)')
    
        return _arg_parser.parse_args()
        
    def getChildNodesByName(self, node, nodeName):
        return [child_node for child_node in node.childNodes if child_node.nodeName == nodeName]

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
          

        try:
            self.thumbnail_size = int(self.thumbnail_size)
        except:
            self.thumbnail_size = 512
            
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
            
        self.style_dict = {
#                           'IconStyle': {'scale': 0.4, 'Icon': {'href': 'http://maps.google.com/mapfiles/kml/shapes/star.png'}},
                           'LabelStyle': {'color': '9900ffff', 'scale': 1},
                           'LineStyle': {'color': '990000ff', 'width': 2},
                           'PolyStyle': {'color': '997f7fff', 'fill': 1, 'outline': 1}
                          }
    
    def generate(self, kml_filename=None, wrs_shapefile='WRS-2_bound_world.kml'):
        '''
        Generate a KML file
        '''
        def write_xml_file(filename, dom_tree, save_backup=False):
            """Function write the metadata contained in self._metadata_dict to an XML file
            Argument:
                filename: Metadata file to be written
                uses_attributes: Boolean flag indicating whether to write values to tag attributes
            """
            logger.debug('write_file(%s) called', filename)
    
            if save_backup and os.path.exists(filename + '.bck'):
                os.remove(filename + '.bck')
    
            if os.path.exists(filename):
                if save_backup:
                    os.rename(filename, filename + '.bck')
                else:
                    os.remove(filename)
    
            # Open XML document
            try:
                outfile = open(filename, 'w')
                assert outfile is not None, 'Unable to open XML file ' + filename + ' for writing'
    
                logger.debug('Writing XML file %s', filename)
    
                # Strip all tabs and EOLs from around values, remove all empty lines
                outfile.write(re.sub('\>(\s+)(\n\t*)\<',
                                     '>\\2<',
                                     re.sub('(\<\w*[^/]\>)\n(\t*\n)*(\t*)([^<>\n]*)\n\t*\n*(\t+)(\</\w+\>)',
                                            '\\1\\4\\6',
                                            dom_tree.toprettyxml(encoding='utf-8')
                                            )
                                     )
                              )
    
            finally:
                outfile.close()
    
        def get_wrs_placemark_node(wrs_document_node, placemark_name):
            """
            Return a clone of the WRS placemark node with the specified name
            """ 
            try:
                return [placemark_node for placemark_node in self.getChildNodesByName(wrs_document_node, 'Placemark') 
                    if self.getChildNodesByName(placemark_node, 'name')[0].childNodes[0].nodeValue == placemark_name][0].cloneNode(True)
            except:
                return None
                

        
        def create_placemark_node(wrs_document_node, acquisition_info):
            """
            Create a new placemark node for the specified acquisition
            """
            logger.info('Processing %s', acquisition_info['dataset_name'])
            
            wrs_placemark_name = '%d_%d' % (acquisition_info['path'], acquisition_info['row'])
            
            kml_placemark_name = acquisition_info['dataset_name']
            
            placemark_node = get_wrs_placemark_node(wrs_document_node, wrs_placemark_name)
            
            self.getChildNodesByName(placemark_node, 'name')[0].childNodes[0].nodeValue = kml_placemark_name
            
            kml_time_span_node = kml_dom_tree.createElement('TimeSpan')
            placemark_node.appendChild(kml_time_span_node)
            
            kml_time_begin_node = kml_dom_tree.createElement('begin')
            kml_time_begin_text_node = kml_dom_tree.createTextNode(acquisition_info['start_datetime'].isoformat())
            kml_time_begin_node.appendChild(kml_time_begin_text_node)
            kml_time_span_node.appendChild(kml_time_begin_node)
            
            kml_time_end_node = kml_dom_tree.createElement('end')
            kml_time_end_text_node = kml_dom_tree.createTextNode(acquisition_info['end_datetime'].isoformat())
            kml_time_end_node.appendChild(kml_time_end_text_node)
            kml_time_span_node.appendChild(kml_time_end_node)
            
            description_node = self.getChildNodesByName(placemark_node, 'description')[0]
            description_node.childNodes[0].data = '''<strong>Geoscience Australia ARG25 Dataset</strong> 
<table cellspacing="1" cellpadding="1">
    <tr>
        <td>Satellite:</td>
        <td>%(satellite)s</td>
    </tr>
    <tr>
        <td>Sensor:</td>
        <td>%(sensor)s</td>
    </tr>
    <tr>
        <td>Start date/time (UTC):</td>
        <td>%(start_datetime)s</td>
    </tr>
    <tr>
        <td>End date/time (UTC):</td>
        <td>%(end_datetime)s</td>
    </tr>
    <tr>
        <td>WRS Path-Row:</td>
        <td>%(path)03d-%(row)03d</td>
    </tr>
    <tr>
        <td>Bounding Box (LL,UR):</td>
        <td>(%(ll_lon)f,%(lr_lat)f),(%(ur_lon)f,%(ul_lat)f)</td>
    </tr>
    <tr>
        <td>Est. Cloud Cover (USGS):</td>
        <td>%(cloud_cover)s%%</td>
    </tr>
    <tr>
        <td>GCP Count:</td>
        <td>%(gcp_count)s</td>
    </tr>
    <tr>
        <td>
            <a href="http://eos.ga.gov.au/thredds/wms/LANDSAT/%(year)04d/%(month)02d/%(dataset_name)s_BX.nc?REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=FalseColour741&STYLES=&FORMAT=image/png&TRANSPARENT=TRUE&CRS=CRS:84&BBOX=%(ll_lon)f,%(lr_lat)f,%(ur_lon)f,%(ul_lat)f&WIDTH=%(thumbnail_size)d&HEIGHT=%(thumbnail_size)d">View thumbnail</a>
        </td>
        <td>
            <a href="http://eos.ga.gov.au/thredds/fileServer/LANDSAT/%(year)04d/%(month)02d/%(dataset_name)s_BX.nc">Download full NetCDF file</a>
        </td>
    </tr>
</table>''' % acquisition_info
            
            return placemark_node
            
            
        kml_filename = kml_filename or self.output_file
        assert kml_filename, 'Output filename must be specified'
        
        wrs_dom_tree = xml.dom.minidom.parse(wrs_shapefile)
        wrs_document_element = wrs_dom_tree.documentElement
        wrs_document_node = self.getChildNodesByName(wrs_document_element, 'Document')[0]
        
        kml_dom_tree = xml.dom.minidom.getDOMImplementation().createDocument(wrs_document_element.namespaceURI, 
                                                                             'kml', 
                                                                             wrs_dom_tree.doctype)
        kml_document_element = kml_dom_tree.documentElement
        
        # Copy document attributes
        for attribute_value in wrs_document_element.attributes.items():
            kml_document_element.setAttribute(attribute_value[0], attribute_value[1])
            
        kml_document_node = kml_dom_tree.createElement('Document')
        kml_document_element.appendChild(kml_document_node)
        
        
        # Copy all child nodes of the "Document" node except placemarks
        for wrs_child_node in [child_node for child_node in wrs_document_node.childNodes 
                               if child_node.nodeName != 'Placemark']:
                                   
            kml_child_node = kml_dom_tree.importNode(wrs_child_node, True)                                   
            kml_document_node.appendChild(kml_child_node)
            
        # Update document name 
        doc_name = 'Geoscience Australia ARG-25 Landsat Scenes'
        if self.satellite or self.sensor:
            doc_name += ' for'
            if self.satellite:
                doc_name += ' %s' % self.satellite
            if self.sensor:
                doc_name += ' %s' % self.sensor
        if self.start_date:
            doc_name += ' from %s' % self.start_date
        if self.end_date:
            doc_name += ' to %s' % self.end_date
            
        logger.debug('Setting document name to "%s"', doc_name)
        self.getChildNodesByName(kml_document_node, 'name')[0].childNodes[0].data = doc_name
         
        # Update style nodes as specified in self.style_dict
        for style_node in self.getChildNodesByName(kml_document_node, 'Style'):
            logger.debug('Style node found')
            for tag_name in self.style_dict.keys():
                tag_nodes = self.getChildNodesByName(style_node, tag_name)
                if tag_nodes:
                    logger.debug('\tExisting tag node found for %s', tag_name)
                    tag_node = tag_nodes[0]
                else:
                    logger.debug('\tCreating new tag node for %s', tag_name)
                    tag_node = kml_dom_tree.createElement(tag_name)
                    style_node.appendChild(tag_node)
                    
                for attribute_name in self.style_dict[tag_name].keys():
                    attribute_nodes = self.getChildNodesByName(tag_node, attribute_name)
                    if attribute_nodes:
                        logger.debug('\t\tExisting attribute node found for %s', attribute_name)
                        attribute_node = attribute_nodes[0]
                        text_node = attribute_node.childNodes[0]
                        text_node.data = str(self.style_dict[tag_name][attribute_name])
                    else:
                        logger.debug('\t\tCreating new attribute node for %s', attribute_name)
                        attribute_node = kml_dom_tree.createElement(attribute_name)
                        tag_node.appendChild(attribute_node)
                        text_node = kml_dom_tree.createTextNode(str(self.style_dict[tag_name][attribute_name]))
                        attribute_node.appendChild(text_node)
    
           
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
            acquisition_info['thumbnail_size'] = self.thumbnail_size   
            acquisition_info['dataset_name'] = re.search('[^/]+$', acquisition_info['dataset_path']).group(0)
            
            log_multiline(logger.debug, acquisition_info, 'acquisition_info', '\t')
                
            placemark_node = create_placemark_node(wrs_document_node, acquisition_info)
            kml_document_node.appendChild(placemark_node)
            
        logger.info('Writing KML to %s', kml_filename)
        write_xml_file(kml_filename, kml_dom_tree)
        
def main():
        
    skg = SceneKMLGenerator()
    assert skg.output_file, 'No output file specified'
    skg.generate()
    
if __name__ == '__main__':
    main() 
    

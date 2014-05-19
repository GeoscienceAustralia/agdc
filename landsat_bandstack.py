"""
    abstract_bandstack.py - interface for the bandstack class.

    Different types of dataset will have different versions of this
    class, obtained by sub-classing and overriding the abstract methods.

    It is the responsibility of the stack_bands method of the dataset
    object to instantiate the correct subclass.
"""
import os
import re
from osgeo import gdal
from abstract_bandstack import AbstractBandstack
from abstract_ingester import DatasetError
import cube_util

class LandsatBandstack(AbstractBandstack):
    """Landsat subclass of AbstractBandstack class"""
    def __init__(self, band_dict, dataset_metadata_dict):
        self.band_dict = band_dict
        self.dataset_mdd = dataset_metadata_dict
        self.source_file_list = None
        self.nodata_list = None
        self.vrt_name = None
        self.vrt_band_stack = None
    def buildvrt(self, temp_dir):
        """Given a dataset_record and corresponding dataset, build the vrt that
        will be used to reproject the dataset's data to tile coordinates"""
        #Make the list of filenames from the dataset_path/scene01 and each
        #file_number's file_pattern. Also get list of nodata_value.
        self.source_file_list, self.nodata_list = self.list_source_files()
        nodata_value = self.nodata_list[0]
        #TODO: check that this works for PQA where nodata_value is None
        if nodata_value is not None:
            nodata_spec = "-srcnodata %d -vrtnodata %d" %(nodata_value,
                                                          nodata_value)
        else:
            nodata_spec = ""
        #Form the vrt_band_stack_filename
        vrt_band_stack_filename = self.get_vrt_name(temp_dir)
        #build the vrt
        buildvrt_cmd = ["gdalbuildvrt -separate",
                        "-q",
                        nodata_spec,
                        "-overwrite %s %s" %(vrt_band_stack_filename,
                                             ' '.join(self.source_file_list))
                        ]
        result = cube_util.execute(buildvrt_cmd)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s'\
                                   % (buildvrt_cmd, result['stderr']))
        #Add the metadata and return the band_stack as a gdal datatset, storing
        #as an attribute of the Bandstack object
        self.vrt_band_stack = self.add_metadata(vrt_band_stack_filename)

    def list_source_files(self):
        """Given the nested dictionary of band source information, form a list
        of scene file names from which a vrt can be constructed"""
        dataset_dir = os.path.join(self.dataset_mdd['dataset_path'], 'scene01')
        file_list = []
        nodata_list = []
        for file_number in self.band_dict:
            pattern = self.band_dict[file_number]['file_pattern']
            this_file = self.find_file(dataset_dir, pattern)
            file_list.append(this_file)
            nodata_list.append(self.band_dict[file_number]['nodata_value'])
        return (file_list, nodata_list)

    @staticmethod
    def find_file(dataset_dir, file_pattern):
        """Find the file in dataset_dir matching file_pattern and check
        uniqueness"""
        assert os.path.isdir(dataset_dir), '%s is not a valid directory' \
            % dataset_dir
        filelist = [filename for filename in os.listdir(dataset_dir)
                    if re.match(file_pattern, filename)]
        assert len(filelist) == 1, \
            'Unable to find unique match for file pattern %s' % file_pattern
        return os.path.join(dataset_dir, filelist[0])

    def get_vrt_name(self, vrt_dir):
        """Use the dataset's metadata to form the vrt file name"""
        satellite = self.dataset_mdd['satellite_tag'].upper()
        sensor = self.dataset_mdd['sensor_name'].upper()
        start_datetime = \
            self.dataset_mdd['start_datetime'].date().strftime('%Y%m%d')
        x_ref = self.dataset_mdd['x_ref']
        y_ref = self.dataset_mdd['y_ref']
        vrt_band_stack_basename = '%s_%s_%s_%s_%s.vrt' \
            %(satellite, sensor, start_datetime, x_ref, y_ref)
        return os.path.join(vrt_dir, vrt_band_stack_basename)

    def add_metadata(self, vrt_filename):
        """Add metadata and return the open band_stack"""
        band_stack_dataset = gdal.Open(vrt_filename)
        assert band_stack_dataset, 'Unable to open VRT %s' % vrt_filename
        band_stack_dataset.SetMetadata(
            {'satellite': self.dataset_mdd['satellite_tag'].upper(),
             'sensor':  self.dataset_mdd['sensor_name'].upper(),
             'start_datetime': self.dataset_mdd['start_datetime'].isoformat(),
             'end_datetime': self.dataset_mdd['end_datetime'].isoformat(),
             'path': '%03d' % self.dataset_mdd['x_ref'],
             'row': '%03d' % self.dataset_mdd['y_ref']}
            )
        for band_index in range(len(self.band_dict)):
            band = band_stack_dataset.GetRasterBand(band_index + 1)
            band.SetMetadata({'name': self.band_dict[band_index]['band_name'],
                              'filename': self.source_file_list[band_index]})
            if self.nodata_list[band_index] is not None:
                band.SetNoDataValue(self.nodata_list[band_index])
        return band_stack_dataset



















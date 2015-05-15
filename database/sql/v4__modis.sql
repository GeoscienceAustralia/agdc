
-- The Modis codebase uses orbit number as 'x_ref', and so will overflow smallint data type.
alter table acquisition
    alter column x_ref type integer,
    alter column y_ref type integer;

insert into satellite (satellite_id, satellite_name, satellite_tag, name_pattern, semi_major_axis, radius, altitude, inclination, omega, sweep_period, format, projection, spectral_filter_file, tle_format, solar_irrad_file, nominal_pixel_degrees)
values (10, 'Terra', 'MT', null, 7077700, null, 70500, 1.71389761553675002, null, null, null, null, null, null, null, null);


insert into sensor (sensor_id, satellite_id, sensor_name, description)
values (10, 10, 'MODIS-Terra', 'MODIS-Terra Sensor');

insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (2, 'Unprojected WGS84 10-degree at 400 pixels/degree (for MODIS 250m)', 'EPSG:4326', 0, 0, 10, 10, 4000, 4000, 'degree', 'GTiff', '.tif', 'MODIS', 'COMPRESS=LZW');
insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (3, 'Unprojected WGS84 10-degree at 200 pixels/degree (for MODIS 500m)', 'EPSG:4326', 0, 0, 10, 10, 2000, 2000, 'degree', 'GTiff', '.tif', 'MODIS', 'COMPRESS=LZW');
insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (4, 'Unprojected WGS84 10-degree at 100 pixels/degree (for MODIS 1km)', 'EPSG:4326', 0, 0, 10, 10, 1000, 1000, 'degree', 'GTiff', '.tif', 'MODIS', 'COMPRESS=LZW');


insert into band_type (band_type_id, band_type_name) values (5, 'emitted');
insert into band_type (band_type_id, band_type_name) values (31, 'RBQ');


insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (207, 10, 'Modis Surface Reflectance Band 8', 1, null, 500, 0.405000000000000027, 0.419999999999999984, null, 10, 'MB8', 8);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (208, 10, 'Modis Surface Reflectance Band 9', 1, null, 500, 0.438, 0.448000000000000009, null, 10, 'MB9', 9);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (209, 10, 'Modis Surface Reflectance Band 10', 1, null, 500, 0.482999999999999985, 0.492999999999999994, null, 10, 'MB10', 10);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (210, 10, 'Modis Surface Reflectance Band 11', 1, null, 500, 0.526000000000000023, 0.536000000000000032, null, 10, 'MB11', 11);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (203, 10, 'Modis Surface Reflectance Band 4', 1, 204, 500, 0.54500000000000004, 0.564999999999999947, null, 10, 'MB4', 4);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (205, 10, 'Modis Surface Reflectance Band 6', 1, 206, 500, 1.62799999999999989, 1.65199999999999991, null, 10, 'MB6', 6);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (206, 10, 'Modis Surface Reflectance Band 7', 1, 207, 500, 2.10499999999999998, 2.1549999999999998, null, 10, 'MB7', 7);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (201, 10, 'Modis Surface Reflectance Band 2', 1, 202, 500, 0.84099999999999997, 0.876000000000000001, null, 10, 'MB2', 2);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (200, 10, 'Modis Surface Reflectance Band 1', 1, 201, 500, 0.619999999999999996, 0.67000000000000004, null, 10, 'MB1', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (202, 10, 'Modis Surface Reflectance Band 3', 1, 203, 500, 0.459000000000000019, 0.478999999999999981, null, 10, 'MB3', 3);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (204, 10, 'Modis Surface Reflectance Band 5', 1, 205, 500, 1.22999999999999998, 1.25, null, 10, 'MB5', 5);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (211, 10, 'Modis Surface Reflectance Band 12', 1, null, 500, 0.546000000000000041, 0.55600000000000005, null, 10, 'MB12', 12);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (212, 10, 'Modis Surface Reflectance Band 13', 1, null, 500, 0.662000000000000033, 0.672000000000000042, null, 10, 'MB13', 13);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (213, 10, 'Modis Surface Reflectance Band 14', 1, null, 500, 0.673000000000000043, 0.683000000000000052, null, 10, 'MB14', 14);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (214, 10, 'Modis Surface Reflectance Band 15', 1, null, 500, 0.742999999999999994, 0.753000000000000003, null, 10, 'MB15', 15);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (215, 10, 'Modis Surface Reflectance Band 16', 1, null, 500, 0.861999999999999988, 0.877000000000000002, null, 10, 'MB16', 16);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (216, 10, 'Modis Surface Reflectance Band 17', 1, null, 500, 0.890000000000000013, 0.92000000000000004, null, 10, 'MB17', 17);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (217, 10, 'Modis Surface Reflectance Band 18', 1, null, 500, 0.93100000000000005, 0.940999999999999948, null, 10, 'MB18', 18);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (218, 10, 'Modis Surface Reflectance Band 19', 1, null, 500, 0.915000000000000036, 0.964999999999999969, null, 10, 'MB19', 19);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (219, 10, 'Modis Surface Reflectance Band 20', 5, null, 500, 3.66000000000000014, 3.83999999999999986, null, 10, 'MB20', 20);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (220, 10, 'Modis Surface Reflectance Band 21', 22, null, 500, 3.92899999999999983, 3.98899999999999988, null, 10, 'MB21', 21);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (221, 10, 'Modis Surface Reflectance Band 22', 21, null, 500, 3.92899999999999983, 3.98899999999999988, null, 10, 'MB22', 22);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (222, 10, 'Modis Surface Reflectance Band 23', 5, null, 500, 4.01999999999999957, 4.08000000000000007, null, 10, 'MB23', 23);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (223, 10, 'Modis Surface Reflectance Band 24', 5, null, 500, 4.43299999999999983, 4.49800000000000022, null, 10, 'MB24', 24);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (224, 10, 'Modis Surface Reflectance Band 25', 5, null, 500, 4.48200000000000021, 4.54900000000000038, null, 10, 'MB25', 25);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (225, 10, 'Modis Surface Reflectance Band 26', 1, null, 500, 1.3600000000000001, 1.3899999999999999, null, 10, 'MB26', 26);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (226, 10, 'Modis Surface Reflectance Band 27', 5, null, 500, 6.53699999999999992, 6.89499999999999957, null, 10, 'MB27', 27);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (227, 10, 'Modis Surface Reflectance Band 28', 5, null, 500, 7.17499999999999982, 7.47499999999999964, null, 10, 'MB28', 28);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (228, 10, 'Modis Surface Reflectance Band 29', 5, null, 500, 8.40000000000000036, 8.69999999999999929, null, 10, 'MB29', 29);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (229, 10, 'Modis Surface Reflectance Band 30', 5, null, 500, 9.58000000000000007, 9.88000000000000078, null, 10, 'MB30', 30);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (230, 10, 'Modis Surface Reflectance Band 31', 5, null, 500, 10.7799999999999994, 11.2799999999999994, null, 10, 'MB31', 31);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (231, 10, 'Modis Surface Reflectance Band 32', 5, null, 500, 11.7699999999999996, 12.2699999999999996, null, 10, 'MB32', 32);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (232, 10, 'Modis Surface Reflectance Band 33', 5, null, 500, 13.1850000000000005, 13.4849999999999994, null, 10, 'MB33', 33);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (233, 10, 'Modis Surface Reflectance Band 34', 5, null, 500, 13.4849999999999994, 13.7850000000000001, null, 10, 'MB34', 34);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (234, 10, 'Modis Surface Reflectance Band 35', 5, null, 500, 13.7850000000000001, 14.0850000000000009, null, 10, 'MB35', 35);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (235, 10, 'Modis Surface Reflectance Band 36', 5, null, 500, 14.0850000000000009, 14.3849999999999998, null, 10, 'MB36', 36);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (250, 10, '500m Reflectance Band Quality', 31, 10, 500, null, null, null, 10, 'RBQ500', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (260, 10, '1km Reflectance Band Quality', 31, 10, 1000, null, null, null, 10, 'RBQ1000', 1);


insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (20, 'MOD09', -28672, 'near', 'MODIS MOD09');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (21, 'RBQ250', 3, 'near', '250m Reflectance Band Quality');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (22, 'RBQ500', 3, 'near', '500m Reflectance Band Quality');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (23, 'RBQ1000', 3, 'near', '1km Reflectance Band Quality');


insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 200, 20, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 201, 20, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 202, 20, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 203, 20, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 204, 20, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 205, 20, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 206, 20, 7);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (3, 250, 22, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 260, 23, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 207, 20, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 208, 20, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 209, 20, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 210, 20, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 211, 20, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 212, 20, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 213, 20, 7);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 214, 20, 8);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 215, 20, 9);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (4, 225, 20, 10);


insert into band_lookup_scheme (lookup_scheme_id, lookup_scheme_name, lookup_scheme_description)
values (10, 'MODIS', 'MODIS bands');

insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB8', 'MB8', 0.4125, 0.015, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB9', 'MB9', 0.443, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB3', 'MB3', 0.469, 0.02, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB10', 'MB10', 0.488, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB11', 'MB11', 0.531, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB12', 'MB12', 0.551, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB4', 'MB4', 0.555, 0.02, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB1', 'MB1', 0.645, 0.05, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB13', 'MB13', 0.667, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB14', 'MB14', 0.678, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB15', 'MB15', 0.748, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB2', 'MB2', 0.8585, 0.035, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB16', 'MB16', 0.8695, 0.015, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB17', 'MB17', 0.905, 0.03, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB18', 'MB18', 0.936, 0.01, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB19', 'MB19', 0.94, 0.05, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB5', 'MB5', 1.24, 0.02, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB26', 'MB26', 1.375, 0.03, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB6', 'MB6', 1.64, 0.024, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB7', 'MB7', 2.13, 0.05, 0.00025, 0.00025, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB20', 'MB20', 3.75, 0.18, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB21', 'MB21', 3.959, 0.06, 0.00025, 0.00025, 22);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB22', 'MB22', 3.959, 0.06, 0.00025, 0.00025, 21);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB23', 'MB23', 4.05, 0.06, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB24', 'MB24', 4.4655, 0.065, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB25', 'MB25', 4.5155, 0.067, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB27', 'MB27', 6.716, 0.358, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB28', 'MB28', 7.325, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB29', 'MB29', 8.55, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB30', 'MB30', 9.73, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB31', 'MB31', 11.03, 0.5, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB32', 'MB32', 12.02, 0.5, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB33', 'MB33', 13.335, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB34', 'MB34', 13.635, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB35', 'MB35', 13.935, 0.3, 0.00025, 0.00025, 5);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (10, 'MB36', 'MB36', 14.235, 0.3, 0.00025, 0.00025, 5);
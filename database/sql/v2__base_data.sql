--
-- PostgreSQL database dump
--

set statement_timeout = 0;
set lock_timeout = 0;
set client_encoding = 'UTF8';
set standard_conforming_strings = on;
set check_function_bodies = false;
set client_min_messages = warning;

--
-- Data for Name: satellite; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into satellite (satellite_id, satellite_name, satellite_tag, name_pattern, semi_major_axis, radius, altitude, inclination, omega, sweep_period, format, projection, spectral_filter_file, tle_format, solar_irrad_file, nominal_pixel_degrees)
values (1, 'Landsat-5', 'LS5', '^L.*5$', 7083160, 7285600, 705000, 1.71391332545843156, 0.00105900000000000005, 0.0734214390602055816, null, null, 'landsat5_vsir.flt', 'l5_%4d%s_norad.txt', 'solar_irrad_landsat5.txt', 0.000250000000000000005);
insert into satellite (satellite_id, satellite_name, satellite_tag, name_pattern, semi_major_axis, radius, altitude, inclination, omega, sweep_period, format, projection, spectral_filter_file, tle_format, solar_irrad_file, nominal_pixel_degrees)
values (2, 'Landsat-7', 'LS7', '^L.*7$', 7083160, 7285600, 705000, 1.71391332545843156, 0.00105900000000000005, 0.0734214390602055816, null, null, 'landsat7_vsir.flt', 'L7%4d%sASNNOR.S00', 'solar_irrad_landsat7.txt', 0.000250000000000000005);
insert into satellite (satellite_id, satellite_name, satellite_tag, name_pattern, semi_major_axis, radius, altitude, inclination, omega, sweep_period, format, projection, spectral_filter_file, tle_format, solar_irrad_file, nominal_pixel_degrees)
values (3, 'Landsat-8', 'LS8', '^L.*8$', 7083160, 7285600, 705000, 1.71391332545843156, 0.00105900000000000005, 0.0734214390602055816, null, null, 'landsat8_vsir.flt', 'L8%4d%sASNNOR.S00', 'solar_irrad_landsat8.txt', 0.000250000000000000005);

select pg_catalog.setval('satellite_id_seq', 3, true);


--
-- Data for Name: sensor; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into sensor (sensor_id, satellite_id, sensor_name, description) values (1, 1, 'MSS', 'Multi-Spectral Scanner');
insert into sensor (sensor_id, satellite_id, sensor_name, description) values (2, 1, 'TM', 'Thermatic Mapper');
insert into sensor (sensor_id, satellite_id, sensor_name, description)
values (3, 2, 'ETM+', 'Enhanced Thermatic Mapper Plus');
insert into sensor (sensor_id, satellite_id, sensor_name, description) values (5, 3, 'TIRS', 'Thermal Infrared Sensor');
insert into sensor (sensor_id, satellite_id, sensor_name, description)
values (6, 3, 'OLI_TIRS', 'Operational Land Imager and Thermal Infrared Sensor');
insert into sensor (sensor_id, satellite_id, sensor_name, description) values (4, 3, 'OLI', 'Operational Land Imager');

select pg_catalog.setval('sensor_id_seq', 6, true);


--
-- Data for Name: tile_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (1, 'Unprojected WGS84 1-degree at 4000 pixels/degree', 'EPSG:4326', 0, 0, 1, 1, 4000, 4000, 'degree', 'GTiff', '.tif', 'EPSG4326_1deg_0.00025pixel', 'COMPRESS=LZW');
insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (200, 'Half test', 'EPSG:4326', 0, 0, 0.5, 0.5, 2000, 2000, 'degree', 'GTiff', '.tif', 'half_test', 'COMPRESS=LZW');
insert into tile_type (tile_type_id, tile_type_name, crs, x_origin, y_origin, x_size, y_size, x_pixels, y_pixels, unit, file_format, file_extension, tile_directory, format_options)
values (400, 'Quarter test', 'EPSG:4326', 0, 0, 0.25, 0.25, 1000, 1000, 'degree', 'GTiff', '.tif', 'quarter_test', 'COMPRESS=LZW');


--
-- Data for Name: band_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band_type (band_type_id, band_type_name) values (2, 'thermal');
insert into band_type (band_type_id, band_type_name) values (3, 'panchromatic');
insert into band_type (band_type_id, band_type_name) values (4, 'derived');
insert into band_type (band_type_id, band_type_name) values (1, 'reflective');
insert into band_type (band_type_id, band_type_name) values (30, 'PQA');
insert into band_type (band_type_id, band_type_name) values (40, 'FC');
insert into band_type (band_type_id, band_type_name) values (60, 'NDVI');
insert into band_type (band_type_id, band_type_name) values (110, 'slope');
insert into band_type (band_type_id, band_type_name) values (120, 'aspect');
insert into band_type (band_type_id, band_type_name) values (100, 'elevation');
insert into band_type (band_type_id, band_type_name) values (21, 'thermal low-gain');
insert into band_type (band_type_id, band_type_name) values (22, 'thermal high-gain');

select pg_catalog.setval('band_type_id_seq', 121, true);


--
-- Data for Name: band; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (110, null, 'Slope (Degrees)', 110, 20, null, null, null, null, null, 'SLOPE', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (4, 2, 'Visible Blue', 1, 10, 25, 0.450000000000000011, 0.520000000000000018, '.*_B10\..*', 1, 'B10', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (5, 2, 'Visible Green', 1, 20, 25, 0.520000000000000018, 0.599999999999999978, '.*_B20\..*', 1, 'B20', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (6, 2, 'Visible Red', 1, 30, 25, 0.630000000000000004, 0.689999999999999947, '.*_B30\..*', 1, 'B30', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (7, 2, 'Near Infrared', 1, 40, 25, 0.760000000000000009, 0.900000000000000022, '.*_B40\..*', 1, 'B40', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (8, 2, 'Middle Infrared 1', 1, 50, 25, 1.55000000000000004, 1.75, '.*_B50\..*', 1, 'B50', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (9, 2, 'Thermal Infrared', 2, 60, 100, 10.4000000000000004, 12.5, '.*_B60\..*', 1, 'B60', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (10, 2, 'Middle Infrared 2', 1, 70, 25, 2.08000000000000007, 2.35000000000000009, '.*_B70\..*', 1, 'B70', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (11, 3, 'Visible Blue', 1, 10, 25, 0.450000000000000011, 0.520000000000000018, '.*_B10\..*', 2, 'B10', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (12, 3, 'Visible Green', 1, 20, 25, 0.520000000000000018, 0.599999999999999978, '.*_B20\..*', 2, 'B20', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (100, null, 'Elevation', 100, 10, null, null, null, null, null, 'ELEVATION', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (120, null, 'Aspect (Compass Degrees Downhill)', 120, 30, null, null, null, null, null, 'ASPECT', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (43, null, 'Bare Soil', 40, 30, null, null, null, '.*_BS\..*', null, 'BS', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (44, null, 'Unmixing Error', 40, 40, null, null, null, '.*_UE\..*', null, 'UE', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (42, null, 'Non-Photosynthetic Vegetation', 40, 20, null, null, null, '.*_NPV\..*', null, 'NPV', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (34, null, 'NDVI', 60, 10, null, null, null, '.*_NDVI_.*', null, 'NDVI', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (32, null, 'Pixel Quality Assurance', 30, 10, null, null, null, 'L.*_(0|1){16}\..*', null, 'PQA', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (41, null, 'Photosynthetic Vegetation', 40, 10, null, null, null, '.*_PV\..*', null, 'PV', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (13, 3, 'Visible Red', 1, 30, 25, 0.630000000000000004, 0.689999999999999947, '.*_B30\..*', 2, 'B30', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (14, 3, 'Near Infrared', 1, 40, 25, 0.760000000000000009, 0.900000000000000022, '.*_B40\..*', 2, 'B40', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (15, 3, 'Middle Infrared 1', 1, 50, 25, 1.55000000000000004, 1.75, '.*_B50\..*', 2, 'B50', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (18, 3, 'Middle Infrared 2', 1, 70, 25, 2.08000000000000007, 2.35000000000000009, '.*_B70\..*', 2, 'B70', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (19, 3, 'Panchromatic', 3, 80, 12.5, 0.520000000000000018, 0.900000000000000022, '.*_B80\..*', 2, 'B80', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (20, 6, 'Coastal Aerosol', 1, 1, 25, 0.432999999999999996, 0.453000000000000014, '.*_B1\..*', 3, 'B1', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (21, 6, 'Visible Blue', 1, 2, 25, 0.450000000000000011, 0.515000000000000013, '.*_B2\..*', 3, 'B2', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (22, 6, 'Visible Green', 1, 3, 25, 0.525000000000000022, 0.599999999999999978, '.*_B3\..*', 3, 'B3', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (23, 6, 'Visible Red', 1, 4, 25, 0.630000000000000004, 0.680000000000000049, '.*_B4\..*', 3, 'B4', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (24, 6, 'Near Infrared', 1, 5, 25, 0.844999999999999973, 0.885000000000000009, '.*_B5\..*', 3, 'B5', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (25, 6, 'Short-wave Infrared 1', 1, 6, 25, 1.56000000000000005, 1.65999999999999992, '.*_B6\..*', 3, 'B6', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (26, 6, 'Short-wave Infrared 2', 1, 7, 25, 2.10000000000000009, 2.29999999999999982, '.*_B7\..*', 3, 'B7', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (27, 6, 'Panchromatic', 3, 8, 12.5, 0.5, 0.680000000000000049, '.*_B8\..*', 3, 'B8', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (28, 6, 'Cirrus', 1, 9, 25, 1.3600000000000001, 1.3899999999999999, '.*_B9\..*', 3, 'B9', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (50, 4, 'Coastal Aerosol', 1, 1, 25, 0.432999999999999996, 0.453000000000000014, '.*_B1\..*', 3, 'B1', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (51, 4, 'Visible Blue', 1, 2, 25, 0.450000000000000011, 0.515000000000000013, '.*_B2\..*', 3, 'B2', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (52, 4, 'Visible Green', 1, 3, 25, 0.525000000000000022, 0.599999999999999978, '.*_B3\..*', 3, 'B3', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (53, 4, 'Visible Red', 1, 4, 25, 0.630000000000000004, 0.680000000000000049, '.*_B4\..*', 3, 'B4', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (54, 4, 'Near Infrared', 1, 5, 25, 0.844999999999999973, 0.885000000000000009, '.*_B5\..*', 3, 'B5', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (55, 4, 'Short-wave Infrared 1', 1, 6, 25, 1.56000000000000005, 1.65999999999999992, '.*_B6\..*', 3, 'B6', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (56, 4, 'Short-wave Infrared 2', 1, 7, 25, 2.10000000000000009, 2.29999999999999982, '.*_B7\..*', 3, 'B7', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (57, 4, 'Panchromatic', 3, 8, 12.5, 0.5, 0.680000000000000049, '.*_B8\..*', 3, 'B8', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (58, 4, 'Cirrus', 1, 9, 25, 1.3600000000000001, 1.3899999999999999, '.*_B9\..*', 3, 'B9', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (59, 5, 'Thermal Infrared 1', 2, 10, 25, 10.5999999999999996, 11.1899999999999995, '.*_B10\..*', 3, 'B10', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (60, 5, 'Thermal Infrared 2', 2, 11, 25, 11.5, 12.5099999999999998, '.*_B11\..*', 3, 'B11', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (30, 6, 'Thermal Infrared 2', 2, 11, 25, 11.5, 12.5099999999999998, '.*_B11\..*', 3, 'B11', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (16, 3, 'Thermal Infrared (Low Gain)', 21, 61, 50, 10.4000000000000004, 12.5, '.*_B61\..*', 2, 'B61', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (17, 3, 'Thermal Infrared (High Gain)', 22, 62, 50, 10.4000000000000004, 12.5, '.*_B62\..*', 2, 'B62', 1);
insert into band (band_id, sensor_id, band_name, band_type_id, file_number, resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (29, 6, 'Thermal Infrared 1', 2, 10, 25, 10.5999999999999996, 11.1899999999999995, '.*_B10\..*', 3, 'B10', 1);


--
-- Name: band_id_seq; Type: SEQUENCE SET; Schema: public; Owner: cube_admin
--

select pg_catalog.setval('band_id_seq', 121, true);


--
-- Data for Name: band_lookup_scheme; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band_lookup_scheme (lookup_scheme_id, lookup_scheme_name, lookup_scheme_description)
values (2, 'LANDSAT-LS5/7', 'Landsat equivalent bands with adjustments to make LS8 conform to LS5/7

Source:
================================================================================
Remote Sens. 2014, 6, 7952-7970; doi:10.3390/rs6097952

OPEN ACCESS

Remote Sensing
ISSN 2072-4292
www.mdpi.com/journal/remotesensing

Article
Continuity of Reflectance Data between Landsat-7 ETM+ and Landsat-8 OLI, for Both Top-of-Atmosphere and Surface Reflectance: A Study in the Australian Landscape

Neil Flood

Joint Remote Sensing Research Program, School of Geography, Planning and Environmental Management, University of Queensland, St Lucia, QLD 4072, Australia; E-Mail: n.flood@uq.edu.au;
Tel.: +61-7-3170-5677

Received: 19 May 2014; in revised form: 17 July 2014 / Accepted: 31 July 2014 /
Published: 26 August 2014
================================================================================
');
insert into band_lookup_scheme (lookup_scheme_id, lookup_scheme_name, lookup_scheme_description)
values (3, 'LANDSAT-LS8', 'Landsat equivalent bands with adjustments to make LS5/7 conform to LS8

Source:
================================================================================
Remote Sens. 2014, 6, 7952-7970; doi:10.3390/rs6097952

OPEN ACCESS

Remote Sensing
ISSN 2072-4292
www.mdpi.com/journal/remotesensing

Article
Continuity of Reflectance Data between Landsat-7 ETM+ and Landsat-8 OLI, for Both Top-of-Atmosphere and Surface Reflectance: A Study in the Australian Landscape

Neil Flood

Joint Remote Sensing Research Program, School of Geography, Planning and Environmental Management, University of Queensland, St Lucia, QLD 4072, Australia; E-Mail: n.flood@uq.edu.au;
Tel.: +61-7-3170-5677

Received: 19 May 2014; in revised form: 17 July 2014 / Accepted: 31 July 2014 /
Published: 26 August 2014
================================================================================
');
insert into band_lookup_scheme (lookup_scheme_id, lookup_scheme_name, lookup_scheme_description)
values (1, 'LANDSAT-UNADJUSTED', 'Landsat equivalent bands with no adjustments');


--
-- Data for Name: band_adjustment; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 26, 0.0, 0.99);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 56, 0.0, 0.99);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 4, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 6, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 18, 0.0, 1.01);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 11, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 5, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 12, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 13, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 7, 0.0, 0.99);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 14, 0.0, 0.99);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 8, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 15, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (3, 10, 0.0, 1.01);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 20, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 25, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 27, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 28, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 29, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 30, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 50, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 55, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 57, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 58, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 59, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 60, 0.0, 1.0);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 51, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 21, 0.0, 1.02);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 52, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 22, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 23, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 53, 0.0, 0.98);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 54, 0.0, 1.01);
insert into band_adjustment (lookup_scheme_id, band_id, adjustment_offset, adjustment_multiplier)
values (2, 24, 0.0, 1.01);


--
-- Data for Name: band_equivalent; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Visible Blue', 'B', 0.485, 0.07, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Visible Green', 'G', 0.56, 0.08, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Visible Red', 'R', 0.66, 0.06, 0.005, 0.01, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Near Infrared', 'NIR', 0.83, 0.14, 0.035, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Shortwave Infrared 1', 'SWIR1', 1.65, 0.2, 0.04, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Shortwave Infrared 2', 'SWIR2', 2.215, 0.27, 0.015, 0.07, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Visible Blue', 'B', 0.485, 0.07, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Visible Green', 'G', 0.56, 0.08, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Visible Red', 'R', 0.66, 0.06, 0.005, 0.01, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Near Infrared', 'NIR', 0.83, 0.14, 0.035, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Shortwave Infrared 1', 'SWIR1', 1.65, 0.2, 0.04, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Shortwave Infrared 2', 'SWIR2', 2.215, 0.27, 0.015, 0.07, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Visible Blue', 'B', 0.485, 0.07, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Visible Green', 'G', 0.56, 0.08, 0.0025, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Visible Red', 'R', 0.66, 0.06, 0.005, 0.01, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Near Infrared', 'NIR', 0.83, 0.14, 0.035, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Shortwave Infrared 1', 'SWIR1', 1.65, 0.2, 0.04, 0.1, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Shortwave Infrared 2', 'SWIR2', 2.215, 0.27, 0.015, 0.07, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Thermal Infrared', 'TIR', 11.45, 2.1, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Thermal Infrared - Low Gain', 'TIR-LG', 11.45, 2.1, 0.005, 0.005, 21);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Thermal Infrared - High-Gain', 'TIR-HG', 11.45, 2.1, 0.005, 0.005, 22);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Thermal Infrared 1', 'TIR1', 10.89, 0.59, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Thermal Infrared 2', 'TIR2', 12.01, 1.01, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (1, 'Narrow Blue', 'NB', 0.44, 0.02, 0.005, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Thermal Infrared', 'TIR', 11.45, 2.1, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Thermal Infrared - Low Gain', 'TIR-LG', 11.45, 2.1, 0.005, 0.005, 21);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Thermal Infrared - High-Gain', 'TIR-HG', 11.45, 2.1, 0.005, 0.005, 22);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Thermal Infrared 1', 'TIR1', 10.89, 0.59, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Thermal Infrared 2', 'TIR2', 12.01, 1.01, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (2, 'Narrow Blue', 'NB', 0.44, 0.02, 0.005, 0.005, 1);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Thermal Infrared', 'TIR', 11.45, 2.1, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Thermal Infrared - Low Gain', 'TIR-LG', 11.45, 2.1, 0.005, 0.005, 21);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Thermal Infrared - High-Gain', 'TIR-HG', 11.45, 2.1, 0.005, 0.005, 22);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Thermal Infrared 1', 'TIR1', 10.89, 0.59, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Thermal Infrared 2', 'TIR2', 12.01, 1.01, 0.005, 0.005, 2);
insert into band_equivalent (lookup_scheme_id, master_band_name, master_band_tag, nominal_centre, nominal_bandwidth, centre_tolerance, bandwidth_tolerance, band_type_id)
values (3, 'Narrow Blue', 'NB', 0.44, 0.02, 0.005, 0.005, 1);


--
-- Data for Name: processing_level; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (4, 'FC', -999, 'near', 'Fractional Cover');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (1, 'ORTHO', 0, 'near', 'Level 1 Terrain Corrected');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (2, 'NBAR', -999, 'near', 'Optical Surface Reflectance (NBAR) Corrected');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (3, 'PQA', null, 'near', 'Pixel Quality Bit-Mapped Data');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (5, 'L1T', 0, 'near', 'Level 1 Terrain Corrected');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (10, 'MAP', 0, 'near', 'Level 1 Geographically Corrected');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (100, 'DSM', null, 'bilinear', 'Digital Surface Model (GA-only)');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (110, 'DEM', null, 'bilinear', 'Digital Elevation Model - Bare Earth');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (120, 'DEM-S', null, 'bilinear', 'Digital Elevation Model - Smoothed');
insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (130, 'DEM-H', null, 'bilinear', 'Digital Elevation Model - Hydrologically Enforced');


select pg_catalog.setval('level_id_seq', 131, true);


--
-- Data for Name: band_source; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 4, 2, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 5, 2, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 6, 2, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 7, 2, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 8, 2, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 9, 1, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 10, 2, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 11, 2, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 12, 2, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 13, 2, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 14, 2, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 15, 2, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 16, 1, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 17, 1, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 18, 2, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 32, 3, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 41, 4, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 42, 4, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 43, 4, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 44, 4, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 100, 100, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 110, 100, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 120, 100, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 100, 110, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 110, 110, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 120, 110, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 100, 120, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 110, 120, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 120, 120, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 100, 130, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 110, 130, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 120, 130, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 20, 2, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 21, 2, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 22, 2, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 23, 2, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 24, 2, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 25, 2, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 26, 2, 7);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 29, 1, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 30, 1, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 50, 2, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 51, 2, 2);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 52, 2, 3);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 53, 2, 4);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 54, 2, 5);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 55, 2, 6);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 56, 2, 7);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 59, 1, 1);
insert into band_source (tile_type_id, band_id, level_id, tile_layer) values (1, 60, 1, 2);


--
-- Data for Name: lock_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into lock_type (lock_type_id, lock_type_name) values (1, 'Standard Lock');


--
-- Data for Name: tile_class; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

insert into tile_class (tile_class_id, tile_class_name) values (1, 'Single-scene tile');
insert into tile_class (tile_class_id, tile_class_name) values (2, 'Empty tile (file deleted)');
insert into tile_class (tile_class_id, tile_class_name)
values (4, 'Two-scene composite tile for North-South scene overlap');
insert into tile_class (tile_class_id, tile_class_name) values (1001, 'Temporarily ignored non-empty');
insert into tile_class (tile_class_id, tile_class_name) values (1002, 'Temporarily ignored empty');
insert into tile_class (tile_class_id, tile_class_name) values (0, 'Pending Ingestion');
insert into tile_class (tile_class_id, tile_class_name) values (3, 'Overlapped tile replaced by mosaic');
insert into tile_class (tile_class_id, tile_class_name) values (1003, 'Temporarily ignored overlapped tile');
insert into tile_class (tile_class_id, tile_class_name) values (1004, 'Temporarily ignored composite tile');


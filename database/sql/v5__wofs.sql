insert into band_type (band_type_name) values ('WATER');

insert into band (sensor_id, band_name, band_type_id,
                         file_number,
                         resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (null, 'WATER',
        (select band_type_id from band_type where band_type_name = 'WATER'),
        1, -- ??
        null, null, null, '.*_WATER_.*', null, 'WATER', 1);

insert into processing_level (level_name, nodata_value, resampling_method, level_description)
values ('WATER', 0, 'near', 'Water');

insert into band_source (tile_type_id, band_id, level_id, tile_layer)
values (1,
        (select band_id from band where band_name = 'WATER'),
        (select level_id from processing_level where level_name = 'WATER'),
        1 -- ??
);

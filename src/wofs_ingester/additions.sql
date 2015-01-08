-- Water types

insert into band_type (band_type_id, band_type_name)
values (50, 'WATER');

insert into public.band (band_id, sensor_id, band_name, band_type_id,
                         file_number,
                         resolution, min_wavelength, max_wavelength, file_pattern, satellite_id, band_tag, band_number)
values (34, null, 'WATER', 50,
        10, -- ??
        null, null, null, '.*_WATER_.*', null, 'WATER', 1);

insert into processing_level (level_id, level_name, nodata_value, resampling_method, level_description)
values (6, 'WATER', 0, 'near', 'Water');

insert into band_source (tile_type_id, band_id, level_id, tile_layer)
values (1,
        34, 6,
        1 -- ??
);

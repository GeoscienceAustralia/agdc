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
-- Name: _final_median(anyarray); Type: FUNCTION; Schema: public; Owner: cube_admin
--

CREATE FUNCTION _final_median(anyarray) RETURNS double precision
LANGUAGE sql IMMUTABLE
AS $_$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$_$;




alter function _final_median( anyarray )
owner to cube_admin;

--
-- Name: median(anyelement); Type: AGGREGATE; Schema: public; Owner: cube_admin
--

CREATE AGGREGATE median(anyelement) (
    SFUNC = array_append,
    STYPE = anyarray,
    INITCOND = '{}',
    FINALFUNC = _final_median
);




alter aggregate median( anyelement )
owner to cube_admin;

set default_tablespace = '';

set default_with_oids = false;

--
-- Name: acquisition; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table acquisition (
    acquisition_id bigint not null,
    satellite_id   integer,
    sensor_id      integer,
    x_ref          smallint,
    y_ref          smallint,
    start_datetime timestamp without time zone,
    end_datetime   timestamp without time zone,
    ll_lon         double precision,
    ll_lat         double precision,
    lr_lon         double precision,
    lr_lat         double precision,
    ul_lon         double precision,
    ul_lat         double precision,
    ur_lon         double precision,
    ur_lat         double precision,
    gcp_count      integer,
    mtl_text       text,
    cloud_cover    double precision
);


alter table acquisition owner to cube_admin;

--
-- Name: TABLE acquisition; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table acquisition is 'Defines source acquisition independent of processing level';


--
-- Name: COLUMN acquisition.cloud_cover; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on column acquisition.cloud_cover is 'Cloud Cover Percentage from Level 1 Metadata';


--
-- Name: acquisition_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table acquisition_footprint (
    tile_type_id   bigint not null,
    acquisition_id bigint not null,
    x_min          double precision,
    y_min          double precision,
    x_max          double precision,
    y_max          double precision
);


alter table acquisition_footprint owner to cube_admin;

--
-- Name: acquisition_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence acquisition_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table acquisition_id_seq owner to cube_admin;

--
-- Name: band_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence band_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table band_id_seq owner to cube_admin;

--
-- Name: band; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band (
    band_id        integer default nextval('band_id_seq' :: regclass) not null,
    sensor_id      integer,
    band_name      text                                               not null,
    band_type_id   integer,
    file_number    smallint,
    resolution     double precision,
    min_wavelength double precision,
    max_wavelength double precision,
    file_pattern   text,
    satellite_id   integer,
    band_tag       text,
    band_number    smallint
);


alter table band owner to cube_admin;

--
-- Name: TABLE band; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table band is 'Band information';


--
-- Name: band_adjustment; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band_adjustment (
    lookup_scheme_id      integer not null,
    band_id               integer not null,
    adjustment_offset     numeric,
    adjustment_multiplier numeric
);


alter table band_adjustment owner to cube_admin;

--
-- Name: band_equivalent; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band_equivalent (
    lookup_scheme_id    integer not null,
    master_band_name    character varying(32),
    master_band_tag     character varying(16),
    nominal_centre      numeric not null,
    nominal_bandwidth   numeric not null,
    centre_tolerance    numeric,
    bandwidth_tolerance numeric,
    band_type_id        integer not null
);


alter table band_equivalent owner to cube_admin;

--
-- Name: TABLE band_equivalent; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table band_equivalent is 'Table for managing equivalent bands between sensors';


--
-- Name: band_lookup_scheme; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band_lookup_scheme (
    lookup_scheme_id          integer               not null,
    lookup_scheme_name        character varying(32) not null,
    lookup_scheme_description text
);


alter table band_lookup_scheme owner to cube_admin;

--
-- Name: band_source; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band_source (
    tile_type_id bigint   not null,
    band_id      integer  not null,
    level_id     smallint not null,
    tile_layer   integer
);


alter table band_source owner to cube_admin;

--
-- Name: TABLE band_source; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table band_source is 'Defines which bands will be selected from which datasets and stored in which layer';


--
-- Name: band_type_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence band_type_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table band_type_id_seq owner to cube_admin;

--
-- Name: band_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table band_type (
    band_type_id   integer default nextval('band_type_id_seq' :: regclass) not null,
    band_type_name text
);


alter table band_type owner to cube_admin;

--
-- Name: TABLE band_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table band_type is 'Band type information. May be reflective, thermal, panchromatic or derived';


--
-- Name: level_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence level_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table level_id_seq owner to cube_admin;

--
-- Name: processing_level; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table processing_level (
    level_id          smallint default nextval('level_id_seq' :: regclass) not null,
    level_name        text,
    nodata_value      bigint,
    resampling_method text,
    level_description text
);


alter table processing_level owner to cube_admin;

--
-- Name: TABLE processing_level; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table processing_level is 'Processing level for source dataset (e.g. L1T, NBAR or PQA)';


--
-- Name: satellite_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence satellite_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table satellite_id_seq owner to cube_admin;

--
-- Name: satellite; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table satellite (
    satellite_id          integer default nextval('satellite_id_seq' :: regclass) not null,
    satellite_name        text,
    satellite_tag         text,
    name_pattern          text,
    semi_major_axis       double precision,
    radius                double precision,
    altitude              double precision,
    inclination           double precision,
    omega                 double precision,
    sweep_period          double precision,
    format                text,
    projection            text,
    spectral_filter_file  text,
    tle_format            text,
    solar_irrad_file      text,
    nominal_pixel_degrees double precision
);


alter table satellite owner to cube_admin;

--
-- Name: TABLE satellite; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table satellite is 'Satellite information. Will have one or more sensors';


--
-- Name: sensor_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence sensor_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table sensor_id_seq owner to cube_admin;

--
-- Name: sensor; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table sensor (
    sensor_id    integer default nextval('sensor_id_seq' :: regclass) not null,
    satellite_id integer                                              not null,
    sensor_name  text,
    description  text
);


alter table sensor owner to cube_admin;

--
-- Name: TABLE sensor; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table sensor is 'Sensor information. May have one or more bands';


--
-- Name: band_lookup; Type: VIEW; Schema: public; Owner: cube_admin
--


CREATE VIEW band_lookup AS
    SELECT DISTINCT band_lookup_scheme.lookup_scheme_name,
        COALESCE(satellite.satellite_tag, 'DERIVED'::text) AS satellite_tag,
        COALESCE(sensor.sensor_name, processing_level.level_name) AS sensor_name,
        band_equivalent.master_band_tag,
        processing_level.level_name,
        band_source.tile_layer,
        (band_equivalent.nominal_centre)::double precision AS nominal_centre,
        (band_equivalent.nominal_bandwidth)::double precision AS nominal_bandwidth,
        (band_equivalent.centre_tolerance)::double precision AS centre_tolerance,
        (band_equivalent.bandwidth_tolerance)::double precision AS bandwidth_tolerance,
        (COALESCE(band_adjustment.adjustment_offset, 0.0))::double precision AS adjustment_offset,
        (COALESCE(band_adjustment.adjustment_multiplier, 1.0))::double precision AS adjustment_multiplier,
        band_lookup_scheme.lookup_scheme_id,
        band.satellite_id,
        band.sensor_id,
        band.band_id,
        band_equivalent.master_band_name,
        band.min_wavelength,
        band.max_wavelength,
        band_lookup_scheme.lookup_scheme_description
    FROM ((((((((band
                 JOIN band_type USING (band_type_id))
                 JOIN band_source USING (band_id))
                JOIN processing_level USING (level_id))
               JOIN band_equivalent USING (band_type_id))
              JOIN band_lookup_scheme USING (lookup_scheme_id))
             LEFT JOIN band_adjustment USING (lookup_scheme_id, band_id))
            LEFT JOIN sensor USING (satellite_id, sensor_id))
           LEFT JOIN satellite USING (satellite_id))
    WHERE ((abs(((((band.max_wavelength)::numeric + (band.min_wavelength)::numeric) / 2.0) - band_equivalent.nominal_centre)) <= band_equivalent.centre_tolerance) AND (abs((((band.max_wavelength)::numeric - (band.min_wavelength)::numeric) - band_equivalent.nominal_bandwidth)) <= band_equivalent.bandwidth_tolerance))
    ORDER BY band_lookup_scheme.lookup_scheme_name, COALESCE(satellite.satellite_tag, 'DERIVED'::text), COALESCE(sensor.sensor_name, processing_level.level_name), processing_level.level_name, band_source.tile_layer, (band_equivalent.nominal_centre)::double precision;


alter table band_lookup owner to cube_admin;

--
-- Name: dataset_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence dataset_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table dataset_id_seq owner to cube_admin;

--
-- Name: dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table dataset (
    dataset_id         bigint default nextval('dataset_id_seq' :: regclass) not null,
    acquisition_id     bigint,
    dataset_path       text,
    level_id           smallint,
    dataset_size       integer,
    datetime_processed timestamp without time zone,
    crs                text,
    ll_x               double precision,
    ll_y               double precision,
    lr_x               double precision,
    lr_y               double precision,
    ul_x               double precision,
    ul_y               double precision,
    ur_x               double precision,
    ur_y               double precision,
    x_pixels           integer,
    y_pixels           integer,
    xml_text           text
);


alter table dataset owner to cube_admin;

--
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table dataset is 'Source dataset for tile provenance';


--
-- Name: lock; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table lock (
    lock_type_id   integer not null,
    lock_object    text    not null,
    lock_owner     text,
    lock_status_id integer,
    lock_detail    text
);


alter table lock owner to cube_admin;

--
-- Name: lock_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table lock_type (
    lock_type_id   integer not null,
    lock_type_name text
);


alter table lock_type owner to cube_admin;

--
-- Name: tile_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence tile_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table tile_id_seq owner to cube_admin;

--
-- Name: tile; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table tile (
    tile_id       bigint default nextval('tile_id_seq' :: regclass) not null,
    x_index       integer                                           not null,
    y_index       integer                                           not null,
    tile_type_id  bigint                                            not null,
    tile_pathname text,
    dataset_id    bigint,
    tile_class_id integer,
    tile_size     integer,
    ctime         timestamp with time zone default now(),
    tile_status   smallint
);


alter table tile owner to cube_admin;

--
-- Name: TABLE tile; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table tile is 'Tile information for individual tiles';


--
-- Name: tile_class; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table tile_class (
    tile_class_id   integer not null,
    tile_class_name text
);


alter table tile_class owner to cube_admin;

--
-- Name: TABLE tile_class; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table tile_class is 'Definitions for different classes of tile
e.g. class 1 indicates a real tile containing data while class 2 indicates an empty tile not stored as a file';


--
-- Name: tile_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table tile_footprint (
    x_index      integer not null,
    y_index      integer not null,
    tile_type_id bigint  not null,
    x_min        double precision,
    y_min        double precision,
    x_max        double precision,
    y_max        double precision,
    bbox         geometry,
    constraint enforce_dims_bbox check ((st_ndims(bbox) = 2)),
    constraint enforce_geotype_bbox check (((geometrytype(bbox) = 'POLYGON' :: text) or (bbox is null))),
    constraint enforce_srid_bbox check ((st_srid(bbox) = 4326))
);


alter table tile_footprint owner to cube_admin;

--
-- Name: TABLE tile_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table tile_footprint is 'Definitions for each unique tile footprint.
Pre-populated to allow simple range queries without calculation';


--
-- Name: tile_type_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

create sequence tile_type_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;


alter table tile_type_id_seq owner to cube_admin;

--
-- Name: tile_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

create table tile_type (
    tile_type_id   bigint default nextval('tile_type_id_seq' :: regclass) not null,
    tile_type_name text,
    crs            text,
    x_origin       double precision,
    y_origin       double precision,
    x_size         double precision,
    y_size         double precision,
    x_pixels       bigint,
    y_pixels       bigint,
    unit           text,
    file_format    text,
    file_extension text,
    tile_directory text,
    format_options text
);


alter table tile_type owner to cube_admin;

--
-- Name: TABLE tile_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

comment on table tile_type is 'Contains a definition for each different tiling scheme.';


--
-- Name: acquisition_footprint_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only acquisition_footprint
add constraint acquisition_footprint_pkey primary key (tile_type_id, acquisition_id);


--
-- Name: acquisition_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only acquisition
add constraint acquisition_pkey primary key (acquisition_id);


--
-- Name: acquisition_reference_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only acquisition
add constraint acquisition_reference_key unique (satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime);


--
-- Name: band_adjustment_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band_adjustment
add constraint band_adjustment_pkey primary key (lookup_scheme_id, band_id);


--
-- Name: band_equivalent_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band_equivalent
add constraint band_equivalent_pkey primary key (lookup_scheme_id, nominal_centre, nominal_bandwidth, band_type_id);


--
-- Name: band_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band
add constraint band_pkey primary key (band_id);


--
-- Name: band_source_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band_source
add constraint band_source_pkey primary key (tile_type_id, band_id, level_id);


--
-- Name: band_type_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band_type
add constraint band_type_pkey primary key (band_type_id);


--
-- Name: dataset_acquisition_id_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only dataset
add constraint dataset_acquisition_id_key unique (acquisition_id, level_id, crs);


--
-- Name: dataset_path_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only dataset
add constraint dataset_path_key unique (dataset_path);


--
-- Name: dataset_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only dataset
add constraint dataset_pkey primary key (dataset_id);


--
-- Name: dataset_reference_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only dataset
add constraint dataset_reference_key unique (acquisition_id, level_id);


--
-- Name: lock_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only lock
add constraint lock_pkey primary key (lock_type_id, lock_object);


--
-- Name: lock_type_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only lock_type
add constraint lock_type_pkey primary key (lock_type_id);


--
-- Name: lookup_scheme_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only band_lookup_scheme
add constraint lookup_scheme_pkey primary key (lookup_scheme_id);


--
-- Name: processing_level_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only processing_level
add constraint processing_level_pkey primary key (level_id);


--
-- Name: satellite_name_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only satellite
add constraint satellite_name_key unique (satellite_name);


--
-- Name: satellite_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only satellite
add constraint satellite_pkey primary key (satellite_id);


--
-- Name: satellite_tag_key; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only satellite
add constraint satellite_tag_key unique (satellite_tag);


--
-- Name: sensor_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only sensor
add constraint sensor_pkey primary key (satellite_id, sensor_id);


--
-- Name: tile_class_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only tile_class
add constraint tile_class_pkey primary key (tile_class_id);


--
-- Name: tile_footprint_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only tile_footprint
add constraint tile_footprint_pkey primary key (x_index, y_index, tile_type_id);


--
-- Name: tile_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only tile
add constraint tile_pkey primary key (tile_id);


--
-- Name: tile_tile_pathname_unique; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only tile
add constraint tile_tile_pathname_unique unique (tile_pathname);


--
-- Name: tile_type_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

alter table only tile_type
add constraint tile_type_pkey primary key (tile_type_id);


--
-- Name: dataset_dataset_path_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create unique index dataset_dataset_path_idx on dataset using btree (dataset_path);


--
-- Name: dataset_ll_x_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ll_x_idx on dataset using btree (ll_x);


--
-- Name: dataset_ll_y_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ll_y_idx on dataset using btree (ll_y);


--
-- Name: dataset_lr_x_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_lr_x_idx on dataset using btree (ll_y);


--
-- Name: dataset_lr_y_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_lr_y_idx on dataset using btree (ll_y);


--
-- Name: dataset_pk; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_pk on dataset using btree (dataset_id);


--
-- Name: dataset_ul_x_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ul_x_idx on dataset using btree (ul_x);


--
-- Name: dataset_ul_y_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ul_y_idx on dataset using btree (ul_y);


--
-- Name: dataset_ur_x_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ur_x_idx on dataset using btree (ur_x);


--
-- Name: dataset_ur_y_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index dataset_ur_y_idx on dataset using btree (ur_y);


--
-- Name: fk_acquisition_footprint_id_fki; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fk_acquisition_footprint_id_fki on acquisition_footprint using btree (acquisition_id);


--
-- Name: fk_acquisition_sensor_fki; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fk_acquisition_sensor_fki on acquisition using btree (sensor_id, satellite_id);


--
-- Name: fk_tile_class_id_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fk_tile_class_id_idx on tile using btree (tile_class_id);


--
-- Name: fki_band_adjustment_band; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_band_adjustment_band on band_adjustment using btree (band_id);


--
-- Name: fki_band_band_type_id_key; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_band_band_type_id_key on band using btree (band_type_id);


--
-- Name: fki_band_equivalent_band_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_band_equivalent_band_type on band_equivalent using btree (band_type_id);


--
-- Name: fki_band_sensor_id_key; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_band_sensor_id_key on band using btree (satellite_id, sensor_id);


--
-- Name: fki_dataset_acquisition_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_dataset_acquisition_fkey on dataset using btree (acquisition_id);


--
-- Name: fki_dataset_processing_level_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_dataset_processing_level_fkey on dataset using btree (level_id);


--
-- Name: fki_tile_dataset_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_tile_dataset_idx on tile using btree (dataset_id);


--
-- Name: fki_tile_footprint_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_tile_footprint_idx on tile using btree (x_index, y_index, tile_type_id);


--
-- Name: fki_tile_footprint_tile_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index fki_tile_footprint_tile_type_id_fkey on tile_footprint using btree (tile_type_id);


--
-- Name: satellite_tag_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create unique index satellite_tag_idx on satellite using btree (satellite_tag);


--
-- Name: sensor_name_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create unique index sensor_name_idx on sensor using btree (sensor_name);


--
-- Name: tile_ctime_idx; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

create index tile_ctime_idx on tile using btree (ctime);


--
-- Name: acquisition_footprint_acquisition_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only acquisition_footprint
add constraint acquisition_footprint_acquisition_id_fkey foreign key (acquisition_id) references acquisition (acquisition_id) on update cascade;


--
-- Name: acquisition_footprint_tile_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only acquisition_footprint
add constraint acquisition_footprint_tile_type_id_fkey foreign key (tile_type_id) references tile_type (tile_type_id);


--
-- Name: acquisition_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only acquisition
add constraint acquisition_satellite_id_fkey foreign key (satellite_id) references satellite (satellite_id) on update cascade;


--
-- Name: acquisition_sensor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only acquisition
add constraint acquisition_sensor_id_fkey foreign key (sensor_id, satellite_id) references sensor (sensor_id, satellite_id) on update cascade;


--
-- Name: band_band_type_id_key; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band
add constraint band_band_type_id_key foreign key (band_type_id) references band_type (band_type_id) on update cascade;


--
-- Name: band_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band
add constraint band_satellite_id_fkey foreign key (satellite_id, sensor_id) references sensor (satellite_id, sensor_id) on update cascade;


--
-- Name: band_source_band_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_source
add constraint band_source_band_id_fkey foreign key (band_id) references band (band_id) on update cascade;


--
-- Name: band_source_level_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_source
add constraint band_source_level_id_fkey foreign key (level_id) references processing_level (level_id) on update cascade;


--
-- Name: band_source_tile_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_source
add constraint band_source_tile_type_id_fkey foreign key (tile_type_id) references tile_type (tile_type_id) on update cascade;


--
-- Name: dataset_level_id_key; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only dataset
add constraint dataset_level_id_key foreign key (level_id) references processing_level (level_id) on update cascade;


--
-- Name: fk_band_adjustment_band; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_adjustment
add constraint fk_band_adjustment_band foreign key (band_id) references band (band_id) on update cascade on delete cascade;


--
-- Name: fk_band_adjustment_band_lookup_scheme; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_adjustment
add constraint fk_band_adjustment_band_lookup_scheme foreign key (lookup_scheme_id) references band_lookup_scheme (lookup_scheme_id) on update cascade on delete cascade;


--
-- Name: fk_band_equivalent_band_lookup_scheme; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_equivalent
add constraint fk_band_equivalent_band_lookup_scheme foreign key (lookup_scheme_id) references band_lookup_scheme (lookup_scheme_id);


--
-- Name: fk_band_equivalent_band_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only band_equivalent
add constraint fk_band_equivalent_band_type foreign key (band_type_id) references band_type (band_type_id);


--
-- Name: lock_lock_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only lock
add constraint lock_lock_type_id_fkey foreign key (lock_type_id) references lock_type (lock_type_id);


--
-- Name: sensor_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only sensor
add constraint sensor_satellite_id_fkey foreign key (satellite_id) references satellite (satellite_id) on update cascade;


--
-- Name: tile_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only tile
add constraint tile_dataset_id_fkey foreign key (dataset_id) references dataset (dataset_id) on update cascade;


--
-- Name: tile_footprint_tile_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only tile_footprint
add constraint tile_footprint_tile_type_id_fkey foreign key (tile_type_id) references tile_type (tile_type_id) on update cascade;


--
-- Name: tile_tile_class_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only tile
add constraint tile_tile_class_id_fkey foreign key (tile_class_id) references tile_class (tile_class_id);


--
-- Name: tile_tile_footprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

alter table only tile
add constraint tile_tile_footprint_fkey foreign key (x_index, y_index, tile_type_id) references tile_footprint (x_index, y_index, tile_type_id) on update cascade;


--
-- Name: acquisition; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table acquisition from public;
revoke all on table acquisition from cube_admin;
grant all on table acquisition to cube_admin;
grant all on table acquisition to cube_admin_group;
grant select on table acquisition to cube_user_group;


--
-- Name: acquisition_footprint; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table acquisition_footprint from public;
revoke all on table acquisition_footprint from cube_admin;
grant all on table acquisition_footprint to cube_admin;
grant all on table acquisition_footprint to cube_admin_group;
grant select on table acquisition_footprint to cube_user_group;


--
-- Name: band; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band from public;
revoke all on table band from cube_admin;
grant all on table band to cube_admin;
grant all on table band to cube_admin_group;
grant select on table band to cube_user_group;


--
-- Name: band_adjustment; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_adjustment from public;
revoke all on table band_adjustment from cube_admin;
grant all on table band_adjustment to cube_admin;
grant all on table band_adjustment to cube_admin_group;
grant select on table band_adjustment to cube_user_group;


--
-- Name: band_equivalent; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_equivalent from public;
revoke all on table band_equivalent from cube_admin;
grant all on table band_equivalent to cube_admin;
grant all on table band_equivalent to cube_admin_group;
grant select on table band_equivalent to cube_user_group;


--
-- Name: band_lookup_scheme; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_lookup_scheme from public;
revoke all on table band_lookup_scheme from cube_admin;
grant all on table band_lookup_scheme to cube_admin;
grant all on table band_lookup_scheme to cube_admin_group;
grant select on table band_lookup_scheme to cube_user_group;


--
-- Name: band_source; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_source from public;
revoke all on table band_source from cube_admin;
grant all on table band_source to cube_admin;
grant all on table band_source to cube_admin_group;
grant select on table band_source to cube_user_group;


--
-- Name: band_type; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_type from public;
revoke all on table band_type from cube_admin;
grant all on table band_type to cube_admin;
grant all on table band_type to cube_admin_group;
grant select on table band_type to cube_user_group;


--
-- Name: processing_level; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table processing_level from public;
revoke all on table processing_level from cube_admin;
grant all on table processing_level to cube_admin;
grant all on table processing_level to cube_admin_group;
grant select on table processing_level to cube_user_group;


--
-- Name: satellite; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table satellite from public;
revoke all on table satellite from cube_admin;
grant all on table satellite to cube_admin;
grant all on table satellite to cube_admin_group;
grant select on table satellite to cube_user_group;


--
-- Name: sensor; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table sensor from public;
revoke all on table sensor from cube_admin;
grant all on table sensor to cube_admin;
grant all on table sensor to cube_admin_group;
grant select on table sensor to cube_user_group;


--
-- Name: band_lookup; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table band_lookup from public;
revoke all on table band_lookup from cube_admin;
grant all on table band_lookup to cube_admin;
grant all on table band_lookup to cube_admin_group;
grant select on table band_lookup to cube_user_group;


--
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table dataset from public;
revoke all on table dataset from cube_admin;
grant all on table dataset to cube_admin;
grant all on table dataset to cube_admin_group;
grant select on table dataset to cube_user_group;


--
-- Name: lock; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table lock from public;
revoke all on table lock from cube_admin;
grant all on table lock to cube_admin;
grant all on table lock to cube_admin_group;
grant select, insert, delete, truncate, update on table lock to cube_user_group;


--
-- Name: lock_type; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table lock_type from public;
revoke all on table lock_type from cube_admin;
grant all on table lock_type to cube_admin;
grant all on table lock_type to cube_admin_group;
grant select on table lock_type to cube_user_group;


--
-- Name: tile; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table tile from public;
revoke all on table tile from cube_admin;
grant all on table tile to cube_admin;
grant all on table tile to cube_admin_group;
grant select on table tile to cube_user_group;


--
-- Name: tile_class; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table tile_class from public;
revoke all on table tile_class from cube_admin;
grant all on table tile_class to cube_admin;
grant all on table tile_class to cube_admin_group;
grant select on table tile_class to cube_user_group;


--
-- Name: tile_footprint; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table tile_footprint from public;
revoke all on table tile_footprint from cube_admin;
grant all on table tile_footprint to cube_admin;
grant all on table tile_footprint to cube_admin_group;
grant select on table tile_footprint to cube_user_group;


--
-- Name: tile_type; Type: ACL; Schema: public; Owner: cube_admin
--

revoke all on table tile_type from public;
revoke all on table tile_type from cube_admin;
grant all on table tile_type to cube_admin;
grant all on table tile_type to cube_admin_group;
grant select on table tile_type to cube_user_group;


--
-- PostgreSQL database dump complete
--


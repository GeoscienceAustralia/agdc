--
-- PostgreSQL database dump
--

-- Dumped from database version 8.4.4
-- Dumped by pg_dump version 9.2.0
-- Started on 2013-02-23 13:04:21

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

--
-- TOC entry 6 (class 2615 OID 4496004)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: satcat_v10
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO satcat_v10;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 159 (class 1259 OID 4541949)
-- Name: acquisition; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE acquisition (
    acquisition_id bigint NOT NULL,
    satellite_id integer,
    sensor_id integer,
    x_ref smallint,
    y_ref smallint,
    start_datetime timestamp without time zone,
    end_datetime timestamp without time zone,
    ll_lon double precision,
    ll_lat double precision,
    lr_lon double precision,
    lr_lat double precision,
    ul_lon double precision,
    ul_lat double precision,
    ur_lon double precision,
    ur_lat double precision
);


ALTER TABLE public.acquisition OWNER TO satcat_v10;

--
-- TOC entry 1953 (class 0 OID 0)
-- Dependencies: 159
-- Name: TABLE acquisition; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE acquisition IS 'Defines source acquisition independent of processing level';


--
-- TOC entry 158 (class 1259 OID 4541939)
-- Name: acquisition_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE acquisition_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.acquisition_id_seq OWNER TO satcat_v10;

--
-- TOC entry 141 (class 1259 OID 4496005)
-- Name: band_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE band_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.band_id_seq OWNER TO satcat_v10;

--
-- TOC entry 142 (class 1259 OID 4496007)
-- Name: band; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE band (
    band_id integer DEFAULT nextval('band_id_seq'::regclass) NOT NULL,
    sensor_id integer,
    band_name text NOT NULL,
    band_type_id integer,
    file_number smallint,
    resolution double precision,
    min_wavelength double precision,
    max_wavelength double precision,
    file_pattern text,
    satellite_id integer,
    band_tag text
);


ALTER TABLE public.band OWNER TO satcat_v10;

--
-- TOC entry 1954 (class 0 OID 0)
-- Dependencies: 142
-- Name: TABLE band; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE band IS 'Band information';


--
-- TOC entry 161 (class 1259 OID 4542866)
-- Name: band_source; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE band_source (
    tile_type_id bigint NOT NULL,
    band_id integer NOT NULL,
    level_id smallint NOT NULL,
    tile_layer integer
);


ALTER TABLE public.band_source OWNER TO satcat_v10;

--
-- TOC entry 1955 (class 0 OID 0)
-- Dependencies: 161
-- Name: TABLE band_source; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE band_source IS 'Defines which bands will be selected from which datasets and stored in which layer';


--
-- TOC entry 143 (class 1259 OID 4496014)
-- Name: band_type_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE band_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.band_type_id_seq OWNER TO satcat_v10;

--
-- TOC entry 144 (class 1259 OID 4496016)
-- Name: band_type; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE band_type (
    band_type_id integer DEFAULT nextval('band_type_id_seq'::regclass) NOT NULL,
    band_type_name text
);


ALTER TABLE public.band_type OWNER TO satcat_v10;

--
-- TOC entry 1956 (class 0 OID 0)
-- Dependencies: 144
-- Name: TABLE band_type; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE band_type IS 'Band type information. May be reflective, thermal, panchromatic or derived';


--
-- TOC entry 154 (class 1259 OID 4496179)
-- Name: dataset_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE dataset_id_seq
    START WITH 540218
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dataset_id_seq OWNER TO satcat_v10;

--
-- TOC entry 160 (class 1259 OID 4541976)
-- Name: dataset; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE dataset (
    dataset_id bigint DEFAULT nextval('dataset_id_seq'::regclass) NOT NULL,
    acquisition_id bigint,
    dataset_path text,
    level_id smallint,
    dataset_size integer,
    datetime_processed timestamp without time zone,
    crs text,
    ll_x double precision,
    ll_y double precision,
    lr_x double precision,
    lr_y double precision,
    ul_x double precision,
    ul_y double precision,
    ur_x double precision,
    ur_y double precision,
    x_pixels integer,
    y_pixels integer
);


ALTER TABLE public.dataset OWNER TO satcat_v10;

--
-- TOC entry 1957 (class 0 OID 0)
-- Dependencies: 160
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE dataset IS 'Source dataset for tile provenance';


--
-- TOC entry 145 (class 1259 OID 4496023)
-- Name: level_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE level_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.level_id_seq OWNER TO satcat_v10;

--
-- TOC entry 163 (class 1259 OID 4543915)
-- Name: lock; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE lock (
    lock_type_id integer NOT NULL,
    lock_object text NOT NULL,
    lock_owner text,
    lock_status_id integer,
    lock_detail text
);


ALTER TABLE public.lock OWNER TO satcat_v10;

--
-- TOC entry 164 (class 1259 OID 4543934)
-- Name: lock_type; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE lock_type (
    lock_type_id integer,
    lock_type_name text
);


ALTER TABLE public.lock_type OWNER TO satcat_v10;

--
-- TOC entry 146 (class 1259 OID 4496025)
-- Name: processing_level; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE processing_level (
    level_id smallint DEFAULT nextval('level_id_seq'::regclass) NOT NULL,
    level_name text,
    nodata_value bigint,
    resampling_method text,
    level_description text
);


ALTER TABLE public.processing_level OWNER TO satcat_v10;

--
-- TOC entry 1958 (class 0 OID 0)
-- Dependencies: 146
-- Name: TABLE processing_level; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE processing_level IS 'Processing level for source dataset (e.g. L1T, NBAR or PQA)';


--
-- TOC entry 147 (class 1259 OID 4496032)
-- Name: satellite_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE satellite_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.satellite_id_seq OWNER TO satcat_v10;

--
-- TOC entry 148 (class 1259 OID 4496034)
-- Name: satellite; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE satellite (
    satellite_id integer DEFAULT nextval('satellite_id_seq'::regclass) NOT NULL,
    satellite_name text,
    satellite_tag text,
    name_pattern text,
    semi_major_axis double precision,
    radius double precision,
    altitude double precision,
    inclination double precision,
    omega double precision,
    sweep_period double precision,
    format text,
    projection text,
    spectral_filter_file text,
    tle_format text,
    solar_irrad_file text,
    nominal_pixel_degrees double precision
);


ALTER TABLE public.satellite OWNER TO satcat_v10;

--
-- TOC entry 1959 (class 0 OID 0)
-- Dependencies: 148
-- Name: TABLE satellite; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE satellite IS 'Satellite information. Will have one or more sensors';


--
-- TOC entry 149 (class 1259 OID 4496043)
-- Name: sensor_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE sensor_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sensor_id_seq OWNER TO satcat_v10;

--
-- TOC entry 150 (class 1259 OID 4496045)
-- Name: sensor; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE sensor (
    sensor_id integer DEFAULT nextval('sensor_id_seq'::regclass) NOT NULL,
    satellite_id integer NOT NULL,
    sensor_name text,
    description text
);


ALTER TABLE public.sensor OWNER TO satcat_v10;

--
-- TOC entry 1960 (class 0 OID 0)
-- Dependencies: 150
-- Name: TABLE sensor; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE sensor IS 'Sensor information. May have one or more bands';


--
-- TOC entry 156 (class 1259 OID 4541220)
-- Name: tile_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE tile_id_seq
    START WITH 100
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tile_id_seq OWNER TO satcat_v10;

--
-- TOC entry 155 (class 1259 OID 4496262)
-- Name: tile; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE tile (
    tile_id bigint DEFAULT nextval('tile_id_seq'::regclass) NOT NULL,
    x_index integer NOT NULL,
    y_index integer NOT NULL,
    tile_type_id bigint NOT NULL,
    tile_pathname text,
    dataset_id bigint,
    tile_class_id integer,
    tile_size integer,
    ctime timestamp with time zone DEFAULT now()
);


ALTER TABLE public.tile OWNER TO satcat_v10;

--
-- TOC entry 1961 (class 0 OID 0)
-- Dependencies: 155
-- Name: TABLE tile; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE tile IS 'Tile information for individual tiles';


--
-- TOC entry 157 (class 1259 OID 4541627)
-- Name: tile_class; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE tile_class (
    tile_class_id integer NOT NULL,
    tile_class_name text
);


ALTER TABLE public.tile_class OWNER TO satcat_v10;

--
-- TOC entry 1962 (class 0 OID 0)
-- Dependencies: 157
-- Name: TABLE tile_class; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE tile_class IS 'Definitions for different classes of tile
e.g. class 1 indicates a real tile containing data while class 2 indicates an empty tile not stored as a file';


--
-- TOC entry 151 (class 1259 OID 4496065)
-- Name: tile_footprint; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE tile_footprint (
    x_index integer NOT NULL,
    y_index integer NOT NULL,
    tile_type_id bigint NOT NULL,
    x_min double precision,
    y_min double precision,
    x_max double precision,
    y_max double precision
);


ALTER TABLE public.tile_footprint OWNER TO satcat_v10;

--
-- TOC entry 1963 (class 0 OID 0)
-- Dependencies: 151
-- Name: TABLE tile_footprint; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE tile_footprint IS 'Definitions for each unique tile footprint.
Pre-populated to allow simple range queries without calculation';


--
-- TOC entry 152 (class 1259 OID 4496068)
-- Name: tile_type_id_seq; Type: SEQUENCE; Schema: public; Owner: satcat_v10
--

CREATE SEQUENCE tile_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tile_type_id_seq OWNER TO satcat_v10;

--
-- TOC entry 153 (class 1259 OID 4496070)
-- Name: tile_type; Type: TABLE; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE tile_type (
    tile_type_id bigint DEFAULT nextval('tile_type_id_seq'::regclass) NOT NULL,
    tile_type_name text,
    crs text,
    x_origin double precision,
    y_origin double precision,
    x_size double precision,
    y_size double precision,
    x_pixels bigint,
    y_pixels bigint,
    unit text,
    file_format text,
    file_extension text,
    tile_directory text,
    format_options text
);


ALTER TABLE public.tile_type OWNER TO satcat_v10;

--
-- TOC entry 1964 (class 0 OID 0)
-- Dependencies: 153
-- Name: TABLE tile_type; Type: COMMENT; Schema: public; Owner: satcat_v10
--

COMMENT ON TABLE tile_type IS 'Contains a definition for each different tiling scheme.';


SET search_path = ztmp, pg_catalog;

--
-- TOC entry 166 (class 1259 OID 4558636)
-- Name: bad_tiles; Type: TABLE; Schema: ztmp; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE bad_tiles (
    tile_id bigint,
    tile_pathname text
);


ALTER TABLE ztmp.bad_tiles OWNER TO satcat_v10;

--
-- TOC entry 165 (class 1259 OID 4555996)
-- Name: missing_tiles; Type: TABLE; Schema: ztmp; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE missing_tiles (
    x_ref smallint,
    y_ref smallint,
    start_datetime timestamp without time zone,
    end_datetime timestamp without time zone,
    x_index integer,
    y_index integer,
    l1t_tile_count bigint,
    nbar_tile_count bigint,
    pqa_tile_count bigint
);


ALTER TABLE ztmp.missing_tiles OWNER TO satcat_v10;

--
-- TOC entry 162 (class 1259 OID 4543212)
-- Name: missing_tiles1; Type: TABLE; Schema: ztmp; Owner: satcat_v10; Tablespace: 
--

CREATE TABLE missing_tiles1 (
    acquisition_id bigint,
    x_ref smallint,
    y_ref smallint,
    start_datetime timestamp without time zone,
    end_datetime timestamp without time zone,
    tiles_required double precision,
    tile_count bigint
);


ALTER TABLE ztmp.missing_tiles1 OWNER TO satcat_v10;

SET search_path = public, pg_catalog;

--
-- TOC entry 1905 (class 2606 OID 4541953)
-- Name: acquisition_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY acquisition
    ADD CONSTRAINT acquisition_pkey PRIMARY KEY (acquisition_id);


--
-- TOC entry 1907 (class 2606 OID 4541959)
-- Name: acquisition_reference_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY acquisition
    ADD CONSTRAINT acquisition_reference_key UNIQUE (satellite_id, sensor_id, x_ref, y_ref, start_datetime, end_datetime);


--
-- TOC entry 1874 (class 2606 OID 4496085)
-- Name: band_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY band
    ADD CONSTRAINT band_pkey PRIMARY KEY (band_id);


--
-- TOC entry 1929 (class 2606 OID 4542870)
-- Name: band_source_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY band_source
    ADD CONSTRAINT band_source_pkey PRIMARY KEY (tile_type_id, band_id, level_id);


--
-- TOC entry 1878 (class 2606 OID 4496087)
-- Name: band_type_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY band_type
    ADD CONSTRAINT band_type_pkey PRIMARY KEY (band_type_id);


--
-- TOC entry 1910 (class 2606 OID 4542041)
-- Name: dataset_acquisition_id_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_acquisition_id_key UNIQUE (acquisition_id, level_id, crs);


--
-- TOC entry 1917 (class 2606 OID 4541986)
-- Name: dataset_path_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_path_key UNIQUE (dataset_path);


--
-- TOC entry 1919 (class 2606 OID 4541984)
-- Name: dataset_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_pkey PRIMARY KEY (dataset_id);


--
-- TOC entry 1921 (class 2606 OID 4541988)
-- Name: dataset_reference_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_reference_key UNIQUE (acquisition_id, level_id);


--
-- TOC entry 1931 (class 2606 OID 4543922)
-- Name: lock_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY lock
    ADD CONSTRAINT lock_pkey PRIMARY KEY (lock_type_id, lock_object);


--
-- TOC entry 1880 (class 2606 OID 4496091)
-- Name: processing_level_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY processing_level
    ADD CONSTRAINT processing_level_pkey PRIMARY KEY (level_id);


--
-- TOC entry 1882 (class 2606 OID 4496093)
-- Name: satellite_name_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY satellite
    ADD CONSTRAINT satellite_name_key UNIQUE (satellite_name);


--
-- TOC entry 1884 (class 2606 OID 4496095)
-- Name: satellite_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY satellite
    ADD CONSTRAINT satellite_pkey PRIMARY KEY (satellite_id);


--
-- TOC entry 1887 (class 2606 OID 4542903)
-- Name: satellite_tag_key; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY satellite
    ADD CONSTRAINT satellite_tag_key UNIQUE (satellite_tag);


--
-- TOC entry 1890 (class 2606 OID 4496217)
-- Name: sensor_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY sensor
    ADD CONSTRAINT sensor_pkey PRIMARY KEY (satellite_id, sensor_id);


--
-- TOC entry 1903 (class 2606 OID 4541634)
-- Name: tile_class_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY tile_class
    ADD CONSTRAINT tile_class_pkey PRIMARY KEY (tile_class_id);


--
-- TOC entry 1893 (class 2606 OID 4496105)
-- Name: tile_footprint_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY tile_footprint
    ADD CONSTRAINT tile_footprint_pkey PRIMARY KEY (x_index, y_index, tile_type_id);


--
-- TOC entry 1901 (class 2606 OID 4496269)
-- Name: tile_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY tile
    ADD CONSTRAINT tile_pkey PRIMARY KEY (tile_id);


--
-- TOC entry 1895 (class 2606 OID 4496109)
-- Name: tile_type_pkey; Type: CONSTRAINT; Schema: public; Owner: satcat_v10; Tablespace: 
--

ALTER TABLE ONLY tile_type
    ADD CONSTRAINT tile_type_pkey PRIMARY KEY (tile_type_id);


--
-- TOC entry 1911 (class 1259 OID 4541999)
-- Name: dataset_dataset_path_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE UNIQUE INDEX dataset_dataset_path_idx ON dataset USING btree (dataset_path);


--
-- TOC entry 1912 (class 1259 OID 4542000)
-- Name: dataset_ll_x_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ll_x_idx ON dataset USING btree (ll_x);


--
-- TOC entry 1913 (class 1259 OID 4542001)
-- Name: dataset_ll_y_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ll_y_idx ON dataset USING btree (ll_y);


--
-- TOC entry 1914 (class 1259 OID 4542002)
-- Name: dataset_lr_x_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_lr_x_idx ON dataset USING btree (ll_y);


--
-- TOC entry 1915 (class 1259 OID 4542003)
-- Name: dataset_lr_y_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_lr_y_idx ON dataset USING btree (ll_y);


--
-- TOC entry 1922 (class 1259 OID 4542004)
-- Name: dataset_ul_x_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ul_x_idx ON dataset USING btree (ul_x);


--
-- TOC entry 1923 (class 1259 OID 4542005)
-- Name: dataset_ul_y_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ul_y_idx ON dataset USING btree (ul_y);


--
-- TOC entry 1924 (class 1259 OID 4542006)
-- Name: dataset_ur_x_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ur_x_idx ON dataset USING btree (ur_x);


--
-- TOC entry 1925 (class 1259 OID 4542007)
-- Name: dataset_ur_y_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX dataset_ur_y_idx ON dataset USING btree (ur_y);


--
-- TOC entry 1908 (class 1259 OID 4542027)
-- Name: fk_acquisition_sensor_fki; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fk_acquisition_sensor_fki ON acquisition USING btree (sensor_id, satellite_id);


--
-- TOC entry 1896 (class 1259 OID 4541640)
-- Name: fk_tile_class_id_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fk_tile_class_id_idx ON tile USING btree (tile_class_id);


--
-- TOC entry 1875 (class 1259 OID 4496111)
-- Name: fki_band_band_type_id_key; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_band_band_type_id_key ON band USING btree (band_type_id);


--
-- TOC entry 1876 (class 1259 OID 4496244)
-- Name: fki_band_sensor_id_key; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_band_sensor_id_key ON band USING btree (satellite_id, sensor_id);


--
-- TOC entry 1926 (class 1259 OID 4542009)
-- Name: fki_dataset_acquisition_fkey; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_dataset_acquisition_fkey ON dataset USING btree (acquisition_id);


--
-- TOC entry 1927 (class 1259 OID 4542008)
-- Name: fki_dataset_processing_level_fkey; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_dataset_processing_level_fkey ON dataset USING btree (level_id);


--
-- TOC entry 1897 (class 1259 OID 4496291)
-- Name: fki_tile_dataset_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_tile_dataset_idx ON tile USING btree (dataset_id);


--
-- TOC entry 1898 (class 1259 OID 4496292)
-- Name: fki_tile_footprint_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_tile_footprint_idx ON tile USING btree (x_index, y_index, tile_type_id);


--
-- TOC entry 1891 (class 1259 OID 4496115)
-- Name: fki_tile_footprint_tile_type_id_fkey; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX fki_tile_footprint_tile_type_id_fkey ON tile_footprint USING btree (tile_type_id);


--
-- TOC entry 1885 (class 1259 OID 4542901)
-- Name: satellite_tag_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE UNIQUE INDEX satellite_tag_idx ON satellite USING btree (satellite_tag);


--
-- TOC entry 1888 (class 1259 OID 4542887)
-- Name: sensor_name_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE UNIQUE INDEX sensor_name_idx ON sensor USING btree (sensor_name);


--
-- TOC entry 1899 (class 1259 OID 4557301)
-- Name: tile_ctime_idx; Type: INDEX; Schema: public; Owner: satcat_v10; Tablespace: 
--

CREATE INDEX tile_ctime_idx ON tile USING btree (ctime);


--
-- TOC entry 1939 (class 2606 OID 4542945)
-- Name: acquisition_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY acquisition
    ADD CONSTRAINT acquisition_satellite_id_fkey FOREIGN KEY (satellite_id) REFERENCES satellite(satellite_id) ON UPDATE CASCADE;


--
-- TOC entry 1940 (class 2606 OID 4542950)
-- Name: acquisition_sensor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY acquisition
    ADD CONSTRAINT acquisition_sensor_id_fkey FOREIGN KEY (sensor_id, satellite_id) REFERENCES sensor(sensor_id, satellite_id) ON UPDATE CASCADE;


--
-- TOC entry 1932 (class 2606 OID 4541079)
-- Name: band_band_type_id_key; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY band
    ADD CONSTRAINT band_band_type_id_key FOREIGN KEY (band_type_id) REFERENCES band_type(band_type_id) ON UPDATE CASCADE;


--
-- TOC entry 1933 (class 2606 OID 4541084)
-- Name: band_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY band
    ADD CONSTRAINT band_satellite_id_fkey FOREIGN KEY (satellite_id, sensor_id) REFERENCES sensor(satellite_id, sensor_id) ON UPDATE CASCADE;


--
-- TOC entry 1943 (class 2606 OID 4542930)
-- Name: band_source_band_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY band_source
    ADD CONSTRAINT band_source_band_id_fkey FOREIGN KEY (band_id) REFERENCES band(band_id) ON UPDATE CASCADE;


--
-- TOC entry 1944 (class 2606 OID 4542935)
-- Name: band_source_level_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY band_source
    ADD CONSTRAINT band_source_level_id_fkey FOREIGN KEY (level_id) REFERENCES processing_level(level_id) ON UPDATE CASCADE;


--
-- TOC entry 1945 (class 2606 OID 4542940)
-- Name: band_source_tile_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY band_source
    ADD CONSTRAINT band_source_tile_type_id_fkey FOREIGN KEY (tile_type_id) REFERENCES tile_type(tile_type_id) ON UPDATE CASCADE;


--
-- TOC entry 1941 (class 2606 OID 4541989)
-- Name: dataset_acquisition_id_key; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_acquisition_id_key FOREIGN KEY (acquisition_id) REFERENCES acquisition(acquisition_id) ON UPDATE CASCADE;


--
-- TOC entry 1942 (class 2606 OID 4541994)
-- Name: dataset_level_id_key; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT dataset_level_id_key FOREIGN KEY (level_id) REFERENCES processing_level(level_id) ON UPDATE CASCADE;


--
-- TOC entry 1934 (class 2606 OID 4541089)
-- Name: sensor_satellite_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY sensor
    ADD CONSTRAINT sensor_satellite_id_fkey FOREIGN KEY (satellite_id) REFERENCES satellite(satellite_id) ON UPDATE CASCADE;


--
-- TOC entry 1938 (class 2606 OID 4542012)
-- Name: tile_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY tile
    ADD CONSTRAINT tile_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id) ON UPDATE CASCADE;


--
-- TOC entry 1935 (class 2606 OID 4496159)
-- Name: tile_footprint_tile_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY tile_footprint
    ADD CONSTRAINT tile_footprint_tile_type_id_fkey FOREIGN KEY (tile_type_id) REFERENCES tile_type(tile_type_id) ON UPDATE CASCADE;


--
-- TOC entry 1937 (class 2606 OID 4541635)
-- Name: tile_tile_class_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY tile
    ADD CONSTRAINT tile_tile_class_id_fkey FOREIGN KEY (tile_class_id) REFERENCES tile_class(tile_class_id);


--
-- TOC entry 1936 (class 2606 OID 4541109)
-- Name: tile_tile_footprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: satcat_v10
--

ALTER TABLE ONLY tile
    ADD CONSTRAINT tile_tile_footprint_fkey FOREIGN KEY (x_index, y_index, tile_type_id) REFERENCES tile_footprint(x_index, y_index, tile_type_id) ON UPDATE CASCADE;


--
-- TOC entry 1952 (class 0 OID 0)
-- Dependencies: 7
-- Name: public; Type: ACL; Schema: -; Owner: satcat_v10
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM satcat_v10;
GRANT ALL ON SCHEMA public TO satcat_v10;
GRANT USAGE ON SCHEMA public TO satcat_v10ro;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2013-02-23 13:04:30

--
-- PostgreSQL database dump complete
--


-- acquisition end date indices

create index acquisition_end_datetime_idx on acquisition (end_datetime) ;
create index acquisition_end_datetime_month_idx on acquisition (extract(month from end_datetime)) ;

-- tile footprint spatial index

create index tile_footprint_bbox_idx on tile_footprint using gist (bbox) ;




alter table dataset
    add constraint acquisition_ref
    foreign key (acquisition_id) references acquisition;


-- The Modis ingester contains multiple datasets of different processing levels in a single NetCDF file.
-- Replace the unique path constraint with a unique (path, processing level) constraint.

alter table dataset
    drop constraint dataset_path_key;

drop index dataset_dataset_path_idx;

alter table dataset
    add constraint dataset_path_level
    unique (dataset_path, level_id);

Modis Ingestion
===============

Ingest Modis datasets in NetCDF format.

Example Usage
-------------

    agdc-ingest-modis -C agdc-test-environment.conf \
        --source /g/data/v10/projects/ingest_test_data/input/modis
         

Datasets
--------

The modis ingester searches for any files matching `MOD\*.nc`.

Example source:

    .
    `-- 2010365
        |-- MOD09_L2.2010365.0135.20130130162407.remapped_swath_500mbands_0.005deg.nc
        |-- MOD09_L2.2010365.0315.20130130162407.remapped_swath_500mbands_0.005deg.nc
        |-- MOD09_L2.2010365.2300.20130130162407.remapped_swath_500mbands_0.005deg.nc
        |-- orbit_58697.logs.tgz
        |-- orbit_58698.logs.tgz
        `-- orbit_58710.logs.tgz



Water Observations from Space (WOfS) Ingestion
==============================================

The WOfS ingester differs from Landsat and Modis, in that it ingests tiles output
from the Data Cube rather than from external sources.

These outputs are already in the correct projection and tile bounds, and so can be ingested in-place
without modification. The ingester indexes the datasets, but does not copy them, and so the source 
location is expected to be permanent.

Example Usage
-------------

    agdc-ingest-wofs -C agdc-test-environment.conf \
        --source /g/data/v10/projects/ingest_test_data/wofs
         

Datasets
--------

The WOfS ingester searches for any files matching `\*_WATER_\*.tif`

Example:

    .
    └── LS7_ETM_WATER_154_-026_2012-05-18T23-36-13.518391.tif


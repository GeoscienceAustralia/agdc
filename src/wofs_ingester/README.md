
## WOfS Ingestion

Ingest WOfS tiles into the Data Cube.

### Example usage

Ingest all datasets within a folder (searched recursively):

    python -m agdc.wofs_ingester -C datacube.conf --source /path/to/datasets

Or a specific dataset:

    python -m agdc.wofs_ingester -C datacube.conf --source LS7_ETM_WATER_140_-027_2013-08-18T00-26-16.880864.tif



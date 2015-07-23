 

Dataset Ingestion
=================

Purpose
-------

The scripts in this package ingest various datasets into the
Australian Geoscience Data Cube (AGDC). They read the
metadata from the nominated dataset(s) and check this against the
metadata held in the AGDC database to detect whether each source dataset
should be ingested.

If ingestion is required, then the script prepares an intermediate
GDAL-readable band-stack file in the native dataset projection in
preparation for reprojection, resampling and tiling. This intermediate
data file provides an ideal interface to a generic ingestion process
back-end. The subsequent reprojection, resampling and tiling of the
intermediate file, along with the required cataloguing of dataset and
tile entities, then takes place.

There is no impediment to appending the ingestion to the scene
processing chain at the NCI so a scene could be ingested into the AGDC
as soon as it is created.

Background Information
----------------------

The Landsat ingester was written by Matthew Hoyles and
Matthew Hardy in the first half of 2014. The Landsat-specific portions
of the code are specialisations of generic classes, so this
implementation serves as a template for ingesters for other dataset
types.

Basic Usage
-----------

### Database 

An AGDC database is required, whose setup is detailed in [the database readme](../../database/README.md).

### Config file

Sample:
 
    # AGDC configuration file for testing Landsat 8 ingestion

    [datacube]
    default_tile_type_id = 1
    
    # Database connection parameters
    host = 130.56.244.226
    port = 6432
    dbname = datacube
    user = cube_admin
    password = cube_admin
    
    # User-specific temporary directory for scratch files
    temp_dir = /data/tmp
    
    # Root directory for tiles
    tile_root = /data/tiles
    
    # Dataset filter parameters for ingestion.
    start_date = 01/01/2009
    end_date = 30/04/2009
    min_path = 88
    max_path = 115
    min_row = 66
    max_row = 91
    
    # List of tile types
    tile_types = [1]

### Quick example


When installed as a python package, `agdc-ingest-*` commands will be available in the shell:
    
    $ agdc-ingest-landsat 
    usage: agdc-ingest-landsat [-h] [-C CONFIG_FILE] [-d] [--fastfilter]
                               [--synctime SYNC_TIME] [--synctype SYNC_TYPE]
                               --source SOURCE_DIR [--followsymlinks]
    agdc-ingest-landsat: error: argument --source is required
    $

Example ingestion of all Landsat scenes within a directory (using config file `local.conf`):
 
    $ agdc-ingest-landsat -d -C local.conf --source /data/inputs/ls8/nbar/

### Ingesters

See the README files of each individual ingester for usage instructions and limitations. 

- [Landsat](landsat/README.md)
- [Modis](modis/README.md)
- [WOfS](wofs/README.md)

NCI
===

A module is available for use within the NCI environment.

Ensure the agdc project is on your module path:

    export MODULEPATH="/projects/el8/opt/modules/modulefiles:$MODULEPATH"
    
Load and run:
  
    $ module load agdc
    $ agdc-ingest-landsat ... 

This will load the latest stable release of AGDC. If you wish for more version stability, it's
 advised to always load with a specific version number:

    $ module load agdc/1.2.0

 

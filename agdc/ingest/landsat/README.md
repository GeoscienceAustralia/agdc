
Landsat Ingestion
-----------------

Example usage
-------------

    agdc-ingest-landsat --config agdc_ls8_test.conf --source \
        /g/data/rs0/scenes/ARG25_V0.0/2014-05/LS8_OLI_TIRS_NBAR_P54_GANBAR01-002_115_077_20140507
        /g/data/rs0/scenes/ARG25_V0.0/2014-05/LS8_OLI_TIRS_NBAR_P54_GANBAR01-002_115_078_20140507

Extra Parameters
----------------

There are additional command line options, but they are primarily used for
testing and debugging. 

Usage instructions are as follows:

    usage: agdc-ingest-landsat [-h] [-C CONFIG_FILE] [-d] --source SOURCE_DIR

                       [--followsymlinks] [--fastfilter] [--synctime SYNC_TIME]

                       [--synctype SYNC_TYPE]

    optional arguments:

      -h, --help            show this help message and exit

      -C CONFIG_FILE, --config CONFIG_FILE

                            LandsatIngester configuration file

      -d, --debug           Debug mode flag

      --source SOURCE_DIR   Source root directory containing datasets

      --followsymlinks      Follow symbolic links when finding datasets to ingest

      --fastfilter          Filter datasets using filename patterns.

      --synctime SYNC_TIME  Synchronize parallel ingestions at the given time in

                            seconds after 01/01/1970

      --synctype SYNC_TYPE  Type of transaction to syncronize with synctime, one

                            of "cataloging", "tiling", or "mosaicking".


Dataset Requirements
--------------------

The Landsat ingester has been built to follow the in-house dataset structures within GA.

Datasets are found based on folder naming conventions. The ingester
expects a 'scene01' folder within each dataset containing each band as an individual GeoTIFF. 

The dataset folder name is read to perform filtering (see above), and is
 expected to be in GA's dataset ID format:

    <Sat>_<Sensor>_<Level>_<Level Code>_<Product_Code>-<Groundstation>_<Path>_<Row>_YYYYMMDD
    
However, this ingester ignores everything but the last three fields:

path, row, date: `'_\d{3}_\d{3}_\d{4}\d{2}\d{2}$'`

See the following section for specific examples.

### GA NBAR (Geoscience Australia's Surface Reflectance)

Example NBAR package layouts (LS5, 7 and 8):
        
    .
    |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228
    |   |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228.jpg
    |   |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_FR.jpg
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B10.tif
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B20.tif
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B30.tif
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B40.tif
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B50.tif
    |       |-- LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228_B70.tif
    |       `-- report.txt
    |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225
    |   |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225.jpg
    |   |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_FR.jpg
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B10.tif
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B20.tif
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B30.tif
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B40.tif
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B50.tif
    |       |-- LS7_ETM_NBAR_P54_GANBAR01-002_100_080_20001225_B70.tif
    |       `-- report.txt
    `-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012
        |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012.jpg
        |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_FR.jpg
        |-- md5sum.txt
        |-- metadata.xml
        `-- scene01
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B1.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B2.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B3.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B4.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B5.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B6.tif
            |-- LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B7.tif
            `-- report.txt



### GA Pixel Quality

Example LS5, 7 and 8:

    .
    |-- LS5_TM_PQ_P55_GAPQ01-002_112_080_20080330
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- ACCA_CLOUD_SHADOW_LOGFILE.txt
    |       |-- ACCA_LOGFILE.txt
    |       |-- FMASK_CLOUD_SHADOW_LOGFILE.txt
    |       |-- FMASK_LOGFILE.txt
    |       `-- LS5_TM_PQ_P55_GAPQ01-002_112_080_20080330_1111111111111100.tif
    |-- LS7_ETM_PQ_P55_GAPQ01-002_100_080_20000124
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- ACCA_CLOUD_SHADOW_LOGFILE.txt
    |       |-- ACCA_LOGFILE.txt
    |       |-- FMASK_CLOUD_SHADOW_LOGFILE.txt
    |       |-- FMASK_LOGFILE.txt
    |       `-- LS7_ETM_PQ_P55_GAPQ01-002_100_080_20000124_1111111111111100.tif
    `-- LS8_OLI_TIRS_PQ_P55_GAPQ01-032_114_075_20140804
        |-- md5sum.txt
        |-- metadata.xml
        `-- scene01
            |-- ACCA_CLOUD_SHADOW_LOGFILE.txt
            |-- ACCA_LOGFILE.txt
            |-- FMASK_CLOUD_SHADOW_LOGFILE.txt
            |-- FMASK_LOGFILE.txt
            `-- LS8_OLI_TIRS_PQ_P55_GAPQ01-032_114_075_20140804_1111111111111100.tif

### GA Fractional Cover
    
Example LS5, 7 and 8:

    .
    |-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901
    |   |-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901.jpg
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901_BS.tif
    |       |-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901_NPV.tif
    |       |-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901_PV.tif
    |       `-- LS5_TM_FC_P54_GAFC01-002_106_080_19980901_UE.tif
    |-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819
    |   |-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819.jpg
    |   |-- md5sum.txt
    |   |-- metadata.xml
    |   `-- scene01
    |       |-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819_BS.tif
    |       |-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819_NPV.tif
    |       |-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819_PV.tif
    |       `-- LS7_ETM_FC_P54_GAFC01-002_115_078_20140819_UE.tif
    `-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827
        |-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827.jpg
        |-- md5sum.txt
        |-- metadata.xml
        `-- scene01
            |-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827_BS.tif
            |-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827_NPV.tif
            |-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827_PV.tif
            `-- LS8_OLI_TIRS_FC_P54_GAFC01-032_115_075_20140827_UE.tif
    

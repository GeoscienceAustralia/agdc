
.. toctree::
   :maxdepth: 2


API Command Line Tools
======================

There are also a set of “tools” – i.e. packaged executables – aimed at the NPP (Non-Python “People”):

* Retrieve pixel time series
* Retrieve dataset
* Retrieve dataset time series
* Retrieve dataset stack
* Retrieve aoi time series
* Summarise dataset time series
* …

Retrieve Pixel Time Series
--------------------------

The `Retrieve Pixel Time Series` tool extracts the values of a given pixel from the specified dataset over time to a
CSV file.  Pixel quality masking can be applied.  Acquisitions that are completely masked can be output or omitted::

    $ retrieve_pixel_time_series.py -h
    usage: retrieve_pixel_time_series.py
           [-h]
           [--quiet | --verbose]
           [--acq-min ACQ_MIN] [--acq-max ACQ_MAX]
           [--satellite LS5 LS7 LS8 [LS5 LS7 LS8 ...]]
           [--mask-pqa-apply]
           [--mask-pqa-mask PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR [PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR ...]]
           [--mask-wofs-apply]
           [--mask-wofs-mask DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET [DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET ...]]
           --lat LATITUDE --lon LONGITUDE
           [--hide-no-data]
           --dataset-type ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI
           [--bands-all | --bands-common]
           [--delimiter DELIMITER]
           [--output-directory OUTPUT_DIRECTORY]
           [--overwrite]

    Time Series Retrieval

    optional arguments:
      -h, --help            show this help message and exit
      --quiet               Less output
      --verbose             More output
      --acq-min ACQ_MIN     Acquisition Date
      --acq-max ACQ_MAX     Acquisition Date
      --satellite LS5 LS7 LS8 [LS5 LS7 LS8 ...]
                            The satellite(s) to include
      --mask-pqa-apply      Apply PQA mask
      --mask-pqa-mask PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR [PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR ...]
                            The PQA mask to apply
      --mask-wofs-apply     Apply WOFS mask
      --mask-wofs-mask DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET [DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET ...]
                            The WOFS mask to apply
      --lat LATITUDE        Latitude value of pixel
      --lon LONGITUDE       Longitude value of pixel
      --hide-no-data        Don't output records that are completely no data value(s)
      --dataset-type ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI
                            The type of dataset from which values will be retrieved
      --bands-all           Retrieve all bands with NULL values where the band is N/A
      --bands-common        Retrieve only bands in common across all satellites
      --delimiter DELIMITER
                            Field delimiter in output file
      --output-directory OUTPUT_DIRECTORY
                            Output directory
      --overwrite           Over write existing output file

.. NOTE::

    The ACQ DATE parameters support dates being specified as ``YYYY`` or ``YYYY-MM`` or ``YYYY-MM-DD``.

    The MIN parameter maps the value down - i.e. `2000` -> `2000-01-01` and `2000-12` -> `2012-12-01`

    The MAX parameter maps the value up - i.e. `2000` -> `2000-12-31` and `2000-01` -> `2012-01-31`

Example Uses
++++++++++++

For example to retrieve the NBAR values::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-01 01:58:47,-999,-999,-999,-999,-999,-999
    LS7,2013-12-10 01:53:02,-999,-999,-999,-999,-999,-999
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS7,2013-12-26 01:53:05,-999,-999,-999,-999,-999,-999

and again applying pixel quality masking (defaults to selecting CLEAR pixels but can be changed via the ``--pqa-mask`` argument)::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-01 01:58:47,-999,-999,-999,-999,-999,-999
    LS7,2013-12-10 01:53:02,-999,-999,-999,-999,-999,-999
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS7,2013-12-26 01:53:05,-999,-999,-999,-999,-999,-999

and again omitting no data values::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626

and mixing and matching Landsat 5, 7 and 8 (i.e. data sets that have different band combinations)::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data --satellite LS7 LS8

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2,COASTAL_AEROSOL
    LS8,2013-12-02 01:57:55,507,871,1630,2633,3236,2513,493
    LS8,2013-12-09 02:04:05,448,820,1592,2596,3260,2550,430
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626,
    LS8,2013-12-18 01:57:47,496,847,1605,2638,3252,2509,485

which outputs an **empty** value where the band is not applicable; or to only include the bands common across all satellites::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data --satellite LS7 LS8 --bands-common

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS8,2013-12-02 01:57:55,507,871,1630,2633,3236,2513
    LS8,2013-12-09 02:04:05,448,820,1592,2596,3260,2550
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS8,2013-12-18 01:57:47,496,847,1605,2638,3252,2509

To retrieve WOFS water extent values for a pixel::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type WATER --quiet

    SATELLITE,ACQUISITION DATE,WATER
    LS7,2013-12-01 01:58:47,Saturation/Contiguity,2
    LS7,2013-12-10 01:53:02,Saturation/Contiguity,2
    LS7,2013-12-17 01:58:47,Dry,0
    LS7,2013-12-26 01:53:05,Saturation/Contiguity,2

Retrieving a "batch" of pixel time series...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

If you have a "batch" of pixels each of which you would like to retrieve a time series for, this can be scripted something like...

Create a text file containing the pixel locations (in this instance as lat/lon pairs separated by white space)

.. code-block:: txt
    :caption: pixels.txt

    -20.00  120.00
    -21.25  121.25
    -22.50  122.50

Run a little (bash) while loop to retrieve the values to files...

.. code-block:: bash

    $ while read lat lon; do echo "Retrieving time series for $lon / $lat" ; retrieve_pixel_time_series.py --lon $lon --lat $lat --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --output-directory $PWD ; done <pixels.txt

    Retrieving time series for 120.00 / -20.00
    Retrieving time series for 121.25 / -21.25
    Retrieving time series for 122.50 / -22.50

and then you should have the following output files...

.. code-block:: bash

    $ ls -l

    -rw-r----- 1 sjo547 v10  345 Jan 20 13:11 LS7_NBAR_120.00000_-20.00000_2013-12-01_2013-12-31.csv
    -rw-r----- 1 sjo547 v10  221 Jan 20 13:11 LS7_NBAR_121.25000_-21.25000_2013-12-01_2013-12-31.csv
    -rw-r----- 1 sjo547 v10  221 Jan 20 13:11 LS7_NBAR_122.50000_-22.50000_2013-12-01_2013-12-31.csv

VERY Rudimentary Pixel Change Detection Using WOFS Water Extents...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

A **VERY** basic form of change detection for a pixel can be done quite simply with the outputs from the the pixel drill by:

#. Executing the pixel drill for your chosen pixel
#. Filter the list back to only WATER and NON WATER observations
#. Filter the list back to only changes from WATER to NON WATER and vice versa

For example to get the change history for the ``150.07994`` / ``-36.36939`` pixel...

.. code-block:: bash

    $ retrieve_pixel_time_series.py --lon 150.07994 --lat -36.36939 --acq-min 1987 --acq-max 2015 --satellite LS5 LS7 --dataset-type WATER --quiet --output-directory $PWD

Then to filter back to only WATER and NON WATER observations...

.. code-block:: bash

    $ grep -e 'Wet\|Dry' LS_WOFS_150.07994_-36.36939_1987-01-01_2015-12-31.csv | sort -u -t ',' -k 2,2 >LS_WOFS_150.07994_-36.36939_1987-01-01_2015-12-31.filtered.sorted.csv

This drops back from the initial 940 observations to 215 observations.

Then to filter back to only observations that indication a change...

.. code-block:: bash

    $ IFS="," ; unset pclass ; while read sat acq class iclass; do if [ -z "$pclass" ] || [ "$class" != "$pclass" ]; then echo "$acq,$pclass,$class"; pclass="$class" ; fi; done <LS_WOFS_150.07994_-36.36939_1987-01-01_2015-12-31.filtered.sorted.csv >LS_WOFS_150.07994_-36.36939_1987-01-01_2015-12-31.changes.csv

This filters back to 19 "change" observations.

This can be imported into Excel, for example, and a couple of calculated cells added and so on...

+----------+----+----+------------+--------------+-------------+
|   DATE   |FROM| TO |DAYS BETWEEN|MONTHS BETWEEN|YEARS BETWEEN|
+==========+====+====+============+==============+=============+
|1988-02-25|    | Dry|            |              |             |
+----------+----+----+------------+--------------+-------------+
|1988-09-29| Dry| Wet|         217|             7|            1|
+----------+----+----+------------+--------------+-------------+
|1998-01-19| Wet| Dry|       3,399|           113|            9|
+----------+----+----+------------+--------------+-------------+
|2002-03-27| Dry| Wet|       1,528|            51|            4|
+----------+----+----+------------+--------------+-------------+
|2002-04-05| Wet| Dry|           9|             0|            0|
+----------+----+----+------------+--------------+-------------+
|2002-09-19| Dry| Wet|         167|             6|            0|
+----------+----+----+------------+--------------+-------------+
|2002-10-05| Wet| Dry|          16|             1|            0|
+----------+----+----+------------+--------------+-------------+
|2005-10-05| Dry| Wet|       1,096|            37|            3|
+----------+----+----+------------+--------------+-------------+
|2005-10-06| Wet| Dry|           1|             0|            0|
+----------+----+----+------------+--------------+-------------+
|2010-03-17| Dry| Wet|       1,623|            54|            4|
+----------+----+----+------------+--------------+-------------+
|2010-04-19| Wet| Dry|          33|             1|            0|
+----------+----+----+------------+--------------+-------------+
|2010-08-24| Dry| Wet|         127|             4|            0|
+----------+----+----+------------+--------------+-------------+
|2011-09-20| Wet| Dry|         392|            13|            1|
+----------+----+----+------------+--------------+-------------+
|2011-09-21| Dry| Wet|           1|             0|            0|
+----------+----+----+------------+--------------+-------------+
|2013-01-04| Wet| Dry|         471|            16|            1|
+----------+----+----+------------+--------------+-------------+
|2013-01-20| Dry| Wet|          16|             1|            0|
+----------+----+----+------------+--------------+-------------+
|2013-09-10| Wet| Dry|         233|             8|            1|
+----------+----+----+------------+--------------+-------------+
|2013-11-13| Dry| Wet|          64|             2|            0|
+----------+----+----+------------+--------------+-------------+
|2014-02-24| Wet| Dry|         103|             3|            0|
+----------+----+----+------------+--------------+-------------+

Retrieve Dataset
----------------

The retrieve dataset tool retrieves the given dataset(s) optionally applying pixel quality masks.  It can retrieve both
"physical" - NBAR, FC, PQA - and virtual/derived/calculated - NDVI, EVI, NBR, TCI - datasets::

    $ retrieve_dataset.py -h

    usage: retrieve_dataset.py
           [-h]
           [--quiet | --verbose]
           [--acq-min ACQ_MIN] [--acq-max ACQ_MAX]
           [--satellite LS5 LS7 LS8 [LS5 LS7 LS8 ...]]
           [--mask-pqa-apply]
           [--mask-pqa-mask PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR [PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR ...]]
           [--mask-wofs-apply]
           [--mask-wofs-mask DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET [DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET ...]]
           --x [110 - 155] --y [-45 - -10]
           --dataset-type ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI [ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI ...]
           --output-directory OUTPUT_DIRECTORY
           [--overwrite]
           [--list-only]

    Dataset Retrieval

    optional arguments:
      -h, --help            show this help message and exit
      --quiet               Less output
      --verbose             More output
      --acq-min ACQ_MIN     Acquisition Date
      --acq-max ACQ_MAX     Acquisition Date
      --satellite LS5 LS7 LS8 [LS5 LS7 LS8 ...]
                            The satellite(s) to include
      --mask-pqa-apply      Apply PQA mask
      --mask-pqa-mask PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR [PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR ...]
                            The PQA mask to apply
      --mask-wofs-apply     Apply WOFS mask
      --mask-wofs-mask DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET [DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET ...]
                            The WOFS mask to apply
      --x [110 - 155]       X grid reference
      --y [-45 - -10]       Y grid reference
      --dataset-type ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI [ARG25 PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI EVI NBR TCI ...]
                            The type(s) of dataset to retrieve
      --output-directory OUTPUT_DIRECTORY
                            Output directory
      --overwrite           Over write existing output file
      --list-only           List the datasets that would be retrieved rather than retrieving them

.. NOTE::

    The ACQ DATE parameters support dates being specified as ``YYYY`` or ``YYYY-MM`` or ``YYYY-MM-DD``.

    The MIN parameter maps the value down - i.e. `2000` -> `2000-01-01` and `2000-12` -> `2012-12-01`

    The MAX parameter maps the value up - i.e. `2000` -> `2000-12-31` and `2000-01` -> `2012-01-31`

Example Uses
++++++++++++

For example to find out what LS8 datasets we have from December 2013 for the 120/-25 cell::

    $ retrieve_dataset.py --x 120 --y -20 --satellite LS8 --acq-min 2013-12 --acq-max 2013-12 --dataset-type ARG25 --list-only --output-directory /tmp

    2015-04-29 10:13:49,448 INFO
            acq = 2013-12-01 to 2013-12-31
            satellites = LS8
            PQA mask =
            WOFS mask =

    2015-04-29 10:13:49,448 INFO
            x = 120
            y = -020

    2015-04-29 10:13:49,448 INFO
            datasets to retrieve = ARG25
            output directory = /tmp
            over write existing = False
            list only = True

    2015-04-29 10:13:50,187 INFO Would retrieve datasets [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-02T01-57-07.vrt]
    2015-04-29 10:13:50,188 INFO Would retrieve datasets [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_NBAR_120_-020_2013-12-09T02-03-41.tif]
    2015-04-29 10:13:50,188 INFO Would retrieve datasets [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-18T01-56-59.vrt]

To retrieve the NBAR and FC datasets apply PQ mask (saturation and contiguity only)::

    $ retrieve_dataset.py --x 120 --y -20 --satellite LS8 --acq-min 2013-12 --acq-max 2013-12 --dataset-type ARG25 FC25 --mask-pqa-apply --mask-pqa-mask PQ_MASK_SATURATION PQ_MASK_CONTIGUITY --output-directory /tmp

    2015-04-29 11:07:59,063 INFO
            acq = 2013-12-01 to 2013-12-31
            satellites = LS8
            PQA mask = PQ_MASK_SATURATION PQ_MASK_CONTIGUITY
            WOFS mask =

    2015-04-29 11:07:59,063 INFO
            x = 120
            y = -020

    2015-04-29 11:07:59,063 INFO
            datasets to retrieve = ARG25 FC25
            output directory = /tmp
            over write existing = False
            list only = False

    2015-04-29 11:07:59,257 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-02T01-57-07.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-02T01-57-07.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-02T01-57-07.tif]
    2015-04-29 11:08:37,438 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_FC_120_-020_2013-12-02T01-57-07.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-02T01-57-07.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-02T01-57-07.tif]

    2015-04-29 11:08:53,955 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_NBAR_120_-020_2013-12-09T02-03-41.tif] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_PQA_120_-020_2013-12-09T02-03-41.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-09T02-03-41.tif]
    2015-04-29 11:09:15,763 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_FC_120_-020_2013-12-09T02-03-41.tif] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_PQA_120_-020_2013-12-09T02-03-41.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-09T02-03-41.tif]

    2015-04-29 11:09:27,515 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-18T01-56-59.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-18T01-56-59.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-18T01-56-59.tif]
    2015-04-29 11:10:00,857 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_FC_120_-020_2013-12-18T01-56-59.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-18T01-56-59.tif] and pq mask [[<PqaMask.PQ_MASK_SATURATION: 255>, <PqaMask.PQ_MASK_CONTIGUITY: 256>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-18T01-56-59.tif]

.. code-block:: text
    :caption: NBAR and FC outputs

    $ ls -lh

    -rw-r----- 1 sjo547 u46 214M Apr 29 10:28 LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-02T01-57-07.tif
    -rw-r----- 1 sjo547 u46 123M Apr 29 10:28 LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-02T01-57-07.tif

    -rw-r----- 1 sjo547 u46 214M Apr 29 10:28 LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-09T02-03-41.tif
    -rw-r----- 1 sjo547 u46 123M Apr 29 10:29 LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-09T02-03-41.tif

    -rw-r----- 1 sjo547 u46 123M Apr 29 10:30 LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-18T01-56-59.tif
    -rw-r----- 1 sjo547 u46 214M Apr 29 10:29 LS8_OLI_TIRS_NBAR_WITH_PQA_120_-020_2013-12-18T01-56-59.tif

.. code-block:: text
    :caption: `LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-02T01-57-07.tif`...

    $ gdalinfo LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-02T01-57-07.tif

    Driver: GTiff/GeoTIFF
    Files: LS8_OLI_TIRS_FC_WITH_PQA_120_-020_2013-12-02T01-57-07.tif
    Size is 4000, 4000
    ...
    Origin = (120.000000000000000,-19.000000000000000)
    Pixel Size = (0.000250000000000,-0.000250000000000)
    ...
    Metadata:
      ACQUISITION_DATE=2013-12-02 01:57:31
      AREA_OR_POINT=Area
      DATASET_TYPE=FC25
      PIXEL_QUALITY_FILTER=PQ_MASK_SATURATION PQ_MASK_CONTIGUITY
      SATELLITE=LS8
      X_INDEX=120
      Y_INDEX=-020
    ...
    Band 1 Block=4000x1 Type=Int16, ColorInterp=Gray
      Description = PHOTOSYNTHETIC_VEGETATION
      Minimum=0.000, Maximum=16959.000, Mean=6743.956, StdDev=6415.091
      NoData Value=-999
    ...
    Band 2 Block=4000x1 Type=Int16, ColorInterp=Undefined
      Description = NON_PHOTOSYNTHETIC_VEGETATION
      Minimum=0.000, Maximum=21059.000, Mean=10392.389, StdDev=4577.513
      NoData Value=-999
    ...
    Band 3 Block=4000x1 Type=Int16, ColorInterp=Undefined
      Description = BARE_SOIL
      Minimum=0.000, Maximum=16959.000, Mean=5404.868, StdDev=7320.007
      NoData Value=-999
    ...
    Band 4 Block=4000x1 Type=Int16, ColorInterp=Undefined
      Description = UNMIXING_ERROR
      Minimum=326.000, Maximum=16959.000, Mean=7182.122, StdDev=6224.030
      NoData Value=-999

To generate NDVI and NBR datasets from each NBAR dataset after applying PQA::

    $ retrieve_dataset.py --x 120 --y -20 --satellite LS8 --acq-min 2013-12 --acq-max 2013-12 --dataset-type NDVI NBR --mask-pqa-apply --output-directory /tmp

    2015-04-29 11:45:14,860 INFO
            acq = 2013-12-01 to 2013-12-31
            satellites = LS8
            PQA mask = PQ_MASK_CLEAR
            WOFS mask =

    2015-04-29 11:45:14,860 INFO
            x = 120
            y = -020

    2015-04-29 11:45:14,861 INFO
            datasets to retrieve = NDVI NBR
            output directory = /tmp
            over write existing = False
            list only = False

    2015-04-29 11:45:15,474 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-02T01-57-07.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-02T01-57-07.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-02T01-57-07.tif]
    2015-04-29 11:45:23,697 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-02T01-57-07.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-02T01-57-07.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-02T01-57-07.tif]

    2015-04-29 11:45:30,774 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_NBAR_120_-020_2013-12-09T02-03-41.tif] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_PQA_120_-020_2013-12-09T02-03-41.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-09T02-03-41.tif]
    2015-04-29 11:45:34,722 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_NBAR_120_-020_2013-12-09T02-03-41.tif] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/LS8_OLI_TIRS_PQA_120_-020_2013-12-09T02-03-41.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-09T02-03-41.tif]

    2015-04-29 11:45:38,428 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-18T01-56-59.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-18T01-56-59.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-18T01-56-59.tif]
    2015-04-29 11:45:45,491 INFO Retrieving data from [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_NBAR_120_-020_2013-12-18T01-56-59.vrt] with pq [/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS8_OLI_TIRS/120_-020/2013/mosaic_cache/LS8_OLI_TIRS_PQA_120_-020_2013-12-18T01-56-59.tif] and pq mask [[<PqaMask.PQ_MASK_CLEAR: 16383>]] and wofs [] and wofs mask [] to [/g/data/u46/sjo/testing/0.1.0-b20150428/LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-18T01-56-59.tif]

.. code-block:: text
    :caption: NDVI and NBR outputs

    $ ls -lh

    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-02T01-57-07.tif
    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-02T01-57-07.tif

    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-09T02-03-41.tif
    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-09T02-03-41.tif

    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NBR_WITH_PQA_120_-020_2013-12-18T01-56-59.tif
    -rw-r----- 1 sjo547 u46 62M Apr 29 11:45 LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-18T01-56-59.tif

.. code-block:: text
    :caption: `LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-02T01-57-07.tif`...

    $ gdalinfo LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-02T01-57-07.tif

    Driver: GTiff/GeoTIFF
    Files: LS8_OLI_TIRS_NDVI_WITH_PQA_120_-020_2013-12-02T01-57-07.tif
    Size is 4000, 4000
    ...
    Origin = (120.000000000000000,-19.000000000000000)
    Pixel Size = (0.000250000000000,-0.000250000000000)
    ...
    Metadata:
      ACQUISITION_DATE=2013-12-02 01:57:31
      AREA_OR_POINT=Area
      DATASET_TYPE=NDVI
      PIXEL_QUALITY_FILTER=PQ_MASK_CLEAR
      SATELLITE=LS8
      X_INDEX=120
      Y_INDEX=-020
    ...
    Band 1 Block=4000x1 Type=Float32, ColorInterp=Gray
      Description = NDVI
      Minimum=-0.585, Maximum=100000002004089995264.000, Mean=82418751651744006144.000, StdDev=38066057144637997056.000
      NoData Value=nan

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


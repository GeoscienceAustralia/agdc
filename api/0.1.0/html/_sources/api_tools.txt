
.. toctree::
   :maxdepth: 2


API Command Line Tools
======================

There are also a set of “tools” – i.e. packaged executables – aimed at the NPP (Non-Python “People”):

* Retrieve dataset
* Retrieve pixel time series
* Retrieve dataset time series
* Retrieve dataset stack
* Retrieve aoi time series
* Summarise dataset time series
* …

Retrieve Pixel Time Series
--------------------------

The *Retrieve Pixel Time Series* tool extracts the values of a given pixel from the specified dataset over time to a
CSV file.  Pixel quality masking can be applied.  Acquisitions that are completely masked can be output or omitted.::

    $ retrieve_pixel_time_series.py -h
    usage: retrieve_pixel_time_series.py
           [-h] [--quiet | --verbose] [--acq-min ACQ_MIN] [--acq-max ACQ_MAX]
           [--satellite LS5 LS7 LS8 [LS5 LS7 LS8 ...]] [--mask-pqa-apply]
           [--mask-pqa-mask PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR [PQ_MASK_SATURATION_THERMAL PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK PQ_MASK_CLOUD PQ_MASK_CLEAR ...]]
           [--mask-wofs-apply]
           [--mask-wofs-mask DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET [DRY NO_DATA SATURATION_CONTIGUITY SEA_WATER TERRAIN_SHADOW HIGH_SLOPE CLOUD_SHADOW CLOUD WET ...]]
           --lat LATITUDE --lon LONGITUDE [--hide-no-data] --dataset-type ARG25
           PQ25 FC25 WATER DSM DEM DEM_HYDROLOGICALLY_ENFORCED DEM_SMOOTHED NDVI
           EVI NBR TCI [--bands-all | --bands-common] [--delimiter DELIMITER]
           [--output-directory OUTPUT_DIRECTORY] [--overwrite]

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

    The ACQ DATE parameters support dates being specified as YYYY or YYYY-MM or YYYY-MM-DD.

    The MIN parameter maps the value down - i.e. 2000 -> 2000-01-01 and 2000-12 -> 2012-12-01

    The MAX parameter maps the value up - i.e. 2000 -> 2000-12-31 and 2000-01 -> 2012-01-31

Example Uses
++++++++++++

For example to retrieve the NBAR values...::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-01 01:58:47,-999,-999,-999,-999,-999,-999
    LS7,2013-12-10 01:53:02,-999,-999,-999,-999,-999,-999
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS7,2013-12-26 01:53:05,-999,-999,-999,-999,-999,-999

and again applying pixel quality masking (defaults to selecting CLEAR pixels but can be changed via the --pqa-mask argument)...::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-01 01:58:47,-999,-999,-999,-999,-999,-999
    LS7,2013-12-10 01:53:02,-999,-999,-999,-999,-999,-999
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS7,2013-12-26 01:53:05,-999,-999,-999,-999,-999,-999

and again omitting no data values...::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626

and where a dataset has different bands for different satellites...::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data --satellite LS7 LS8

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2,COASTAL_AEROSOL
    LS8,2013-12-02 01:57:55,507,871,1630,2633,3236,2513,493
    LS8,2013-12-09 02:04:05,448,820,1592,2596,3260,2550,430
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626,
    LS8,2013-12-18 01:57:47,496,847,1605,2638,3252,2509,485

which outputs an **empty** value where the band is not applicable; or to only include the bands common across all satellites...::

    $ retrieve_pixel_time_series.py --lon 120.25 --lat -20.25 --acq-min 2013-12 --acq-max 2013-12 --satellite LS7 --dataset-type ARG25 --quiet --mask-pqa-apply --hide-no-data --satellite LS7 LS8 --bands-common

    SATELLITE,ACQUISITION DATE,BLUE,GREEN,RED,NEAR_INFRARED,SHORT_WAVE_INFRARED_1,SHORT_WAVE_INFRARED_2
    LS8,2013-12-02 01:57:55,507,871,1630,2633,3236,2513
    LS8,2013-12-09 02:04:05,448,820,1592,2596,3260,2550
    LS7,2013-12-17 01:58:47,388,824,1605,2632,3326,2626
    LS8,2013-12-18 01:57:47,496,847,1605,2638,3252,2509


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


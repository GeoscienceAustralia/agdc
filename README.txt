JR-STACK - Directory containing Roger Edberg's job runner framework for the MDBA work (deprecated)
README.txt - This file
datacube.conf - Default configuration file for datacube
datacube.py - Implementation of base class Datacube with DB connectivity and helper functions
dbupdater.py - Script to catalogue datasets. Implements DBUpdater Datacube descendent class
dbupdater.sh
dem_tiler.py - Script to resample and tile DEM (or DSM) file with associated slope & aspect layers
dem_tiler.sh
edit_envi_hdr.py - Sub-module to re-write ENVI file header
fc_stacker.py - Stacker subclass implementation to create PQ-masked temporal stack of FC data
fc_stacker.sh
fc_tiler.conf
fc_tiler.py - Script to reproject and tile FC datasets and create tile files and records in Datacube DB
fc_tiler.sh
index_stacker.py - Stacker subclass implementation to create PQ-masked temporal stack of multiple indices and bands
index_stacker.sh
integrity_checker.py - Script to check whether tile files are valid GDAL datasets
integrity_checker.sh
landsat_tiler.py - Script to  reproject and tile ORTHO, NBAR & PQ datasets and create tile files and records in Datacube DB
landsat_tiler.sh
lpgs_submit.sh - Bash script to generate and submit multiple PBS jobs each cataloguing a single month of L1T files. Calls dbupdater.py
nbar_submit.sh - Bash script to generate and submit multiple PBS jobs each cataloguing a single month of NBAR files. Calls dbupdater.py
ndvi_stacker.py - Stacker subclass implementation to create PQ-masked temporal stack of NDVI tiles
ndvi_stacker.sh
pqa_stacker.py
pqa_stacker.sh
pqa_submit.sh - Bash script to generate and submit multiple PBS jobs each cataloguing a single month of PQ files. Calls dbupdater.py
rgb_stacker.py - Stacker subclass implementation to create PQ-masked RGB (Bands 5-4-2) files. NB: Cannot temporally stack multi-band files
satcat_schema_backup.sql
scene_kml_generator.py - Script to generate KML file of nominal scene footprints for each NBAR scene in DB
scene_kml_generator.sh
season_stacker.py - Stacker subclass implementation to create PQ-masked temporal stack of multiple indices and bands (similar to index_stacker.py) aggregated across multiple years
season_stacker.sh
stacker.py - Stacker class implementation. Should be subclassed for custom algorithms.
stacker.sh
stats - Directory containing Josh Sixsmith's statistics code
thredds_checker.py - Script to list all NBAR datasets in DB which are not loaded into THREDDS
update_dataset_record.py - Sub-module to catalogue NBAR dataset
update_fc_dataset_record.py - Sub-module to catalogue un-packaged FC dataset
update_pqa_dataset_record.py - Sub-module to catalogue un-packaged PQ dataset
vidconvert.sh - Bash script to generate timeslice images for video. Works on outputs from rgb_stacker.py
vidgen.sh - Bash script to run rgb_stacker.py on several tiles
vidstack.sh - Bash script to generate cumulative stack of images for video. Works on outputs from rgb_stacker.py
vrt2bin.py - Sub-module to convert VRT file to binary (ENVI) file
water_rgb.py - Stacker subclass implementation to create PQ-masked RGB files showing water extents in blue. NB: Cannot temporally stack multi-band files
water_rgb.sh

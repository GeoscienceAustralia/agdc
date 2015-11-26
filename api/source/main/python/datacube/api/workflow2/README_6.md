# Requirement 

To generate six bands (Red, Blue, Green, NEAR_INFRARED, SHORT_WAVE_INFRARED_1, SHORT_WAVE_INFRARED_2)of Landsat 8 data from 2013 to 2015. The season covers from APR to OCT. The statistics used to calculate interpolated data are Percentile 10, 50 and 90 for each bands. 

# Solution

I took code from AGDC version 1 from GitHub repository . The source is from  sjo/dev-pm-pic branch.I modified clean_pixel_statistics.py and named  clean_pixel_statistics_6.py program to allow three more bands and more statistical calculations other than default 50 percentile. I also changed datacube/api/utils.py to include APR_TO_OCT season. The previous PBS script of running on 201 nodes was used to run but I made a little change to run for each percentile upto three bands in one job.  So, six jobs are used to complete the whole process. If I try to run the job in one go, I found this common error "Exit Status: -14 (MotherSuperior->SisterMoms communication failure)" and seems to be all the nodes are not able to handle with the scheduler. So it is desirable to run in 30-40 minutes chunk for each job. There are 1001 datasets found after running query from the production database. The task created 1001 tif files for each band. In total 18,018 tif files are generated. At the end, I ran gdalbuildvrt and gdal_translate to have a composite mosaic tif file for each band against each percentile. There are 18 tif files, six for each statistics with 50 GB approx. Norman is the key stakeholder in this project. He has received all the files completed on Tuesday ( 11/10/2015).  

# Operating Instruction

First prepare_csv.py needs to run to get required datasets to feed into clean_pixel_statistics_6.pbs.sh.

#Step 1

   $ mkdir -p input
   
   $ source /projects/u46/venvs/agdc/bin/activate
   
   $ python prepare_csv.py --x-min 110 --x-max 155 --y-min -45 --y-max -10 --acq-min 2013 --acq-max 2015 --satellite LS8 --season APR_TO_OCT --output-directory $PWD/input

Once it is completed then the following steps can be run. This step is to split lat/long into individual cell files and running statistics task.

#Step 2
$ run_split_cells.sh

$ clean_pixel_statistics_6.pbs.sh   (Please make sure it has execute permission otherwise do chmod +x                                                                          clean_pixel_statistics_6.pbs.sh)
Once this job is finished, pbs job build_vrt.pbs.sh needs to be submitted which will execute build_vrt.sh script to get final product of 18 tif files.

#Step 3

$ build_vrt.pbs.sh

# Comments

Pushed codes to the sjo/dev-pm-pic branch in agdc repository.
  1.	Modified codes in datacube/api/__init__.py and utils.py
  2.	Added clean_pixel_statistics_6.py and clean_pixel_statistics_6.pbs.sh to datacube/api/workflow2



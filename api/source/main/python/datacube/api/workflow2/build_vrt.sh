#!/bin/bash

module load openev2/2.1.0
module load gdal/1.9.2 

cnt=0
# cd /g/data/u46/bb/output/clean_pixel2
for i in 10 50 90
do
	cnt=$(($cnt+1))
	for y in BLUE RED GREEN NEAR SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2
	do
		echo creating txt and vrt files for $y 
		if [ "$cnt" -eq 1 ]
		then
			ls | grep "PERCENTILE_10_$y"|grep -v aux > mm_10_$y.txt	
			gdalbuildvrt -input_file_list mm_10_$y.txt mm_10_$y.vrt	
        		gdal_translate -ot Int16 -of GTiff mm_10_$y.vrt  mm_10_$y_compo.tif"
		fi
		if [ "$cnt" -eq 2 ]
		then
			ls | grep "PERCENTILE_50_$y"|grep -v aux > mm_50_$y.txt	
			gdalbuildvrt -input_file_list mm_50_$y.txt mm_50_$y.vrt	
        		gdal_translate -ot Int16 -of GTiff mm_50_$y.vrt  mm_50_$y_compo.tif"
		fi
		if [ "$cnt" -eq 3 ]
		then
			ls | grep "PERCENTILE_90_$y"|grep -v aux > mm_90_$y.txt	
			gdalbuildvrt -input_file_list mm_90_$y.txt mm_90_$y.vrt	
        		gdal_translate -ot Int16 -of GTiff mm_90_$y.vrt  mm_90_$y_compo.tif"
		fi
	done
done

# output results
[fxz547@raijin4 clean_pixel2]$ ls -l mm*compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:42 mm_10_blue_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:43 mm_10_green_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:44 mm_10_near_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:43 mm_10_red_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:49 mm_50_blue_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:50 mm_50_green_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:52 mm_50_near_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:51 mm_50_red_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:52 mm_90_blue_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:53 mm_90_green_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:55 mm_90_near_compo.tif
-rw-r-----+ 1 bxb547 u46 49282240524 Nov  8 08:55 mm_90_red_compo.tif

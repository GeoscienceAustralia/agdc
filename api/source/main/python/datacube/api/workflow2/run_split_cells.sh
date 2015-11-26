#!/bin/bash
#This will split datasets lon/lat into individual cells. The whole continent has land 1001 tiles
# and we are splitting into 20 cells with 50 lat/long data
# Then ultimately 20 pbs jobs will run in 20 nodes for these cells
mkdir -p input_cells
rm input_cells/*
ls input/datasets*|awk -F"_" '{print $2","$3}' > input_cells/cells.txt
fl=1;
cnt=0;
for i in $(cat input_cells/cells.txt)
 do
	 let cnt++; 
	 if [ $cnt -lt 51 ]; then
		if [ $cnt -eq 1 ] ; then
			cc=$i
		else
			cc=$cc" "$i
		fi
	 else 
	 	echo "--cell " $cc >> input_cells/cell.$fl.txt
		cnt=0;
		let  fl++; 
	 fi;
 done
 echo "--cell " $cc >> input_cells/cell.$fl.txt


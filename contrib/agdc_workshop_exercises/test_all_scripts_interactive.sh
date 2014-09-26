#!/bin/bash
# Script to test all finished code examples in agdc_workshop

OUTPUTDIR=/home/user/agdc_workshop/test_output_`date +%y%m%d%H%M`
mkdir -p ${OUTPUTDIR}

for script in `ls *.py`; do bulk_submit_interactive.sh default_interactive_header.txt $script canberra_tiles.txt ${OUTPUTDIR} -s 20100101 -e 20101231i --debug; done


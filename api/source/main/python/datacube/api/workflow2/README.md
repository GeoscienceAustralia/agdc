This is a temporary module created while working on the **Landsat 8** image to be spiffied up and given to the PM.

This was a migration to using `pbsdsh` to run big parallel workflows - e.g. 200 nodes (aka 3200 CPUs).

The intention is to merge this back into the existing workflows.

I was a little worried that having 3200 parallel tasks hitting the database at the same time, or close enough to the same time,
was going to cause a _9am_ effect so I actually pre-queried the datasets for each cell up front.

So the end-to-end workflow process is:

1.  Query the database for matching datasets for each cell
2.  Package up the list of cells for which we have datasets in a form suitable for submitting to the processing workflow
3.  Submit the processing job

# Query the database

In this instance we are calculating a seasonal median (50<sup>th</sup> percentile) for each Landsat 8 pixel.  Initially we were using
_traditional_ season definitions - for e.g. WINTER being defined as 1<sup>st</sup> July to 31<sup>st</sup> August.

To produce a CSV file for each cell containing the datasets::

    $ mkdir -p input
    $ python prepare_csv.py --x-min 110 --x-max 155 --y-min -45 --y-max -10 --acq-min 2013 --acq-max 2015 --satellite LS8 --season WINTER --output-directory $PWD/input

# Package up the list of cells

The processing is going to be spread across a number of nodes - e.g. 200.  In this instance we were producing an output
for each of the **RED**, **GREEN** and **BLUE** bands of the input dataset for each cell.  So 3 outputs per cell which
can be processed independently.  We have 16 CPUs on each node so in this instance I went for 3 cells per node (which
gives 15 processes per node - aka 16 CPUs).

To get a list of the cells I did:

    $ mkdir -p input_cells
    $ ls -1 input |cut -d'_' -f2,3 --output-delimiter=, |sort -u >input_cells/cells.txt

To produce a file for each containing 5 cells I did:

    $ cd input_cells
    $ split -l 5 cells.txt
    $ i=1 ; for f in x*; do   cat $f |xargs echo --cell >cell.$i.txt ; i=$((i+1)) ; done

# Submit the processing job

I then submitted a single PBS job asking for 3200 CPUs and 6400GB RAM (aka 200 nodes).

    #!/usr/bin/env bash
    
    #PBS -N median
    #PBS -P u46
    #PBS -q normal
    #PBS -l walltime=01:00:00,ncpus=3200,mem=6400GB
    #PBS -l wd

and then it did:

    DIR=...
    
    node_count=200
    
    for node in $(seq 1 $node_count)
    do
        cells=$(cat $DIR/input_cells/cell.$node.txt)
        echo node=$node cell=$cells
        pbsdsh -n $((node*16)) -- bash -l -c "cd $DIR ; \\
            export GDAL_CACHEMAX=1073741824 ; \\
            export GDAL_SWATH_SIZE=1073741824 ; \\
            export GDAL_DISABLE_READDIR_ON_OPEN=TRUE ; \\
            source $HOME/bin/setup-agdc-api-arg25-index-statistics ; \\
            python $HOME/source/agdc/agdc-api-arg25-index-statistics/api/source/main/python/datacube/api/workflow2/clean_pixel_statistics.py --acq-min 2013 --acq-max 2015 --season WINTER --satellite LS8 --mask-pqa-apply $cells --dataset-type ARG25 --output-directory $DIR --output-format ENVI" &
    done

and to submit it I then did:

    $ qsub pbs.sh


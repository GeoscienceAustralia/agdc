#!/bin/bash

if [ $# -lt 4 ]
then
  echo "Usage: $0 <pbs_header> <python_script> <tile_list> <output_dir> <additional_arguments>"
  echo "e.g: $0 my_header my_stacker_script.py canberra_tiles.txt ./test -s 20050101 -e 20051231"
  echo "N.B: <pbs_header> contains all of the comments required to set up a PBS job"
  echo "     <tile_list> contains a list of one or more <tile_x_index> <tile_y_index> pairs for the tile(s) to be processed"
  exit 1
fi

pbs_header=`readlink -f $1`
python_script=`readlink -f $2`
tile_list=`readlink -f $3`
output_dir=`readlink -f $4`

# Comment out the following line for interactive use
#PBS=1

shift 4
additional_arguments=$@

# Debug output
echo pbs_header=$pbs_header
echo python_script=$python_script
echo tile_list=$tile_list
echo output_dir=$output_dir
echo additional arguments: $additional_arguments

script_basename=$(basename ${python_script%.*})
mkdir -p ${output_dir}/jobs

# Iterate through tile index pairs in tile_list
while read line
do
    x_index=`echo $line | cut -d' ' -f1`
    y_index=`echo $line | cut -d' ' -f2`

    echo Running ${python_script} on tile $x_index $y_index

    tile_output_path=${output_dir}/${x_index}_${y_index}
    tile_script_path=${output_dir}/jobs/${script_basename}_${x_index}_${y_index}.sh
    tile_stdout_log=${output_dir}/jobs/${script_basename}_${x_index}_${y_index}.log
    tile_stderr_log=${output_dir}/jobs/${script_basename}_${x_index}_${y_index}.err

    # Copy pbs_header file to script
    cp -f ${pbs_header} ${tile_script_path}

    # Append new python line
    echo "mkdir -p ${tile_output_path}
python ${python_script} -x ${x_index} -y ${y_index} -o ${tile_output_path} ${additional_arguments}" \
>> ${tile_script_path}

    if [ "$PBS" != "" ]
    then
        # The following lines are for submitting jobs on the NCI PBS system
        echo Submitting ${tile_script_path}
        #qsub -o ${tile_stdout_log} -e ${tile_stderr_log} ${tile_script_path}
        pushd ${output_dir}/jobs
        qsub ${tile_script_path} # Write output to default PBS log fliles
    else
        # The following lines are for running background jobs on the training VM
        echo Background executing ${tile_script_path}
        chmod 755 ${tile_script_path}
        ${tile_script_path} >${tile_stdout_log} 2>${tile_stderr_log} &
    fi

done < $tile_list

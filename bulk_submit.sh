#!/bin/bash

if [ $# -lt 3 ]
then
  echo "Usage: $0 <python_script> <tile_list> <output_dir> <additional_arguments>"
  echo "e.g: $0 my_stacker_script.py canberra_tiles.txt ./test -s 20050101 -e 20051231"
  echo "N.B: <tile_list> contains a list of one or more <tile_x_index> <tile_y_index> pairs for the tile(s) to be processed"
  exit 1
fi

# The following line must be changed to reflect the path of the datacube directory
# Datacube code path for raijin at NCI
#DATACUBE_ROOT=/projects/u46/opt/modules/datacube/0.1.0
# Datacube code path for datacube training VM
DATACUBE_ROOT=/home/user/ga/datacube

python_script=`readlink -f $1`
tile_list=`readlink -f $2`
output_dir=`readlink -f $3`

shift 3
additional_arguments=$@

# Debug output
echo DATACUBE_ROOT=$DATACUBE_ROOT
echo python_script=$python_script
echo tile_list=$tile_list
echo output_dir=$output_dir
echo additional arguments: $additional_arguments

mkdir -p $output_dir

# Escape "/" where args are used in substitutions
_python_script=`echo $python_script | sed s/\\\\//\\\\\\\\\\\\//g`
_output_dir=`echo $output_dir | sed s/\\\\//\\\\\\\\\\\\//g`
_datacube_root=`readlink $DATACUBE_ROOT -f | sed s/\\\\//\\\\\\\\\\\\//g`

# Iterate through tile index pairs in tile_list
while read line
do
    x_index=`echo $line | cut -d' ' -f1`
    y_index=`echo $line | cut -d' ' -f2`
    
    echo Processing tile $x_index $y_index

    tile_script_name=`echo $_python_script | sed s/.*\\\\///g | sed s/\.py//g`_${x_index}_${y_index}
    script_path=`echo $tile_script_name | sed s/^/${_output_dir}\\\\//g`.sh

    # Copy default stacker.sh script with old python invocation and DATACUBE_ROOT definition removed
    cat ${DATACUBE_ROOT}/stacker.sh | sed s/^python.*//g \
| sed s/^export//g \
> ${script_path}

    # Append new DATACUBE_ROOT definition and python line
    echo "mkdir -p ${output_dir}/${tile_script_name}
python ${python_script} -x ${x_index} -y ${y_index} -o ${output_dir}/${tile_script_name} ${additional_arguments}" \
>> ${script_path} 

    # The following lines are for submitting jobs on the NCI PBS system
    # echo Submitting ${script_path}
    # qsub $script_path

    # The following lines are for running background jobs on the training VM
    echo Background executing ${script_path}
    chmod 755 ${script_path}
    ${script_path} >${script_path}.log 2>${script_path}.err &

done < $tile_list


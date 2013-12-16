#!/bin/bash

for lon in 148 149
do
  for lat in -35 -36
  do
    ./stacker.sh -x $lon -y $lat -o test
  done
done


#!/bin/bash

if [ $# != 1 ]; then
  h=$1
  shift
fi

if [ $# == 1 ]; then
  path="rads/profile"
else
  path=$1
  shift
fi

file=$1

curl -XPOST $h/$path -d @$file

#!/bin/bash

name=data_dep1
jobs=1

build_one()
{
    local tg=$1
    local n=$2

    cd $SCHEDULERS_DIR
    make clean ; OPT="$OPT -O4 -DMODE=1 -DOBJECT_TASKGRAPH=$n " make -j${jobs}
    cd -
    make clean ; OPT="$OPT -O4 -DMODE=1 -DOBJECT_TASKGRAPH=$n " make ${name}
    mv ${name} ${name}_${tg}
}

build_one tkt 1
build_one cs 9
build_one cg 10
build_one ecg 11
build_one vtkt 8

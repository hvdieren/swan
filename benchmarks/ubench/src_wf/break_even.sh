#!/bin/bash

max_threads=$1
arg_ty=$2
num_tasks=$3
num_objects=$4
exec_file=$5

var=NUM_THREADS
cores_per_mem=6

run()
{
    local num_threads=$1
    local arg_ty=$2
    local num_tasks=$3
    local fib=$4

    #echo "$var=$num_threads numactl --physcpubind 0-$[$num_threads-1] --membind 0-$[($num_threads-1)/$cores_per_mem] $exec_file $arg_ty $num_tasks 1 $fib" 
    result=`eval "$var=$num_threads numactl --physcpubind 0-$[$num_threads-1] --membind 0-$[($num_threads-1)/$cores_per_mem] $exec_file $arg_ty $num_tasks $num_objects 0:0 $fib" 2>&1  | grep 'Per-task time' | tail -n 1 | cut -d' ' -f3`
    #result=`eval "$var=$num_threads $exec_file $arg_ty $num_tasks $fib 0" 2>&1 | grep 'Total time' | tail -n 1 | cut -d' ' -f3`

    echo "$exec_file $num_threads $arg_ty $num_tasks $fib $result"
}

sweep()
{
    local arg_ty=$1
    local num_threads=$2
    local corr=$3
    local fib

#    for fib in 0 100 200 300 500 700 900 1100 1300 1500 1700 1900 2100 5000 10000 15000 20000 40000 80000 160000 320000 640000 2560000 ; do
#	run $arg_ty $fib $num_threads $num_tasks
#    done

    run $num_threads $arg_ty $[$corr/1000] 0 
    run $num_threads $arg_ty $[$corr/1000] 1

    for fib in 10 100 200 300 ; do
	run $num_threads $arg_ty $[$corr/$fib/100] $fib
    done

    for fib in 500 700 900 1100 1300 1500 1700 1900 2100 ; do
	run $num_threads $arg_ty $[$corr/$fib/10] $fib
    done

    for fib in 5000 10000 15000 20000 40000 80000 ; do
	run $num_threads $arg_ty $[10*$corr/$fib] $fib
    done

    for fib in 160000 320000 640000 1280000 2560000 ; do
	run $num_threads $arg_ty $[100*$corr/$fib] $fib
    done
}

sweep $arg_ty $max_threads 1000000000
#sweep ref     1            1000000000

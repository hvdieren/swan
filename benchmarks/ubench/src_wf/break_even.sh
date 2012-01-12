#!/bin/bash

max_threads=$1
arg_ty=$2
num_objects=$3
exec_file=$4

var=NUM_THREADS
cores_per_mem=6

run()
{
    local num_threads=$1
    local arg_ty=$2
    local num_tasks=$3
    local fib=$4

    #echo "$var=$num_threads numactl --physcpubind 0-$[$num_threads-1] --membind 0-$[($num_threads-1)/$cores_per_mem] $exec_file $arg_ty $num_tasks 1 $fib" 
    sh -c "$var=$num_threads numactl --physcpubind 0-$[$num_threads-1] --membind 0-$[($num_threads-1)/$cores_per_mem] time $exec_file $arg_ty $num_tasks $num_objects 0:0 $fib" > /tmp/o.$$ 2>&1  
    #result=`eval "$var=$num_threads $exec_file $arg_ty $num_tasks $fib 0" 2>&1 | grep 'Total time' | tail -n 1 | cut -d' ' -f3`
    a=$(egrep Per < /tmp/o.$$ | cut -d' ' -f3)
    b=$(grep system < /tmp/o.$$ | cut -d' ' -f3 | cut -de -f1)
    rm /tmp/o.$$
    echo "$exec_file $num_threads $arg_ty $num_tasks $fib $result $a $b"
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
    run $num_threads $arg_ty $[$corr/1000] 10

    for fib in 100 200 300 ; do
	run $num_threads $arg_ty $[$corr/$fib/10] $fib
    done

    for fib in 500 700 900 1300 ; do
	run $num_threads $arg_ty $[$corr/$fib/10] $fib
    done

    for fib in 1700 2200 2700 ; do
	run $num_threads $arg_ty $[$corr/$fib/5] $fib
    done

    run $num_threads $arg_ty $[$corr/3200/2] 3200
    run $num_threads $arg_ty $[$corr/3700/2] 3700
    run $num_threads $arg_ty $[$corr/4200] 4200
    run $num_threads $arg_ty $[$corr/5000] 5000

    for fib in 10000 20000 40000 80000 ; do
	run $num_threads $arg_ty $[10*$corr/$fib] $fib
    done

    for fib in 120000 160000 200000 240000 320000 ; do
	run $num_threads $arg_ty $[10*$corr/$fib] $fib
    done
}

rsweep()
{
    local arg_ty=$1
    local num_threads=$2
    local corr=$3
    local fib

    run $num_threads $arg_ty $[$corr/10] 0 
    #run $num_threads $arg_ty $[$corr/1000] 1
    #run $num_threads $arg_ty $[$corr/1000] 10

    for fib in 1 10 100 200 300 500 700 900 1300 1700 2200 2700 3200 3700 4200 5000 10000 20000 40000 80000 120000 160000 200000 240000 320000 ; do
	run $num_threads $arg_ty $[$corr/$fib/5] $fib
    done
}

sweep $arg_ty $max_threads 4000000000
rsweep ref     1            4000000000

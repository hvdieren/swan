#!/bin/bash

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    if [ $nproc -le 4 ] ; then
        echo -n $var=$nproc $prog $flags $input CPU0-$[$nproc-1] mem0" "
        eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0 ./$prog $flags $input > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
        if ! grep Running junk ; then echo ; fi
        rm junk
    else
        echo -n $var=$nproc $prog $flags $input CPU0-$[$nproc-1] mem0" "
        eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0,1 ./$prog $flags $input > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
        if ! grep Running junk ; then echo ; fi
        rm junk
    fi
    #echo
    #popd > /dev/null
}

input=
repeat=10
#repeat=1

prog=matmul_novec_goto_wfo
var=NUM_THREADS
for flags in 64 ; do
    for nproc in 1 2 3 4 5 6 7 8 ; do
        for run in `seq $repeat` ; do
	    run $prog $flags "$input" $var $nproc
	done
    done
done

prog=matmul_novec_goto_smpss
var=CSS_NUM_CPUS
for flags in 64 ; do
    for nproc in 1 2 3 4 5 6 7 8 ; do
        for run in `seq $repeat` ; do
	    run $prog $flags "$input" $var $nproc
	done
    done
done

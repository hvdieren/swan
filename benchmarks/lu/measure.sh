#!/bin/bash

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    #pushd $input > /dev/null 
    #args=$(cat CMD | cut -d' ' -f2-)

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

#repeat=1
#input=i06.hmmer.test.bombesin
repeat=10
flags=

input="-benchmark long"

algo=0

prog=lu_wf_a${algo}
var=NUM_THREADS
for bsize in 16 32 64 ; do
    for nproc in `seq 8` ; do
	for run in `seq $repeat` ; do
	    run $prog_b${bsize} "$flags" "$input" $var $nproc
	done
    done
done

prog=lu_wfo_a${algo}
var=NUM_THREADS
for bsize in 16 32 64 ; do
    for nproc in `seq 8` ; do
	for run in `seq $repeat` ; do
	    run $prog_b${bsize} "$flags" "$input" $var $nproc
	done
    done
done

prog=lu_cilk++_a${algo}
var=CILK_NPROC
for bsize in 16 32 64 ; do
    for nproc in `seq 8` ; do
	for run in `seq $repeat` ; do
	    run $prog_b${bsize} "$flags" "$input" $var $nproc
	done
    done
done

prog=lu_smpss_a${algo}
var=CSS_NUM_CPUS
for bsize in 16 32 64 ; do
    for nproc in `seq 8` ; do
	for run in `seq $repeat` ; do
	    run $prog_b${bsize} "$flags" "$input" $var $nproc
	done
    done
done

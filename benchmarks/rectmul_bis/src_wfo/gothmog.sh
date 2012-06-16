#!/bin/bash

THREADS=48
NUMA=$[$THREADS/8]

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    echo -n $var=$nproc $prog $flags CPU0-$[$nproc-1] mem0-$[($nproc-1)/$NUMA]" "
    eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0-$[($nproc-1)/$NUMA] ./$prog $flags > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
    if ! grep Running junk ; then echo ; fi
    rm junk
}

input=
repeat=10
#repeat=1

#prog=J_jacobi_c
#var=NONE
#for nproc in 1 ; do
#    for run in `seq $repeat` ; do
#	for flags in 64 ; do
#	    run $prog "$flags" "$input" "$var" $nproc
#	done
#    done
#done

flags="-x 4096 -y 4096 -z 4096"

var=NUM_THREADS
for lo in jik ; do # kji
    for bs in 32 64 16 ; do
	for tg in tkt cs cg ecg gtkt ; do
	    for nproc in 1 `seq 8 8 $THREADS` ; do
		for run in `seq $repeat` ; do
		    prog=rectmul_${lo}_${tg}_a1_b${bs}
		    run $prog "$flags" "$input" $var $nproc
		done
	    done
	done
    done
done

#for lo in jik ; do
#    for bs in 32 ; do
#	for tg in tkt cs cg ecg gtkt ; do
#	    for nproc in $THREADS ; do
#		for run in `seq $repeat` ; do
#		    prog=rectmul_${lo}_${tg}_a1_b${bs}
#		    run $prog "$flags" "$input" $var $nproc
#		done
#	    done
#	done
#    done
#done

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
input="-n 4096"
flags=

prog=strassen_wf792
var=NUM_THREADS
for nproc in 1 2 4 6 8 ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" $var $nproc
    done
done

prog=strassen_cilk++
var=CILK_NPROC
for nproc in 1 2 4 6 8 ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" $var $nproc
    done
done

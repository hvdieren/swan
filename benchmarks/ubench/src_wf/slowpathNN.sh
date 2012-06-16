#!/bin/bash

dep=$1

run()
{
    local dep=$1
    local tasks=$2
    local batch1=$3
    local batch2=$4
    local exec=$5

    echo -ne "1 $exec $dep $tasks 1 $batch1:$batch2 0 "
    sh -c "ulimit -t $[60*2] ;
           NUM_THREADS=1 numactl --physcpubind 0 --membind 0 time $exec $dep $tasks 1 $batch1:$batch2 0 > /tmp/o.$$ 2>&1"
    a=$(egrep Per < /tmp/o.$$ | cut -d' ' -f3)
    b=$(grep system < /tmp/o.$$ | cut -d' ' -f3 | cut -de -f1)
    echo "$a $b"
    rm /tmp/o.$$
    #NUM_THREADS=1 $exec $dep $tasks 1 $batch1:$batch2 0 | grep Per | cut -d' ' -f3
}

sizes="1 2 5 10 50 100 500 1000 5000 10000 50000 100000"

if [ $dep = "cinoutdep" -o $dep = "scalred" -o $dep = "objred" ] ; then
    lsizesa="1 2 5 10 50 100 500 1000 5000"
    lsizesb="10000 50000 100000"
else
    lsizesa=$sizes
    lsizesb=""
fi

# 1-1k
echo 1-1k
echo tkt
run $dep 50000000 0 0 ./data_dep1_tkt
for n in $sizes ; do
    run $dep 5000000 0 $n ./data_dep1_tkt
done

echo cs
run $dep 50000000 0 0 ./data_dep1_cs
for n in $sizes ; do
    run $dep 5000000 0 $n ./data_dep1_cs
done

echo cg
run $dep 50000000 0 0 ./data_dep1_cg
for n in $sizes ; do
    run $dep 5000000 0 $n ./data_dep1_cg
done

echo ecg
run $dep 50000000 0 0 ./data_dep1_ecg
for n in $sizes ; do
    run $dep 5000000 0 $n ./data_dep1_ecg
done

echo gtkt
run $dep 50000000 0 0 ./data_dep1_gtkt
for n in $sizes ; do
    run $dep 5000000 0 $n ./data_dep1_gtkt
done

#echo otkt
#run $dep 50000000 0 0 ./data_dep1_otkt
#for n in $sizes ; do
#    run $dep 5000000 0 $n ./data_dep1_otkt
#done

# 1-1k-1k
echo 1-1k-1k
echo tkt
run $dep 50000000 0 0 ./data_dep1_tkt
for n in $sizes ; do
    run $dep 5000000 $n $n ./data_dep1_tkt
done

echo cs
run $dep 50000000 0 0 ./data_dep1_cs
for n in $lsizesa ; do
    run $dep 5000000 $n $n ./data_dep1_cs
done
for n in $lsizesb ; do
    run $dep 500000 $n $n ./data_dep1_cs
done

echo cg
run $dep 50000000 0 0 ./data_dep1_cg
for n in $lsizesa ; do
    run $dep 5000000 $n $n ./data_dep1_cg
done
for n in $lsizesb ; do
    run $dep 500000 $n $n ./data_dep1_cg
done

echo ecg
run $dep 50000000 0 0 ./data_dep1_ecg
for n in $lsizesa ; do
    run $dep 5000000 $n $n ./data_dep1_ecg
done
for n in $lsizesb ; do
    run $dep 500000 $n $n ./data_dep1_ecg
done

echo gtkt
run $dep 50000000 0 0 ./data_dep1_gtkt
for n in $sizes ; do
    run $dep 5000000 $n $n ./data_dep1_gtkt
done

#echo otkt
#run $dep 50000000 0 0 ./data_dep1_otkt
#for n in $sizes ; do
#    run $dep 5000000 $n $n ./data_dep1_otkt
#done


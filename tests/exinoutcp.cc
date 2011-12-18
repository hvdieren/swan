// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

void task0( outdep<int> out ) {
    printf( "Task 0 start %p\n", out.get_version() );
    sleep(2); // secs
    out = 3;
    printf( "Task 0 done %p\n", out.get_version() );
}

void task1( indep<int> in ) {
    printf( "Task 1 start %p\n", in.get_version() );
    sleep(1);
    printf( "Task 1 done %p\n", in.get_version() );
}

void task2( inoutdep<int> inout ) {
    inout++;
    printf( "Task 2 increments to %d %p\n", (int)inout, inout.get_version() );
}

void task3( indep<int> in ) {
    printf( "Task 3 reads %d from %p\n", (int)in, in.get_version() );
}

int my_main( int argc, char * argv[] ) {
    object_t<int> v;
    spawn( task0, (outdep<int>)v );
    spawn( task1, (indep<int>)v );
    spawn( task2, (inoutdep<int>)v );
    spawn( task3, (indep<int>)v );

    ssync();

    return 0;
}

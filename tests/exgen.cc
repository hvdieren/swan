// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include <unistd.h>

#include "wf_interface.h"

using namespace obj;

void task_in( indep<int> in, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_in n=" << int(n) << '\n';
}

void task_out( outdep<int> out, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_out n=" << int(n) << '\n';
}

void task_inout( inoutdep<int> inout, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_inout n=" << int(n) << '\n';
}

#if OBJECT_COMMUTATIVITY
void task_cinout( cinoutdep<int> inout, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_cinout n=" << int(n) << '\n';
}
#endif

void test0( int n ) {
    object_t<int> obj;

    spawn( task_inout, (inoutdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

void test1( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_out, (outdep<int>)obj, 0 );

    ssync();
}

void test2( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

void test3( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

#if OBJECT_COMMUTATIVITY
void test4( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	spawn( task_cinout, (cinoutdep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );

    ssync();
}

void test5( int n ) {
    object_t<int> obj, x;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	if( i&1 )
	    spawn( task_cinout, (cinoutdep<int>)obj, (n-i)*10 );
	else
	    spawn( task_cinout, (cinoutdep<int>)x, (n-i)*10 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );

    ssync();
}
#endif

int main( int argc, char * argv[] ) {
    if( argc <= 2 ) {
	std::cerr << "Usage: " << argv[0] << " <test> <n>\n";
	return 1;
    }

    int t = atoi( argv[1] );
    int n = atoi( argv[2] );
    switch( t ) {
    case 0:
	run( test0, n );
	break;
    case 1:
	run( test1, n );
	break;
    case 2:
	run( test2, n );
	break;
    case 3:
	run( test3, n );
	break;
    case 4:
#if OBJECT_COMMUTATIVITY
	run( test4, n );
#else
	std::cerr << argv[0] << ": commutativity not enabled\n";
#endif
	break;
    case 5:
#if OBJECT_COMMUTATIVITY
	run( test5, n );
#else
	std::cerr << argv[0] << ": commutativity not enabled\n";
#endif
	break;
    }

    return 0;
}

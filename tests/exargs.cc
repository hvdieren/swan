// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"
#include "logger.h"
#include "debug.h"

using namespace obj;

#if DEBUG_CERR
void iolock() { }
void iounlock() { }
#else
pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }
#endif

void many_args( int i,
		int a, int b, int c, int d, int f, int g, int h,
		int j, int k, int l, int m, int n ) {
    printf( "we're here! %d %d %d %d %d %d %d %d %d %d %d %d %d\n",
	    i, a, b, c, d, f, g, h, j, k, l, m, n );
}

int main( int argc, char * argv[] ) {
#if !DEBUG_CERR
    pthread_mutex_init( &io_lock_var, NULL );
#endif

    if( argc <= 1 ) {
	std::cerr << "Usage: " << argv[0] << " <n>\n";
	return 1;
    }

    int n = atoi( argv[1] );
    run( many_args, n, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 );

    return 0;
}

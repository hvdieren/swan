// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"
#include "logger.h"

using namespace obj;

pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }

int my_main( int argc, char * argv[] );

/*
int f( int n, outdep<char> obj ) __attribute__((noinline));
int g( int n, indep<char> obj ) __attribute__((noinline));

int f( int n, outdep<char> obj ) {
    if( n == 1 )
	usleep(10000);
    else
	usleep(100000);
    iolock();
    std::cerr << "This is the f function with n=" << n
	      << " object=" << *obj.get_object()
	      << " ptr=" << obj.get_ptr<int>() << "\n";
    iounlock();
    return n;
}

int g( int n, indep obj ) {
    iolock();
    std::cerr << "This is the g function with n=" << n
	      << " object=" << *obj.get_object()
	      << " ptr=" << obj.get_ptr<int>() << "\n";
    iounlock();
    return n;
}
*/

int stage_a( int i, outdep<float> ab ) {
    float * f = ab.get_ptr();
    *f = float(i);
    iolock();
    std::cerr << "stage A: f in=" << f << " " << *ab.get_version() << '\n';
    iounlock();
    return 0;
}

int stage_b( indep<float> ab, outdep<int> bc ) {
    float * f = ab.get_ptr();
    int * i = bc.get_ptr();
    *i = int(*f);
    iolock();
    std::cerr << "stage B: i=" << *i << " in " << i << ' ' << *bc.get_version() << " and f in " << f << " " << *ab.get_version() << '\n';
    iounlock();
    return 0;
}

int stage_c( indep<int> bc ) {
    static int seen[100] = { 0 };
    int * i = bc.get_ptr();
    iolock();
    std::cerr << "stage C: i=" << *i << " in " << i << ' ' << *bc.get_version() << '\n';
    iounlock();
    assert( seen[*i] == 0 );
    seen[*i] = 1;
    return 0;
}

void apipe( int n ) {
    object_t<float> obj_ab = object_t<float>::create( 1 );
    object_t<int> obj_bc = object_t<int>::create( 1 );
    chandle<int> ka[n], kb[n], kc[n];

    *obj_ab.get_ptr() = -1.0;
    *obj_bc.get_ptr() = -1;

    for( int i=0; i < n; ++i ) {
	spawn( stage_a, ka[i], i, (outdep<float>)obj_ab );
	// iolock(); std::cerr << "Between A and B\n"; iounlock();
	spawn( stage_b, kb[i], (indep<float>)obj_ab, (outdep<int>)obj_bc );
	// iolock(); std::cerr << "Between B and C\n"; iounlock();
	spawn( stage_c, kc[i], (indep<int>)obj_bc );
	// iolock(); std::cerr << "Between C and A\n"; iounlock();
    }
    ssync();
}

int my_main( int argc, char * argv[] ) {
    pthread_mutex_init( &io_lock_var, NULL );

    if( argc <= 1 ) {
	std::cerr << "Usage: " << argv[0] << " <n>\n";
	return 1;
    }

    int n = atoi( argv[1] );
    object_t<char> obj = object_t<char>::create( 128 );
    std::cerr << "obj=" << obj << "\n";

/*
  chandle<int> k, l, m;
  spawn( f, k, n, (outdep)obj );
  spawn( f, m, 1, (outdep)obj );
  spawn( g, l, n, (indep)obj );
  ssync();
  std::cerr << "f(" << n << ", " << obj << ") = " << k << "\n";
*/
    call( apipe, n );

    return 0;
}

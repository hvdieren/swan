#ifndef __USE_BSD
#define __USE_BSD
#endif
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <math.h>

#define UBENCH_HOOKS
#include "wf_interface.h"

// ----------------------------------------------------------------------
// Time measurement
// ----------------------------------------------------------------------
// Some excursion in measuring time.
// gettimeofday() resolution is usec,
// clock_gettime() is very sensitive to NTP adjustments
// and rdtsc seems to give same result as gettimeofday()!
#define USE_CLOCK_RT 2

#if USE_CLOCK_RT == 1
typedef struct timespec time_val_t;
static time_val_t time_resolution;
void get_time( time_val_t * tm ) {
    if( clock_gettime( CLOCK_MONOTONIC, tm ) < 0 ) {
	fprintf( stderr, "clock_gettime: %s\n", strerror( errno ) );
	exit( 1 );
    }
}
void sub_time( time_val_t * t1, time_val_t * t2, time_val_t * tr ) {
    // Documentation states that struct timespec is like struct timeval
    // except that the second field is called nsec instead of usec.
    timersub( (struct timeval*)t1, (struct timeval*)t2, (struct timeval*)tr );
}
double read_time( time_val_t * t ) {
    double res = ((double)time_resolution.tv_sec) * 1e9 + (double)time_resolution.tv_nsec;
    double tim = ((double)t->tv_sec) * 1e9 + (double)t->tv_nsec;
    return tim - fmodl( tim, res );
}
const char * get_unit() { return "ns"; }
void show_resolution() {
    if( clock_getres( CLOCK_MONOTONIC, &time_resolution ) < 0 ) {
	fprintf( stderr, "clock_getres: %s\n", strerror( errno ) );
	exit( 1 );
    }
    double res = ((double)time_resolution.tv_sec) * 1e9 + (double)time_resolution.tv_nsec;
    printf( "Clock resolution: %ld secs, %ld nsecs, %.3lf %s\n",
            time_resolution.tv_sec, time_resolution.tv_nsec, res, get_unit() );
}
#else
#if USE_CLOCK_RT == 2
#include "rdtsc.h"
typedef unsigned long long time_val_t;
void get_time( time_val_t * tm ) {
    *tm = rdtsc();
}
void sub_time( time_val_t * t1, time_val_t * t2, time_val_t * tr ) {
    // Documentation states that struct timespec is like struct timeval
    // except that the second field is called nsec instead of usec.
    *tr = *t1 - *t2;
}
double read_time( time_val_t * t ) {
    return (double)*t;
}
const char * get_unit() { return "cycles"; }
void show_resolution() { }
#else
typedef struct timeval time_val_t;
void show_resolution() { }
void get_time( time_val_t * tm ) {
    if( gettimeofday( tm, NULL ) < 0 ) {
	fprintf( stderr, "gettimeofday: %s\n", strerror( errno ) );
	exit( 1 );
    }
}
void sub_time( time_val_t * t1, time_val_t * t2, time_val_t * tr ) {
    timersub( t1, t2, tr );
}
double read_time( time_val_t * t ) {
    return ((double)t->tv_sec) * 1e6 + (double)t->tv_usec;
}
const char * get_unit() { return "us"; }
#endif
#endif

// ----------------------------------------------------------------------
// Object interface
// ----------------------------------------------------------------------

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

// ----------------------------------------------------------------------
// Worker tasks
// ----------------------------------------------------------------------

// Sets workload size. 
int g_maxfibo;


// Iterative fibonacci. Return fibonacci(n)
int fibonacci(int n)
{
  int u = 0;
  int v = 1;
  int i, t;

  for(i = 2; i <= n; i++)
  {
    t = u + v;
    u = v;
    v = t;
  }
  return v;
}


int global_sink = 0;

/* No dependency
 * Measures the overhead of spawning a task where there is no data dependency.
 * The tasks should run in parallel.
 */
void taskWithNoArgs(int y) {
// Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_i( indep<T> arg ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_o( outdep<T> arg ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_io( inoutdep<T> arg ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_i_o( indep<T> arg1, outdep<T> arg2 ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_i_io( indep<T> arg1, inoutdep<T> arg2 ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_o_io( outdep<T> arg1, inoutdep<T> arg2 ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T>
void task_i_o_io( indep<T> arg1, outdep<T> arg2, inoutdep<T> arg3 ) {
    // Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

void taskref() {
    int x = 0;
    // for( int i=0; i<g_maxfibo; ++i )
    // x += fibonacci( i );
    x += leaf_call( fibonacci, (int)g_maxfibo );
    global_sink += x;
}

/* Reference
 * The reference time for these benchmarks is generated as:
 */
void task(int arg) {
// Do something ...
    int i;
    for(i=0; i<g_maxfibo; ++i) {
	leaf_call( fibonacci, i);
    }
}

// ----------------------------------------------------------------------
// Benchmark driver setup
// ----------------------------------------------------------------------

enum exp_enum {
    exp_reference,
    exp_pipe_serial,
    exp_NUM
};

const char * const experiment_desc[exp_NUM] = {
    "reference",
    "pipe_serial"
};

const char * const experiment_arg[exp_NUM] = {
    "ref",
    "serial"
};

exp_enum decode( const char * arg ) {
    int match=0, nmatch = 0;
    int len = strlen( arg );
    for( int i=0; i < exp_NUM; ++i ) {
	if( !strncmp( experiment_arg[i], arg, len ) ) {
	    ++nmatch;
	    match = i;
	}
    }
    return nmatch == 1 ? exp_enum(match) : exp_NUM;
}

// ----------------------------------------------------------------------
// Drivers
// ----------------------------------------------------------------------
template<typename T>
void par_pipe( unsigned int num_iters,
	       unsigned int num_stages,
	       object_t<T> ** pipe_in,
	       object_t<T> ** pipe_out,
	       object_t<T> ** pipe_inout ) {
    for( unsigned int j=0; j < num_iters; ++j ) {
	for( unsigned int s=0; s < num_stages; ++s ) {
	    if( pipe_in[s] && pipe_out[s] && pipe_inout[s] ) {
		spawn( task_i_o_io<T>, (indep<T>)*pipe_in[s],
		       (outdep<T>)*pipe_out[s],
		       (inoutdep<T>)*pipe_inout[s] );
	    } else if( !pipe_in[s] && pipe_out[s] && pipe_inout[s] ) {
		spawn( task_o_io<T>, (outdep<T>)*pipe_out[s],
		       (inoutdep<T>)*pipe_inout[s] );
	    } else if( pipe_in[s] && !pipe_out[s] && pipe_inout[s] ) {
		spawn( task_i_io<T>, (indep<T>)*pipe_in[s],
		       (inoutdep<T>)*pipe_inout[s] );
	    } else if( !pipe_in[s] && !pipe_out[s] && pipe_inout[s] ) {
		spawn( task_io<T>, (inoutdep<T>)*pipe_inout[s] );
	    } else if( pipe_in[s] && pipe_out[s] && !pipe_inout[s] ) {
		spawn( task_i_o<T>, (indep<T>)*pipe_in[s],
		       (outdep<T>)*pipe_out[s] );
	    } else if( !pipe_in[s] && pipe_out[s] && !pipe_inout[s] ) {
		spawn( task_o<T>, (outdep<T>)*pipe_out[s] );
	    } else if( pipe_in[s] && !pipe_out[s] && !pipe_inout[s] ) {
		spawn( task_i<T>, (indep<T>)*pipe_in[s] );
	    } // Last case: no arguments, no task.
	}
    }
    ssync();
}
		  

void ref_region( unsigned int niters, void (*fn)() ) {
    for( unsigned int j=0; j < niters; ++j )
	fn();
}

template<typename T>
double experiment( unsigned int num_iters, unsigned int num_stages,
		   object_t<T> ** pipe_in,
		   object_t<T> ** pipe_out,
		   object_t<T> ** pipe_inout )
    __attribute__((always_inline));

template<typename T>
double experiment( unsigned int num_iters, unsigned int num_stages,
		   object_t<T> ** pipe_in,
		   object_t<T> ** pipe_out,
		   object_t<T> ** pipe_inout ) {
    time_val_t start, end, diff;

    get_time( &start );
    run( par_pipe<T>, num_iters, num_stages, pipe_in, pipe_out, pipe_inout );
    get_time( &end );
    sub_time( &end, &start, &diff );
    return read_time( &diff );
}

double reference( unsigned int niters, void (*fn)() ) {
    time_val_t start, end, diff;

    get_time( &start );
    run( ref_region, niters, fn );
    get_time( &end );
    sub_time( &end, &start, &diff );
    return read_time( &diff );
}

// ----------------------------------------------------------------------
// Benchmark dispatch
// ----------------------------------------------------------------------
int main( int argc, char* argv[] ) {
    exp_enum exp_type = exp_NUM;
    unsigned int num_tasks = 0, num_stages = 0;
    double elapsed = 0;

    // Correct syntax is:
    // NUM_THREADS=<threads> ./data_dep1 <exp_type> <total_tasks> <workload>
  
    if( argc > 4 ) {
	exp_type   = decode( argv[1] );
	num_tasks = atoi( argv[2] );
	num_stages = atoi( argv[3] );
	g_maxfibo  = atoi( argv[4] );
    }
    if( argc <= 4 || exp_type == exp_NUM ) {
	fprintf( stderr, "Usage: %s <exp_type> <num_tasks> "
		 "<num_stages> <g_maxfibo>\n", argv[0] );
	fprintf( stderr, "\t exp_type | description\n");
	for( int i=0; i < exp_NUM; ++i )
	    fprintf(stderr, "\t %8s : %s\n",
		    experiment_arg[i], experiment_desc[i]);
	exit(1);
    }

    printf( "%s tasks=%u stages=%u workload=%u\n",
	    experiment_desc[exp_type], num_tasks, num_stages, g_maxfibo );

    show_resolution();

    object_t<int> * pipe_in[num_stages];
    object_t<int> * pipe_out[num_stages];
    object_t<int> * pipe_inout[num_stages];

    switch( exp_type ) {
    case exp_reference:
	elapsed = reference( num_tasks, taskref );
	break;
    case exp_pipe_serial:
	for( unsigned int s=0; s < num_stages; ++s ) {
	    pipe_inout[s] = new object_t<int>();
	    pipe_out[s] = s == (num_stages-1) ? 0 : new object_t<int>();
	    pipe_in[s] = s == 0 ? 0 : pipe_out[s-1];
	}
	elapsed = experiment( num_tasks, num_stages,
			      pipe_in, pipe_out, pipe_inout );
	break;
    case exp_NUM:
	elapsed = 0;
    break;
    }

    printf("Total time %.3lf %s\n", elapsed, get_unit());
    printf("Per-task time %.3lf %s\n", elapsed / num_tasks, get_unit());

    return 0;
}

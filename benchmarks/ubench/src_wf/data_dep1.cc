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

#if DELAY_DEVIATION
#include <gsl/gsl_rng.h>
#include <gsl/gsl_cdf.h>
#include <gsl/gsl_randist.h>
#endif

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
// Worker tasks
// ----------------------------------------------------------------------

// Sets workload size. 
int g_maxfibo;


// Sets deviation in workload size (gaussian distribution). 
unsigned int g_deviation;
#if DELAY_DEVIATION
gsl_rng *rand_generator;
#endif

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
    // for( int i=0; i<g_maxfibo; ++i )
    // x += leaf_call( fibonacci, i );
#ifdef DELAY_DEVIATION
    if(g_deviation != 0) {
        fibo = (unsigned int)gsl_ran_gaussian_ziggurat(rand_generator, g_deviation)
								     + g_maxfibo;
	if (fibo < 0) { fibo=0; }
    }
#endif
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T, template<typename U> class DepTy>
void taskWithArg( DepTy<T> arg ) {
// Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    // for( int i=0; i<g_maxfibo; ++i )
    // x += leaf_call( fibonacci, i );
#ifdef DELAY_DEVIATION
    if(g_deviation != 0) {
	fibo = (unsigned int)gsl_ran_gaussian_ziggurat(rand_generator, g_deviation)
								     + g_maxfibo;
	if (fibo < 0) { fibo=0; }
    }
#endif
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T, template<typename U> class DepTy1,
	 template<typename U> class DepTy2>
void taskWithTwoArgs( DepTy1<T> arg1, DepTy2<T> arg2 ) {
// Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    // for( int i=0; i<g_maxfibo; ++i )
    // x += leaf_call( fibonacci, i );
#ifdef DELAY_DEVIATION
    if(g_deviation != 0) {
	fibo = (unsigned int)gsl_ran_gaussian_ziggurat(rand_generator, g_deviation)
								     + g_maxfibo;
	if (fibo < 0) { fibo=0; }
    }
#endif
    x += leaf_call( fibonacci, (int)fibo );
    global_sink += x;
}

template<typename T, template<typename U> class DepTy1,
	 template<typename U> class DepTy2,
	 template<typename U> class DepTy3>
void taskWithThreeArgs( DepTy1<T> arg1, DepTy2<T> arg2, DepTy3<T> arg3 ) {
// Do something ...
    int x = 0;
    int fibo=g_maxfibo;
    // for( int i=0; i<g_maxfibo; ++i )
    // x += leaf_call( fibonacci, i );
#ifdef DELAY_DEVIATION
    if(g_deviation != 0) {
	fibo = (unsigned int)gsl_ran_gaussian_ziggurat(rand_generator, g_deviation)
								     + g_maxfibo;
	if (fibo < 0) { fibo=0; }
    }
#endif
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
// Object interface, benchmark driver setup
// ----------------------------------------------------------------------

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;
#if OBJECT_COMMUTATIVITY
using obj::cinoutdep;
#endif
#if OBJECT_REDUCTION
using obj::reduction;
#endif
using obj::truedep;

#if OBJECT_REDUCTION
template<typename T>
struct add_monad {
    typedef T value_type;
    typedef obj::cheap_reduction_tag reduction_tag;
    static void identity( T * p ) { new (p) T(0); }
    static void reduce( T * left, T * right ) { *left += *right; }
};

template<typename T>
struct madd_monad {
    typedef T value_type;
    typedef obj::expensive_reduction_tag reduction_tag;
    static void identity( value_type * p ) { new (p) T(0); }
    static void reduce( value_type * left, value_type * right ) { taskref(); }
};
#endif

enum exp_enum {
    exp_cpuid,
    exp_atomic_inc,
    exp_atomic_dec,
    exp_reference,
    exp_nodep,
    exp_indep,
    exp_outdep,
    exp_inoutdep,
    exp_cinoutdep,
    exp_scalar_reduction,
    exp_object_reduction,
    exp_truedep,
    exp_cholesky,
    exp_NUM
};

const char * const experiment_desc[exp_NUM] = {
    "cpuid overhead",
    "atomic increment",
    "atomic decrement",
    "reference",
    "no dependency",
    "input dependency",
    "output dependency",
    "in/out dependency",
    "commutative in/out dependency",
    "scalar reduction in/out dependency",
    "object reduction in/out dependency",
    "true dependency",
    "cholesky structure"
};

const char * const experiment_arg[exp_NUM] = {
    "cpuid",
    "inc",
    "dec",
    "ref",
    "nodep",
    "indep",
    "outdep",
    "inoutdep",
    "cinoutdep",
    "scalred",
    "objred",
    "truedep",
    "cholesky"
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
template<typename M, typename T, template<typename U> class DepTy>
void par_region( unsigned int niters, object_t<T> * obj ) {
    for( unsigned int j=0; j < niters; ++j )
	spawn( taskWithArg<M, DepTy>, (DepTy<M>)*obj );
    ssync();
}

template<typename M, typename T, template<typename U> class DepTy>
void par_region2( unsigned int niters, unsigned int nobj, object_t<T> * obj ) {
    unsigned int i = 0;
    for( unsigned int j=0; j < niters; ++j ) {
	spawn( taskWithArg<M, DepTy>, (DepTy<M>)obj[i] );
	if( ++i == nobj )
	    i = 0;
    }
    ssync();
}

template<typename T>
struct fake_dependency {
    inoutdep<T> io_obj;
    stack_frame * cur;
    stack_frame * fr0;
    full_frame * ff0;

    fake_dependency( object_t<T> * obj, stack_frame * cur_ ) : cur( cur_ ) { 
	io_obj = inoutdep<T>::create( obj->get_version() );
	io_obj.get_version()->add_ref(); // don't deallocate (for outdep)

	// statically computed
	size_t args_size = arg_size( io_obj );
	size_t tags_size
	    = the_task_graph_traits::arg_stored_size< inoutdep<T> >();
	size_t fn_tags_size = the_task_graph_traits::fn_stored_size();
	size_t num_args = arg_num< inoutdep<T> >();

	// create a stack frame
	fr0 = new stack_frame( args_size, tags_size, fn_tags_size, num_args,
			       cur, 0, false, false );
	fr0->push_args( io_obj );

	// issue it to the task graph
	wf_arg_issue( fr0, cur, io_obj );

	// convert it to a full frame - not really necessary?
	ff0 = fr0->convert_to_full();
    }
    ~fake_dependency() {
	// Don't release, already done before sync.
	// the_task_graph_traits::release_task( fr0 );
	// Redo the decrement of the release
	fr0->get_parent()->get_full()->add_child();
	ff0->lock( fr0->get_owner() );
	fr0->set_state( fs_executing );
	ff0->~full_frame();
	delete fr0;

	io_obj.get_version()->del_ref();
    }

    void issue() {
	// issue it to the task graph
	// auto ofr0 = stack_frame_traits<stack_frame>::get_metadata( fr0 );
	// obj::arg_dgrab_fn( ofr0, ofr0, true, io_obj );
	fr0->get_parent()->get_full()->add_child();
	wf_arg_issue( fr0, cur, io_obj );
    }

    void release() {
	the_task_graph_traits::release_task( fr0 );
	fr0->get_parent()->get_full()->remove_child();
#if OBJECT_TASKGRAPH == 1 || OBJECT_TASKGRAPH == 8
	// update_depth() triggers assert in debug mode
	io_obj.get_version()->get_metadata()->update_depth_ubench(0);
	// clear out task graph (hashed lists)
	stack_frame_traits<full_frame>::get_metadata( ff0 )->reset();
#endif
#if OBJECT_TASKGRAPH == 5 || OBJECT_TASKGRAPH == 9
	stack_frame_traits<stack_frame>::get_metadata( fr0 )->reset_deps();
#endif
    }
};

template<typename T>
unsigned int
par_cholesky( unsigned int niters, unsigned int DIM, object_t<T> * obj ) {
    unsigned int num_tasks = 0;
    for( unsigned int ni=0; ni < niters; ++ni ) {
	for( unsigned int j=0; j < DIM; ++j ) {
	    for( unsigned int k=0; k < j; ++k ) {
		for( unsigned int i=j+1; i < DIM; ++i ) {
		    spawn( taskWithThreeArgs<T, indep, indep, inoutdep>,
			   (indep<T>)obj[i+DIM*k], (indep<T>)obj[j+DIM*k],
			   (inoutdep<T>)obj[i+DIM*j] );
		    ++num_tasks;
		}
	    }
	    for( unsigned int i=0; i < j; ++i ) {
		spawn( taskWithTwoArgs<T, indep, inoutdep>,
		       (indep<T>)obj[j+DIM*i], (inoutdep<T>)obj[j+DIM*j] );
		++num_tasks;
	    }
	    spawn( taskWithArg<T, inoutdep>, (inoutdep<T>)obj[j+DIM*j] );
	    ++num_tasks;
	    for( unsigned int i=j+1; i < DIM; ++i ) {
		spawn( taskWithTwoArgs<T, indep, inoutdep>,
		       (indep<T>)obj[j+DIM*j], (inoutdep<T>)obj[i+DIM*j] );
		++num_tasks;
	    }
	}
	ssync();
    }
    return num_tasks;
}

template<typename M, typename T, template<typename U> class DepTy>
void par_region4( unsigned int niters, unsigned int nobj,
		  unsigned int batch1, unsigned int batch2,
		  unsigned int batchT, object_t<T> * obj ) {
    // This code is intended to be run by only a single thread.
    // It is kind of "hacky"
    // auto md = (*obj).get_version()->get_metadata();
    unsigned int i=0;
    unsigned int b=0;

    fake_dependency<T> fake_dep( obj, stack_frame::my_stack_frame() );
    --niters;

    // errs() << "v=" << obj->get_version() << "\n";

    for( unsigned int j=0; j < niters; ++j ) {
	// errs() << "j=" << j << " i=" << i << "\n";
	if( b < batch1 )
	    spawn( taskWithArg<T, indep>, (indep<T>)*obj );
	else
	    spawn( taskWithArg<M, DepTy>, (DepTy<M>)*obj );

	if( ++b == batch1+batch2 )
	    b = 0;

	if( ++i == batchT ) {
	    fake_dep.release();

	    // errs() << "inl sync\n";
	    ssync();

	    fake_dep.issue();
	    --niters;
	    i = 0;
	}
    }

    fake_dep.release();

    // errs() << "final sync\n";
    ssync();
}

template<typename M, typename T, template<typename U> class DepTy>
void par_region4b( unsigned int niters, unsigned int nobj,
		   unsigned int batch1, unsigned int batch2,
		   unsigned int batchT, object_t<T> * obj ) {
    // This code is intended to be run by only a single thread.
    // It is kind of "hacky"
    // auto md = (*obj).get_version()->get_metadata();
    unsigned int i=0;
    unsigned int b=0;

    // errs() << "v=" << obj->get_version() << "\n";

    for( unsigned int j=0; j < niters; ++j ) {
	// errs() << "j=" << j << " i=" << i << "\n";
	if( b < batch1 )
	    spawn( taskWithArg<T, indep>, (indep<T>)*obj );
	else
	    spawn( taskWithArg<M, DepTy>, (DepTy<M>)*obj );

	if( ++b == batch1+batch2 )
	    b = 0;

	if( ++i == batchT ) {
	    // errs() << "inl sync\n";
	    ssync();
	    i = 0;
	}
    }

    // errs() << "final sync\n";
    ssync();
}


template<typename M, typename T, template<typename U> class DepTy>
void par_region3( unsigned int niters, unsigned int nobj,
		  unsigned int batch1, unsigned int batch2,
		  object_t<T> * obj ) {
    // This code is intended to be run by only a single thread.
    // It is kind of "hacky"
    // auto md = (*obj).get_version()->get_metadata();
    unsigned int i=0;

    fake_dependency<T> fake_dep( obj, stack_frame::my_stack_frame() );
    --niters;

    // errs() << "v=" << obj->get_version() << "\n";

    for( unsigned int j=0; j < niters; ++j ) {
	// errs() << "j=" << j << " i=" << i << "\n";
	if( i < batch1 )
	    spawn( taskWithArg<T, indep>, (indep<T>)*obj );
	else
	    spawn( taskWithArg<M, DepTy>, (DepTy<M>)*obj );

	if( ++i == batch1 + batch2 ) {
	    fake_dep.release();

	    // errs() << "inl sync\n";
	    ssync();

	    fake_dep.issue();
	    --niters;
	    i = 0;
	}
    }

    fake_dep.release();

    // errs() << "final sync\n";
    ssync();
}


template<typename T>
void par_region( unsigned int niters, void (*fn)(T) ) {
    T x = 0;
    for( unsigned int j=0; j < niters; ++j )
	call( fn, x );
}

void ref_region( unsigned int niters, void (*fn)() ) {
    for( unsigned int j=0; j < niters; ++j )
	fn();
}

template<typename T, template<typename U> class DepTy, typename M = T>
double experiment( unsigned int niters, unsigned int nobj,
		   unsigned int batch1, unsigned int batch2,
		   unsigned int batchT, size_t prebuild, object_t<T> * obj )
    __attribute__((always_inline));
template<typename T>
double experiment( unsigned int niters, void (*fn)(T) )
    __attribute__((always_inline));
double reference( unsigned int niters, int (*fn)() )
    __attribute__((always_inline));

template<typename T, template<typename U> class DepTy, typename M>
double experiment( unsigned int niters, unsigned int nobj,
		   unsigned int batch1, unsigned int batch2,
		   unsigned int batchT, size_t prebuild, object_t<T> * obj ) {
    time_val_t start, end, diff;

    get_time( &start );
    if( batchT > 0 ) {
	if( prebuild )
	    run( par_region4<M, T, DepTy>, niters, nobj, batch1, batch2, batchT, obj );
	else
	    run( par_region4b<M, T, DepTy>, niters, nobj, batch1, batch2, batchT, obj );
    } else if( batch1+batch2 > 0 ) {
	run( par_region3<M, T, DepTy>, niters, nobj, batch1, batch2, obj );
    } else if( nobj > 1 )
	run( par_region2<M, T, DepTy>, niters, nobj, obj );
    else
	run( par_region<M, T, DepTy>, niters, obj );
    get_time( &end );
    sub_time( &end, &start, &diff );
    return read_time( &diff );
}

template<typename T>
double experiment( unsigned int niters, void (*fn)(T) ) {
    time_val_t start, end, diff;

    get_time( &start );
    run( par_region<T>, niters, fn );
    get_time( &end );
    sub_time( &end, &start, &diff );
    return read_time( &diff );
}

template<typename T>
double experiment_cholesky( unsigned int & num_tasks, unsigned int niters,
			    unsigned int DIM, object_t<T> * obj ) {
    time_val_t start, end, diff;

    get_time( &start );
    num_tasks = run( par_cholesky<T>, niters, DIM, obj );
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
    unsigned int j;
    exp_enum exp_type = exp_NUM;
    unsigned int num_tasks = 0, num_objects = 0, batch1 = 0, batch2 = 0,
	batchT = 0, prebuild = 0;
    double elapsed = 0;

    // Correct syntax is:
    // NUM_THREADS=<threads> ./data_dep1 <exp_type> <total_tasks> <workload>
  
    if( argc > 5 ) {
	exp_type   = decode( argv[1] );
	num_tasks = atoi( argv[2] );
	num_objects  = atoi( argv[3] );
	if( sscanf( argv[4], "%d:%d:%d:%d", &batch1, &batch2, &batchT,
		    &prebuild ) != 4 ) {
	    if( sscanf( argv[4], "%d:%d:%d", &batch1, &batch2, &batchT )
		!= 3 ) {
		if( sscanf( argv[4], "%d:%d", &batch1, &batch2 ) != 2 )
		    exp_type = exp_NUM; // failure code
		batchT = 0;
	    }
	    prebuild = batchT > 0;
	}
	g_maxfibo  = atoi( argv[5] );
	if(argc > 6) {
	    g_deviation = atoi(argv[6]);
	} else {
	    g_deviation = 0;
	}
    }
    if( argc <= 5 || exp_type == exp_NUM || num_objects <= 0 ) {
	fprintf( stderr, "Usage: %s <exp_type> <num_tasks> <num_objects> "
		 "<batch1>:<batch2> <g_maxfibo> [deviation]\n", argv[0] );
	fprintf( stderr, "\t exp_type | description\n");
	for( int i=0; i < exp_NUM; ++i )
	    fprintf(stderr, "\t %8s : %s\n",
		    experiment_arg[i], experiment_desc[i]);
	exit(1);
    }

    printf( "%s tasks=%u objects=%u batch=%d:%d:%d:%d workload=%u "
	    "deviation=%u\n",
	    experiment_desc[exp_type], num_tasks, num_objects,
	    batch1, batch2, batchT, prebuild, g_maxfibo, g_deviation );

    //Set up enviroment for GSL (random number generator with gaussian distro)
#if DELAY_DEVIATION
    gsl_rng_env_setup();
    rand_generator = gsl_rng_alloc(gsl_rng_default);
#endif

    show_resolution();

    object_t<int> param[num_objects];

    switch( exp_type ) {
    case exp_reference:
	elapsed = reference( num_tasks, taskref );
	break;
    case exp_nodep:
	elapsed = experiment( num_tasks, taskWithNoArgs );
	break;
    case exp_indep:
	elapsed = experiment<int, indep>(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
    case exp_outdep:
	elapsed = experiment<int, outdep>(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
    case exp_inoutdep:
	elapsed = experiment<int, inoutdep>(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
#if OBJECT_COMMUTATIVITY
    case exp_cinoutdep:
	elapsed = experiment<int, cinoutdep>(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
#endif
#if OBJECT_REDUCTION
    case exp_scalar_reduction:
	elapsed = experiment<int, reduction, add_monad<int> >(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
    case exp_object_reduction:
	elapsed = experiment<int, reduction, madd_monad<int> >(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
#endif
    case exp_truedep:
	elapsed = experiment<int, truedep>(
	    num_tasks, num_objects, batch1, batch2, batchT, prebuild, param );
	break;
    case exp_cholesky:
    {
	if( batch1*batch1 > num_objects ) {
	    fprintf(stderr, "\t %8s : too few objects for dimensions (batch1)\n",
		    experiment_arg[(int)exp_cholesky] );
	    exit(1);
	}
	unsigned int num_iters = num_tasks;
	elapsed = experiment_cholesky<int>( num_tasks, num_iters, batch1, param ); 
	break;
    }
    case exp_atomic_inc:
    {
	time_val_t start, end, diff;
	int var = 0;
	get_time( &start );
	for (j = 0; j < num_tasks; ++j) {
	    __sync_fetch_and_add( &var, 1 );
	    // __asm__ __volatile__( "cpuid\n\t" : : : "rax", "rbx", "rcx", "rdx" );
	}
	get_time( &end );
	sub_time( &end, &start, &diff );
	elapsed = read_time( &diff );
    }
    break;
    case exp_atomic_dec:
    {
	time_val_t start, end, diff;
	int var = 0;
	get_time( &start );
	for (j = 0; j < num_tasks; ++j) {
	    __sync_fetch_and_add( &var, -1 );
	    // __asm__ __volatile__( "cpuid\n\t" : : : "rax", "rbx", "rcx", "rdx" );
	}
	get_time( &end );
	sub_time( &end, &start, &diff );
	elapsed = read_time( &diff );
    } 
    break;
    case exp_cpuid:
    {
	time_val_t start, end, diff;
	get_time( &start );
	for (j = 0; j < num_tasks; ++j) {
	    __asm__ __volatile__( "cpuid\n\t" : : : "rax", "rbx", "rcx", "rdx" );
	}
	get_time( &end );
	sub_time( &end, &start, &diff );
	elapsed = read_time( &diff );
    }
    break;
    case exp_NUM:
	elapsed = 0;
    break;
    }

    printf("Total time %.3lf %s\n", elapsed, get_unit());
    printf("Per-task time %.3lf %s\n", elapsed / num_tasks, get_unit());

    return 0;
}

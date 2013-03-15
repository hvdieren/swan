/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.gr)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.gr)
 * 
 * This file is part of Swan.
 * 
 * Swan is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Swan is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Swan.  If not, see <http://www.gnu.org/licenses/>.
 */

// -*- c++ -*-
#include "swan_config.h"

#include <cstdlib>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <csetjmp>

#ifdef HAVE_LIBHWLOC
#include <hwloc.h>
#endif

#include <iostream>
#include <deque>
#include <vector>

#include "wf_spawn_deque.h"
#include "wf_stack_frame.h"
#include "wf_worker.h"
#include "wf_interface.h"
#include "logger.h"

worker_state * ws;
__thread worker_state * tls_worker_state;

logger * thread_logger = 0;
__thread logger * tls_thread_logger = 0;

size_t nthreads;
__thread size_t threadid;
static pthread_t * thread;

volatile long ini_barrier = 0;

void validate_spawn_deque( spawn_deque * d ) {
    bool fnd = false;
    for( unsigned i=0; i < nthreads; ++i )
	if( d == ws[i].get_deque() ) {
	    fnd = true;
	    break;
	}
    if( !fnd )
	exit( 1 );
}

void wf_initialize() {
    const char * pv = getenv( "PRINT_VERSION" );
    if( pv && atoi(pv) > 0 ) {
#include "current_version.h"
#define xstr(s) str(s)
#define str(s) #s
#define SHOWI(x) "\n\t"#x" = " << int(x)
#define SHOWS(x) "\n\t"#x" = " << xstr(x)
	std::cerr << "Work-first unified scheduler, version "
		  << SVN_VERSION
		  << "\nConfiguration:"
		  << SHOWI(SUPPORT_GDB)
		  << SHOWI(DBG_CONTINUATION)
		  << SHOWI(DBG_VERIFY)
		  << SHOWI(DBG_LOGGER)
		  << SHOWI(DBG_SF_LOCKER)
		  << SHOWI(DBG_MCS_MUTEX)
		  << SHOWI(PROFILE_WORKER)
		  << SHOWI(PROFILE_WORKER_SUMMARY)
		  << SHOWI(PROFILE_SPAWN_DEQUE)
		  << SHOWI(PROFILE_OBJECT)
		  << SHOWI(OBJECT_INOUT_RENAME)
		  << SHOWI(TIME_STEALING)
		  << SHOWS(PREFERRED_MUTEX)
		  << SHOWI(FF_MCS_MUTEX)
		  << SHOWI(TIME_STEALING)
		  << SHOWI(TRACING)
		  << SHOWI(DEBUG_CERR)
		  << SHOWI(IMPROVED_STUBS)
		  << SHOWI(OBJECT_TASKGRAPH)
		  << SHOWI(OBJECT_COMMUTATIVITY)
		  << SHOWI(OBJECT_REDUCTION)
		  << SHOWI(CACHE_ALIGNMENT)
		  << SHOWI(STACK_FRAME_SIZE)
#ifdef HAVE_LIBHWLOC
		  << "\n\tHAVE_LIBHWLOC = 1"
#else
		  << "\n\tHAVE_LIBHWLOC = 0"
#endif
		  << SHOWI(PACT11_VERSION)
		  << '\n';
#undef xstr
#undef str
#undef SHOWI
#undef SHOWS
	if( atoi(pv) > 1 )
	    exit( 0 );
    }

    const char * str = getenv( "NUM_THREADS" );
    nthreads = str ? atoi( str ) : 2;

    ws = new worker_state[nthreads];
    thread = new pthread_t[nthreads];
    thread_logger = new logger[nthreads];

    // Creation of thread 0 (main thread)
    tls_worker_state = &ws[0];
    tls_thread_logger = &thread_logger[0];
    threadid = 0;

    // Get the initial thread affinity for the initial thread.
    // All threads will be scheduled to this set in round-robin fashion.
#if !defined( __APPLE__ )
    cpu_set_t cpu_hint;
    unsigned cpu_current;
    unsigned cpu_max = sizeof(cpu_hint)*8;
    pthread_getaffinity_np( pthread_self(), sizeof(cpu_hint), &cpu_hint );
    if( (unsigned)CPU_COUNT( &cpu_hint ) < nthreads ) {
	std::cerr << "Error: number of CPUs in initial affinity set ("
		  << CPU_COUNT( &cpu_hint )
		  << ") is less than number of threads ("
		  << nthreads << ").\n";
    }
    for( cpu_current=0; cpu_current < cpu_max; ++cpu_current ) 
       if( CPU_ISSET( cpu_current, &cpu_hint ) )
           break;
#else
    unsigned cpu_current = 0; // No affinity yet for MacOSX
#endif

#ifdef HAVE_LIBHWLOC
    // Use HWLOC library to figure out cores and memory nodes
    // Allocate and initialize topology object.
    hwloc_topology_t topology;
    hwloc_topology_init( &topology );
    
    // Perform the topology detection.
    hwloc_topology_load( topology );

    // Using PU here instead of CORE means we accept hyper-threaded
    // architectures and we will run threads on them
    int pdepth = hwloc_get_type_or_below_depth( topology, HWLOC_OBJ_PU );
    if( pdepth == HWLOC_TYPE_DEPTH_UNKNOWN ) {
	fprintf( stderr, "hwloc: no objects of type PU?\n" );
	exit( 2 );
    }
    size_t num_pus = hwloc_get_nbobjs_by_depth( topology, pdepth );
    if( num_pus < nthreads ) {
	fprintf( stderr, "hwloc: fewer PUs (%lu) than threads (%lu)\n",
		 num_pus, nthreads );
	exit( 2 );
    }

    // Initializing main and other threads
    for( size_t i=0; i < nthreads; ++i ) {
	hwloc_obj_t pu
	    = hwloc_get_obj_by_type( topology, HWLOC_OBJ_PU, cpu_current );
	if( !pu ) {
	    fprintf( stderr, "hwloc: could not find object for PU %u\n",
		     cpu_current );
	    exit( 2 );
	}
	hwloc_obj_t obj;
	for( obj=pu->parent; obj; obj=obj->parent ) {
	    if( obj->type == HWLOC_OBJ_NODE )
		break;
	}
	if( !obj ) {
	    fprintf( stderr, "hwloc: could not find memory for thread %lu, "
	    	     "PU %u\n", i, cpu_current );
	    exit( 2 );
	}

	ws[i].initialize( i, nthreads, topology, pu->logical_index,
			  obj->logical_index, ws[0].get_future() );

#if !defined( __APPLE__ )
	// Advance to next allowed (hinted) CPU
	for( ++cpu_current; cpu_current < cpu_max; ++cpu_current ) 
	   if( CPU_ISSET( cpu_current, &cpu_hint ) )
	       break;
#else
	++cpu_current;
#endif
    }
#else
    for( size_t i=0; i < nthreads; ++i ) {
	ws[i].initialize( i, nthreads, cpu_current, 0, ws[0].get_future() );
#if !defined( __APPLE__ )
	// Advance to next allowed (hinted) CPU
	for( ++cpu_current; cpu_current < cpu_max; ++cpu_current ) 
	   if( CPU_ISSET( cpu_current, &cpu_hint ) )
	       break;
#else
	++cpu_current;
#endif
    }
#endif

    ws[0].cpubind();
    ini_barrier = nthreads - 1;

    // Creation of other threads
    for( size_t i=1; i < nthreads; ++i ) {
	pthread_create( &thread[i], NULL, worker_state::initiator,
			reinterpret_cast<void *>( &ws[i] ) );
    }

    // Why do we want to have a barrier here? In principle, we only need the
    // current (initial) thread to start executing the program. But, if we
    // do a parallel initialization phase (e.g. assuming that each thread
    // will participate in the initialization and allocate data in its own
    // NUMA partition using the first-touch policy) then we better make sure
    // that we don't miss out on any thread or we will feel it throughout the
    // application.
    // Also, it may solve some issues with performance measurement.
    while( ini_barrier > 0 ); // busy-wait barrier :(
}

void wf_shutdown() {
    // Make sure that the computation is flagged as finished, in case
    // the executed call path of the program did not execute in parallel.
    ws[0].get_future()->flag_result();

    // Signal all threads to shutdown
    for( size_t i=0; i < nthreads; ++i )
	ws[i].shutdown();

    // Join the threads
    for( size_t i=1; i < nthreads; ++i )
	pthread_join( thread[i], NULL );

#if PROFILE_WORKER && PROFILE_WORKER_SUMMARY
    worker_state::profile_worker summary;
    for( size_t i=0; i < nthreads; ++i )
	summary.summarize( ws[i].get_profile_worker() );
    summary.dump_profile( 0 );
#endif

#if PROFILE_OBJECT
    obj::dump_statistics();
#endif

    delete[] ws;
    delete[] thread;
    delete[] thread_logger;
}

struct wf_initializer {
    wf_initializer() { wf_initialize(); }
    ~wf_initializer() { wf_shutdown(); }
};

static wf_initializer execute_calls_around_main;

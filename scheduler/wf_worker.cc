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

#include "swan_config.h"

#include <sched.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_LIBHWLOC
#include <hwloc.h>
#endif

#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cerrno>

#include "platform.h"
#include "wf_worker.h"
#include "wf_stack_frame.h"
#include "wf_spawn_deque.h"
#include "wf_setup_stack.h"
#include "logger.h"

extern worker_state * ws;

worker_state *
worker_state::get_thread_worker_state() {
    extern __thread worker_state * tls_worker_state;
    return tls_worker_state;
}

#if PROFILE_WORKER
worker_state::profile_worker::profile_worker() :
#define INIT(x) num_##x(0)
    INIT(invoke),
    INIT(waiting),
    INIT(pending),
    INIT(ssync),
    INIT(split_ret),
    INIT(split_body),
    INIT(resume),
    INIT(steal_attempt),
    INIT(steal_delay),
    INIT(setjmp),
    INIT(longjmp),
    INIT(edc_call),
    INIT(edc_spawn),
    INIT(edc_sync),
    INIT(edc_bootstrap),
    INIT(uncond_steals),
    INIT(provgd_steals),
    INIT(provgd_steals_fr),
    INIT(random_steals),
    INIT(focussed_steals)
#undef INIT
{
     memset( &time_since_longjmp, 0, sizeof(time_since_longjmp) );
     memset( &time_longjmp, 0, sizeof(time_longjmp) );
     memset( &time_clueless, 0, sizeof(time_clueless) );
     memset( &time_release_ready_success, 0, sizeof(time_release_ready_success) );
     memset( &time_release_ready_fail, 0, sizeof(time_release_ready_fail) );
}

worker_state::profile_worker::~profile_worker() {
}

void
worker_state::profile_worker::summarize( const worker_state::profile_worker & w ) {
#define SUM(x) num_##x += w.num_##x
    SUM(invoke);
    SUM(waiting);
    SUM(pending);
    SUM(ssync);
    SUM(split_ret),
    SUM(split_body),
    SUM(resume);
    SUM(steal_attempt);
    SUM(steal_delay);
    SUM(setjmp);
    SUM(longjmp);
    SUM(edc_call);
    SUM(edc_spawn);
    SUM(edc_sync);
    SUM(edc_bootstrap);
    SUM(uncond_steals);
    SUM(provgd_steals);
    SUM(provgd_steals_fr);
    SUM(random_steals);
    SUM(focussed_steals);
#undef SUM

    pp_time_max( &time_since_longjmp, &w.time_since_longjmp );
    pp_time_add( &time_longjmp, &w.time_longjmp );
    pp_time_add( &time_clueless, &w.time_clueless );
    pp_time_add( &time_release_ready_success, &w.time_release_ready_success );
    pp_time_add( &time_release_ready_fail, &w.time_release_ready_fail );
#if PROFILE_QUEUE
    queue += w.queue;
#endif // PROFILE_QUEUE
}
#endif

worker_state::worker_state()
    : cresult( 0 ), main_sp( 0 ), root( 0 ), dummy( 0 ),
      my_cpu( 0 ), my_mem( 0 )
{
}

worker_state::~worker_state() {
#if PROFILE_WORKER
    pp_time_end( &wprofile.time_since_longjmp );
    // Assuming an array of worker_state is deleted at once, there will
    // be no races on I/O during dumping of the profile.
#if !PROFILE_WORKER_SUMMARY
    wprofile.dump_profile( id );
#endif
#endif
}

#if PROFILE_WORKER
void
worker_state::profile_worker::dump_profile( size_t id ) const {
    std::cerr << "Profile ID=" << id;
#define DUMP(x) std::cerr << "\n num_" << #x << '=' << num_##x
    DUMP(invoke);
    DUMP(waiting);
    DUMP(pending);
    DUMP(ssync);
    std::cerr << "\n num_tasks=" << (num_invoke+num_waiting+num_pending);
    DUMP(split_ret),
    DUMP(split_body),
    DUMP(resume);
    // DUMP(steal_attempt);
    // DUMP(steal_delay);
    DUMP(setjmp);
    DUMP(longjmp);
    DUMP(edc_call);
    DUMP(edc_spawn);
    DUMP(edc_sync);
    DUMP(edc_bootstrap);
    DUMP(uncond_steals);
    DUMP(provgd_steals);
    DUMP(provgd_steals_fr);
    DUMP(random_steals);
    DUMP(focussed_steals);
    std::cerr << '\n';
#undef DUMP
#define SHOW(x) pp_time_print( (pp_time_t *)&x, (char *)#x )
    SHOW( time_since_longjmp );
    SHOW( time_longjmp );
    SHOW( time_clueless );
    SHOW( time_release_ready_success );
    SHOW( time_release_ready_fail );
#undef SHOW
#if PROFILE_QUEUE
    queue.dump_profile();
#endif // PROFILE_QUEUE
}
#endif

#if PROFILE_WORKER
#define PROFILE(x)  ++(wprofile.num_##x)
#else
#define PROFILE(x)
#endif

void worker_state::initialize( size_t id_, size_t nthreads_,
#ifdef HAVE_LIBHWLOC
			       hwloc_topology_t topology_,
#endif
			       size_t cpu_, size_t mem_,
			       future * cresult_ ) {
    id = id_;
    nthreads = nthreads_;
#ifdef HAVE_LIBHWLOC
    topology = topology_;
#endif
    my_cpu = cpu_;
    my_mem = mem_;

    if( id == 0 ) {
	dummy = (new stack_frame())->get_full();
	cresult = new future();
    } else
	cresult = cresult_;
}

void
worker_state::cpubind() const {
#if !defined( __APPLE__ )
#ifdef HAVE_LIBHWLOC
    hwloc_bitmap_t bits = hwloc_bitmap_alloc();
    hwloc_bitmap_set( bits, my_mem );
    if( hwloc_set_membind_nodeset( topology, bits, HWLOC_MEMBIND_BIND,
				   HWLOC_MEMBIND_THREAD ) < 0 ) {
	fprintf( stderr, "hwloc: ID=%lu could not bind to memory %lu"
		 ": %s\n", id, my_mem, strerror(errno) );
	exit( 2 );
    }

    hwloc_bitmap_zero( bits );
    hwloc_bitmap_set( bits, my_cpu );
    if( hwloc_set_cpubind( topology, bits, HWLOC_MEMBIND_THREAD ) < 0 ) {
	fprintf( stderr, "hwloc: ID=%lu could not bind to memory %lu"
		 ": %s\n",
		 id, my_mem, strerror(errno) );
	exit( 2 );
    }

    hwloc_bitmap_free( bits );
#else
    // Force thread on the appropriate CPU
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(my_cpu, &set);
    if( sched_setaffinity( 0, sizeof(set), &set ) < 0 ) {
	std::cerr << "sched_setaffinity for thread id=" << id
		  << " on cpu=" << id << " fails due to: "
		  << strerror( errno ) << "\n";
    }
#endif
#endif

    /*
    cpu_set_t cpu_set;
    pthread_getaffinity_np( pthread_self(), sizeof(cpu_set), &cpu_set );
    std::cout << "CHECK: thread " << id << " has " << CPU_COUNT(&cpu_set) << " cpus in set\n";
    for( unsigned i=0; i < nthreads; ++i )
       if( CPU_ISSET( i, &cpu_set ) ) std::cout << "cpu " << i << " is set\n";
    */

}

void
worker_state::longjmp( int retval ) {
    extern __thread worker_state * tls_worker_state;
    assert( retval != 0 && "longjmp(): only return 0 from setjmp()" );
    LOG( id_longjmp, retval );
#if PROFILE_WORKER
    pp_time_start( &tls_worker_state->wprofile.time_longjmp );
#endif
    ::longjmp( tls_worker_state->jb_ret, retval );
}

/* @brief
 * Select a victim worker to steal from at random. Make sure that we
 * do not select ourselves and that the victim has stealable work.
 * We return when the random number generation fails because we want
 * to poll in between for termination.
 */
void
worker_state::random_steal() {
    assert( sd.empty() && "Stack must be empty for random stealing" );

    // if( backoff.maybe_delay() ) {
	// PROFILE(steal_delay);
    // }
    size_t victim = lf_rand() % nthreads;

    // size_t inc = 64;
    // size_t island = id&~3;
    // size_t island_size = island+4 > nthreads ? nthreads-island : 4;
    // size_t num = lf_rand() % (nthreads+inc*island_size);
    // size_t victim = num < inc*island_size
    // ? (island + num/inc) : (num - inc*island_size);

    assert( victim < nthreads && "victim out of range" );
    // TODO: the stealable attribute does not take into
    // account if there are ready data-flow siblings of
    // the spawn deque top.
    if( victim == id || !ws[victim].sd.stealable() ) {
	// backoff.update( false );
	return;
    }
    LOG( id_steal_stack, ws[victim].sd.get_head()*nthreads+victim );
    full_frame * ff = ws[victim].sd.steal_stack( &sd, sm_random );
    if( ff ) {
	PROFILE(random_steals);
	LOG( id_random_steal, ff );
    }
}

// Need to take into account that dummy frame too will be stolen...
void
worker_state::provably_good_steal( full_frame * fr ) {
    LOG( id_provably_good_steal, fr );
    PROFILE( provgd_steals );

    assert( sd.empty()
	    && "Can only provably-good-steal when extended deque is empty" );
    assert( fr->test_lock( &sd ) && "Must have locked frame" );
    assert( fr->get_frame()->get_owner() != &sd
	    && "Provably-good-steal only on frame not already owned" );
    assert( ( !fr->get_frame()->get_owner()
	      || fr->get_frame()->get_state() == fs_dummy
	      || !fr->get_frame()->get_owner()->empty()
	      || fr->get_frame()->get_state() == fs_executing )
	    && "Provably-good-steal of frame owned by empty deque & !exec?" );

    if( fr->get_frame()->get_state() == fs_suspended
	&& fr->all_children_done() ) {
	// Steal suspended frame
	LOG( id_steal_suspended, fr );
	fr->get_frame()->set_owner( &sd );
	fr->get_frame()->set_state( fs_waiting );
	sd.insert_stack( fr );
	fr->unlock( &sd );
	PROFILE( provgd_steals_fr );
	return;
    }

    // Early release parent lock, replace by partial lock
    // fr->lock_pending( &sd );
    // fr->unlock( &sd );

    // If there are children outstanding that are woken up by us, find them
    // and "steal" them. This should not require a lock on the parent per se
    // (only to protect us against changes in the sibling list).

    // It makes sense to only scan the task graph if we are suspended
    // (came here via edc_sync) because the edc_spawn case has already
    // consulted the task graph.
    // if( !fr->all_children_done() ) {
    if( fr->get_frame()->get_state() == fs_suspended
	&& !fr->all_children_done() ) {
	if( pending_frame * schildq = sd.steal_rchild( fr, sm_sync ) ) {
	    sd.wakeup_steal( fr, schildq );
	    fr->unlock( &sd );
	    return;
	}
    }

    // The following is a focussed steal, i.e. it is a specialization of the
    // random steal where we focus on the spawn deque that is currently holding
    // the parent frame (note that this may change before we actually get to do
    // the stealing). Focussed stealing is stronger than sibling stealing as it
    // considers both continuing execution of the parent and stealing siblings.
#if !PACT11_VERSION
    if( fr->get_frame()->get_state() == fs_waiting ) {
	// Focussed steal
	spawn_deque * owner = fr->get_frame()->get_owner();
	LOG( id_focussed_steal, owner );
	fr->unlock( &sd );
	full_frame * ff = owner->steal_stack( &sd, sm_focussed );
	if( ff ) {
	    PROFILE(focussed_steals);
	    LOG( id_focussed_steal, ff );
	}
	return;
    }
#endif // !PACT11_VERSION

    fr->unlock( &sd );
    // fr->unlock_pending( &sd );
    random_steal();
}

// Note: for task dependencies:
// At an unconditional steal we choose between generating more tasks
// (continuing the parent at the call) and finishing tasks (continuing
// the children list)
void
worker_state::unconditional_steal( full_frame * fr ) {
    LOG( id_unconditional_steal, fr );
    PROFILE( uncond_steals );

    assert( sd.empty()
	    && "Can only unconditional-steal when extended deque is empty" );
    assert( !fr->get_frame()->get_owner()
	    && "Can only unconditional-steal non-owned frames" );
    assert( fr->get_frame()->get_state() == fs_suspended
	    && "Can only unconditional-steal suspended frames" );

    fr->get_frame()->set_owner( &sd );
    fr->get_frame()->set_state( fs_waiting );
    sd.insert_stack( fr );
    fr->unlock( &sd );
}

void
worker_state::worker_fn() {
    empty_deque_condition_t sjr;

    extern __thread worker_state * tls_worker_state;
    tls_worker_state = this;

    last_case = 0;

    root = sd.youngest(); // for debugging

    if( id == 0 )	  // to support multiple calls to run()
	cresult->reset();

    // If != 0 then we come from a longjmp() and we still need to
    // free the left-over stack. (We first needed to longjmp to the
    // main stack before we can free the stack we were executing on.)
#if PROFILE_WORKER
    pp_time_start( &wprofile.time_longjmp );
#endif
    PROFILE(setjmp);
    sjr = (empty_deque_condition_t)setjmp( jb_ret );
#if PROFILE_WORKER
    pp_time_end( &wprofile.time_longjmp );
#endif
    PROFILE(longjmp);
    LOG( id_setjmp, size_t(sjr) );
/*
    iolock();
    std::cerr << "setjump cond=" << sjr << " frame="
	      << sd.youngest() << "\n";
    iounlock();
*/

    // backoff.reset();

#if PROFILE_WORKER
    if( size_t(sjr) != 0 )
    	pp_time_start( &wprofile.time_since_longjmp );

#define PROFILE_CASE(x) case x: PROFILE(x); break
    switch( sjr ) {
	PROFILE_CASE(edc_call);
	PROFILE_CASE(edc_spawn);
	PROFILE_CASE(edc_sync);
	PROFILE_CASE(edc_bootstrap);
    }
#undef PROFILE_CASE
#endif

    // Note for removing get_parent() calls:
    // We need to lock the spawn_deque when stealing and to keep it locked
    // while converting the oldest frame to a full frame. When returning here,
    // we also need to lock the spawn_deque to guarantee that the conversion
    // has finished. At that point, the spawn_deque can have the oldest stack
    // frame in its "oldest full frame" field.

    // See how we got here
    switch( sjr ) {
    case edc_call:
    {
	full_frame * child = sd.get_popped();
	full_frame * parent = child->get_parent();
	assert( sd.empty()
		&& "Deque must be empty when returning through longjmp()" );
	assert( child->get_frame()->is_call() && "Must be call here" );
	the_task_graph_traits::release_task( child->get_frame() );
	parent->lock( &sd );
	child->lock( &sd );
	stack_frame * child_fr = child->get_frame();
	child->~full_frame(); // full_frame destructor, because not deleted
	delete child_fr;      // de-allocates child also, but no destructor call
	unconditional_steal( parent );
	last_case = 1;
	break;
    }
    case edc_spawn:
    {
	full_frame * child = sd.get_popped();
	full_frame * parent = child->get_parent();
	assert( sd.empty()
		&& "Deque must be empty when returning through longjmp()" );
	assert( child->get_frame()->get_owner() == &sd
		&& "child initiating longjmp to us must belong to our deque" );
#if PROFILE_WORKER
	pp_time_t timer;
	memset( &timer, 0, sizeof(timer) );
	pp_time_start( &timer );
#endif
	pending_frame * next
	    = the_task_graph_traits::release_task_and_get_ready( child );
#if PROFILE_WORKER
	pp_time_end( &timer );
	if( next )
	    pp_time_add( &tls_worker_state->wprofile.time_release_ready_success, &timer );
	else
	    pp_time_add( &tls_worker_state->wprofile.time_release_ready_fail, &timer );
#endif
	parent->lock( &sd );
#if !PACT11_VERSION && 0
	parent->rchild_steal_attempt( next != 0, sm_release );
#endif
	child->lock( &sd );
	stack_frame * child_fr = child->get_frame();
	child->~full_frame(); // full_frame destructor, because not deleted
	delete child_fr;      // de-allocates child also, but no destructor call
	if( next ) {
	    sd.wakeup_steal( parent, next );
	    parent->unlock( &sd );
	} else {
	    provably_good_steal( parent );
	}
	last_case = 2;
	break;
    }
    case edc_sync:
    {
	stack_frame * fr = sd.youngest();
	assert( fr->get_owner() == &sd );
	if( !sd.only_has( fr ) )
	    sd.convert_and_pop_all();
	assert( sd.only_has( fr )
		&& "Execute sync only if 1! full frame on deque" );
	fr->get_full()->lock( &sd );
	sd.pop_sync();
	fr->set_state( fs_suspended );
	fr->set_owner( 0 ); // suspended
	provably_good_steal( fr->get_full() ); // fr->get_first_child() ??
	last_case = 3;
	break;
    }
    case edc_bootstrap: // value 0 - only first time
    {
	assert( !sd.youngest() || sd.youngest()->get_full()->test_lock( 0 ) );
	last_case = 4;
	break;
    }
    }

    // errs() << "\n";

    if( sd.empty() ) {
#if PROFILE_WORKER
	bool profile_now = last_case != 4;
	if( profile_now )
	    pp_time_start( &wprofile.time_clueless );
#endif

	do {
	    // First check if my_main() has finished. If so, exit
	    if( likely(cresult != 0) && cresult->is_finished() ) {
		// errs() << "worker " << id << " is finishing...\n";
		assert( sd.empty() );
		// fprintf( stderr, "worker %ld sees finish\n", id );
		return;
	    }
	    // Attempt randomized work stealing
	    PROFILE(steal_attempt);
	    sched_yield();
	    random_steal();
	    last_case = 5;
	    // This has drawbacks related to the point where we start
	    // to see linear scaling.
	    // if( sd.empty() ) usleep( 1 ); // 1 us back-off
	} while( sd.empty() );

#if PROFILE_WORKER
	if( profile_now )
	    pp_time_end( &wprofile.time_clueless );
#endif
    }

    // Youngest can never be stolen, so we don't need a lock on
    // our spawn deque.
    stack_frame * frame = sd.youngest();
    assert( frame->get_owner() == &sd );
    assert( ( !dummy || frame->get_full() != dummy )
	    && "Trying to resume dummy frame" );
    main_sp = get_sp();
    main_sp -= 128; // Jump past next call's stack frame ...
    PROFILE(resume);
    frame->resume(); // Should never return
    assert( 0 && "stack_frame()::resume should never return" );
    last_case = 6;

    // resume should longjmp() back to the setjmp()
    abort();
}

void *
worker_state::initiator( void * data ) {
    // Note: the main thread's worker_fn() is not called by this function.
    worker_state * ws = reinterpret_cast<worker_state *>( data );
    ws->cpubind();

    // Decrement barrier count - we're alive
    extern volatile long ini_barrier;
    __sync_fetch_and_add( &ini_barrier, -1 );

    extern __thread logger * tls_thread_logger;
    extern logger * thread_logger;
    tls_thread_logger = &thread_logger[ws->id];

    extern __thread size_t threadid;
    threadid = ws->id;

    do {
	ws->worker_fn();
    } while( !ws->do_shutdown() );
    return NULL;
}

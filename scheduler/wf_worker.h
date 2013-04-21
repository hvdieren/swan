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
#ifndef WF_WORKER_H
#define WF_WORKER_H

#include "swan_config.h"

#include <unistd.h>
#include <csetjmp>

#ifdef HAVE_LIBHWLOC
#include <hwloc.h>
#endif

#include "object.h"
#include "wf_spawn_deque.h"
#include "wf_stack_frame.h"
#include "alc_allocator.h"
#include "alc_mmappol.h"
#include "alc_flpol.h"

#if PROFILE_WORKER || TIME_STEALING || PROFILE_QUEUE
#include "swan/../util/pp_time.h"
#endif

class stack_frame;
class future;

template<size_t MinSleep, size_t MulT, size_t MaxSleep>
class exp_backoff {
    size_t delay;
public:
    exp_backoff() : delay( 0 ) { }

    void update( bool success ) {
	if( success )
	    delay = 0;
	else {
	    if( delay > 0 ) {
		if( delay < MaxSleep ) {
		    delay = (float(delay) * float(MulT))/1000.0;
		    if( delay > MaxSleep )
			delay = MaxSleep;
		}
	    } else
		delay = MinSleep;
	}
    }

    bool maybe_delay() const {
	if( delay > 0 ) {
	    usleep( delay );
	    return true;
	} else
	    return false;
    }

    void reset() { } // delay = 0; }
};

namespace obj {
    class profile_queue;
}

class worker_state {
    // typedef exp_backoff<10,1100,1000000> backoff_t;
public:
    // Size of stack_frame must be power of 2 to facilitate aligned
    // memory allocation
    // static_assert( (sizeof(stack_frame)&(sizeof(stack_frame)-1)) == 0,
		   // "sizeof(stack_frame) must be power of 2" );
    typedef alc::mmap_alloc_policy<pending_frame, sizeof(pending_frame)>
    pmmap_align_pol;
    typedef alc::freelist_alloc_policy<pending_frame, pmmap_align_pol,
				       64> pflist_align_pol;
    typedef alc::allocator<pending_frame, pflist_align_pol> pf_alloc_type;

    typedef alc::mmap_alloc_policy<stack_frame,
				   stack_frame::Align> mmap_align_pol;
    typedef alc::freelist_alloc_policy<stack_frame, mmap_align_pol,
				       32> flist_align_pol;
    typedef alc::allocator<stack_frame, flist_align_pol> sf_alloc_type;

private:
    spawn_deque __cache_aligned sd;
    jmp_buf jb_ret;
    size_t id;
    size_t nthreads;
    future * cresult;
    sf_alloc_type sf_allocator;
    pf_alloc_type pf_allocator;
    intptr_t main_sp;
    bool shutdown_flag;
    int last_case; // debugging

    stack_frame * root; // debugging
    full_frame * dummy;

#ifdef HAVE_LIBHWLOC
    hwloc_topology_t topology;
#endif
    size_t my_cpu;
    size_t my_mem;

    // backoff_t backoff;

#if PROFILE_WORKER
public:
    struct profile_worker {
	pp_time_t time_since_longjmp;
	pp_time_t time_longjmp;
	pp_time_t time_clueless;
	pp_time_t time_release_ready;

#if PROFILE_QUEUE
	obj::profile_queue queue;
#endif // PROFILE_QUEUE

	size_t num_invoke;
	size_t num_waiting;
	size_t num_pending;
	size_t num_ssync;
	size_t num_split_ret;
	size_t num_split_body;

	size_t num_resume;
	size_t num_steal_attempt;
	size_t num_steal_delay;
	size_t num_setjmp;
	size_t num_longjmp;
	size_t num_edc_call;
	size_t num_edc_spawn;
	size_t num_edc_sync;
	size_t num_edc_bootstrap;
	size_t num_uncond_steals;
	size_t num_provgd_steals;
	size_t num_provgd_steals_fr;
	size_t num_random_steals;
	size_t num_focussed_steals;

	profile_worker();
	~profile_worker();
	void summarize( const profile_worker & w );
	void dump_profile( size_t id ) const;
    };

private:
    profile_worker wprofile;
#endif

public:
    worker_state();
    ~worker_state();

#if PROFILE_WORKER
    profile_worker & get_profile_worker() { return wprofile; }
#endif

    // An accessor function to the TLS worker_state variable to avoid
    // GCC reusing addresses of TLS variables when function bodies move
    // between threads. Especially fishy when lots of inlining occurs.
    // This is a gcc 4.6 issues, but not gcc 4.5.
    static worker_state * get_thread_worker_state() __attribute__((noinline));
    // Short-hand
    static worker_state * tls() { return get_thread_worker_state(); }

    void initialize( size_t id_, size_t nthreads_,
#ifdef HAVE_LIBHWLOC
		     hwloc_topology_t topology_,
#endif
		     size_t cpu_,
		     size_t mem_, future * cresult_ );

    const spawn_deque * get_deque() const { return &sd; }
    intptr_t get_main_sp() const { return main_sp; }

    static void longjmp( int retval ) __attribute__((noreturn));

    void worker_fn( void );

private:
    void random_steal( void );
    void provably_good_steal( full_frame * fr );
    void unconditional_steal( full_frame * fr );

public:
    static void * initiator( void * );
    static inline sf_alloc_type & get_sf_allocator();
    static inline pf_alloc_type & get_pf_allocator();

    full_frame * get_dummy() const { return dummy; }
    future * get_future() const { return cresult; }
    void notify( future * cresult_ );

    void shutdown() { shutdown_flag = true; }
    bool do_shutdown() const volatile { return shutdown_flag; }

    void cpubind() const;

private:
    // Constants provided by
    // http://en.wikipedia.org/wiki/Linear_congruential_generator,
    // MMIX/Knuth version
#ifdef __x86_64__
    uint64_t rand64_x;
    uint64_t lf_rand() {
	static const uint64_t a = 6364136223846793005ULL;
	static const uint64_t c = 1442695040888963407ULL;
	return rand64_x = (a * rand64_x + c); // (mod 2**64)
    }
#else
    uint32_t rand32_x;
    uint32_t lf_rand() {
	static const uint32_t a = 1103515245;
	static const uint32_t c = 12345;
	return rand32_x = (a * rand32_x + c) & 0x3fffffffU;
    }
#endif
} __cache_aligned;

worker_state::sf_alloc_type &
worker_state::get_sf_allocator() {
    return tls()->sf_allocator;
} 

worker_state::pf_alloc_type &
worker_state::get_pf_allocator() {
    return tls()->pf_allocator;
} 


#endif // WF_WORKER_H

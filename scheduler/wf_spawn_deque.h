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
#ifndef SPAWN_DEQUE_H
#define SPAWN_DEQUE_H

#include "swan_config.h"

#include <cassert>

#include "wf_stack_frame.h"
#include "logger.h"
#include "lock.h"

#if TIME_STEALING
#include "swan/../util/pp_time.h"
#endif

class spawn_deque;

struct call_stack {
    stack_frame * head;
    stack_frame * tail;

    call_stack() : head( 0 ), tail( 0 ) { }
    call_stack( stack_frame * head_, stack_frame * tail_ )
	: head( head_ ), tail( tail_ ) { }

    bool empty() const volatile { return !head; }
};

// This class implements a similar interface to STL deque, but hopefully
// more efficient.
class spawn_deque_store {
    typedef aligned_class<mcs_mutex, CACHE_ALIGNMENT/2> sds_mutex;

    static const size_t Chunk = 32;
    call_stack * deque;
    size_t alloc;
    volatile long tail; // for good performance, tail and head must be in
    intptr_t pad0[5];
    volatile long head; // a different cache block
    intptr_t pad1[7];
    sds_mutex __cache_aligned L;
    sds_mutex::node __cache_aligned L_self;

public:
    spawn_deque_store()
	: deque( 0 ), alloc( 0 ), tail( 0 ), head( 0 ) { grow(); }
    ~spawn_deque_store() {
	if( deque )
	    free( (void *)deque );
    }

    void push_back( call_stack & cs ) {
	if( tail >= (long)alloc )
	    grow();
	// assert( empty() || deque[tail-1] != fr );
	LOG( id_push_back, cs.head );
	deque[tail] = cs;
	__vasm__( "mfence" : : : "memory" );
	tail++;
    }

    call_stack pop_back() {
	--tail;
	__vasm__( "mfence" : : : "memory" );
	if( head > tail ) {
	    ++tail;
	    lock_self();
	    --tail;
	    if( head > tail ) {
		++tail;
		unlock_self();
		LOG( id_pop_back, 0 );
		return call_stack();
	    }
	    unlock_self();
	}
	LOG( id_pop_back, deque[tail].head );
	return deque[tail];
    }

    static bool detach( stack_frame * s, stack_frame * t,
			spawn_deque * tgt );

    full_frame * pop_front( spawn_deque * tgt, stack_frame *** new_top ); // steal
    stack_frame * front() const { return head < tail ? deque[head].head : 0; }

    bool empty() const volatile { return tail <= head; }

    void reset() {
	// lock_self();
	assert( empty() && "Reset is meant only for empty deques" );
	head = tail = 0;
	// unlock_self();
    }

    void flush() { head = tail = 0; }

    // Debugging
    size_t get_head() const { return head; }

    void lock( sds_mutex::node * n ) { L.lock( n ); }
    bool try_lock( sds_mutex::node * n ) { return L.try_lock( n ); }
    void unlock( sds_mutex::node * n ) { L.unlock( n ); }

    void lock_self() { L.lock( &L_self ); }
    void unlock_self() { L.unlock( &L_self ); }

private:
    void grow() {
	// Growing memory needs a lock because it may move the array to
	// a different location and we do not want to do that while thiefs
	// are reading it. Taking the lock removes that possibility because
	// grow() is called only by the victim.
	lock_self();
	alloc += Chunk;
	call_stack * ndeque
	    = (call_stack *)realloc( deque,
				     sizeof(*ndeque)*(alloc+Chunk) );
	assert( ndeque && "Could not realloc deque" );
	deque = ndeque;
	unlock_self();
    }
};

// This class implements the extended spawn deque in Cilk terminology
class spawn_deque {
#if FF_MCS_MUTEX
    static const size_t ff_tag_log = 2;
    static const size_t ff_tag_num = 1<<ff_tag_log;
    static const size_t ff_tag_mask = ff_tag_num-size_t(1);
    full_frame::mutex_tag ff_tag[ff_tag_num];
    full_frame::mutex_tag steal_node;
    mutable size_t ff_tag_cur;
#endif
    spawn_deque_store deque;
    call_stack current;
    full_frame * top_parent;
    full_frame * popped;
    bool top_parent_maybe_suspended;

#if PROFILE_SPAWN_DEQUE
    size_t num_pop_call_nfull;
    size_t num_pop_success;
    size_t num_pop_success_lock;
    size_t num_pop_fail;
    size_t num_steal_top;
    size_t num_steal_top_sibling;
    size_t num_steal_fail;
    size_t num_steal_fail_sibling;
    size_t num_rsib_steals;
    size_t num_rsib_steals_success;
    size_t num_cvt_pending;

    void dump_profile() const;

#define SD_PROFILE(x)  ++(num_##x)
#define SD_PROFILE_ON(sd,x)  ++(sd->num_##x)
#else
#define SD_PROFILE(x)
#define SD_PROFILE_ON(sd,x)
#endif

#if TIME_STEALING
    pp_time_t steal_acquire_time;
    pp_time_t steal_time_pop;
    pp_time_t steal_time_deque;
    pp_time_t steal_time_sibling;
    pp_time_t try_pop_lock_time;
    pp_time_t time_rchild_steal;
    pp_time_t time_rchild_steal_fail;
#endif

public:
    spawn_deque();
    ~spawn_deque();

    bool empty() const { return current.empty() && deque.empty(); }
    bool stealable() const { return !deque.empty() || top_parent_maybe_suspended; }
    inline void push_spawn( stack_frame * fr );
    inline void pop_sync();
    inline void push_call( stack_frame * fr );
    inline void pop_call();
    inline void push( full_frame * fr );
    inline void push( stack_frame * fr );
public:
    inline bool try_pop();

#if FF_MCS_MUTEX
    full_frame::mutex_tag * get_ff_tag() const {
	size_t idx = ff_tag_cur++;
	return const_cast<full_frame::mutex_tag *>( &ff_tag[idx&ff_tag_mask] );
    }
#endif

    stack_frame * youngest() const {
	return current.head;
    }

    bool only_has( stack_frame * fr ) const {
	return current.head == fr && current.tail == fr
	    && deque.empty() && fr->is_full();
    }

    full_frame * steal_stack( spawn_deque * tgt, steal_method_t sm );
    void wakeup_steal( full_frame * parent, pending_frame * qf );
    pending_frame * steal_rchild( full_frame * fr, steal_method_t sm );

    void convert_and_pop_all();

    void insert_stack( full_frame * full ) {
#ifndef NDEBUG
	stack_frame * stack = full->get_frame();
#endif
	assert( empty() && "Stealing with a non-empty extended deque?" );
	assert( stack->get_owner() == this && "Must own this to insert it" );
	assert( full->test_lock( this ) && "Must be locked to insert it" );
	assert( stack->get_state() == fs_waiting && "Must be fs_waiting" );
	push( full );
    }

    // Debugging
    size_t get_head() const { return deque.get_head(); }

    void dump() const __attribute__((noinline));

    // friend std::ostream & operator << ( std::ostream & os, const spawn_deque & sd );

    full_frame * get_popped() {
	full_frame * p = popped;
	popped = 0;
	return p;
    }
};

void
spawn_deque::push_spawn( stack_frame * fr ) {
    assert( current.head != fr );
    if( !current.empty() )
	deque.push_back( current );
    current.head = current.tail = fr;
}

void
spawn_deque::pop_sync() {
    assert( current.head && "Popping call stack from empty extended deque" );
    assert( current.head->is_full() && "Syncing frame must be full" );
    current.head = current.tail = 0;
    deque.lock_self();
    top_parent = 0;
    top_parent_maybe_suspended = false;
    deque.unlock_self();
}


void
spawn_deque::push_call( stack_frame * fr ) {
    assert( fr->get_parent() == current.head
	    && "Pushing non-child on current call stack" );
    assert( fr->is_call() );
    current.head = fr;
}

// We can get rid of the get_parent() call in pop_call() as follows:
// Pass current as an argument to try_pop() (we know it when we call)
// and do not store current in the extended spawn_deque - there is
// no need to have it (but it does make debugging easier).
void
spawn_deque::pop_call() {
    assert( current.head && "Popping frame from empty current stack" );
    assert( current.head->is_call() );
    assert( !current.head->is_full() );
    current.head = current.head->get_parent();
    current.head->set_state( fs_executing );
    assert( current.head->get_owner() == this );
}

void spawn_deque::push( full_frame * fr ) {
    assert( empty() && "deque must be empty when pushing full frame" );
    current.head = current.tail = fr->get_frame();
    top_parent = fr->get_parent();
    top_parent_maybe_suspended
	= ( top_parent->get_frame()->get_state() == fs_suspended );
}

void spawn_deque::push( stack_frame * fr ) {
    if( !fr->is_call() && current.head ) {
	deque.push_back( current );
	current.tail = fr;
    }
    current.head = fr;
}

bool spawn_deque::try_pop() {
    call_stack old_current = current;
    assert( !popped );
    // Note: if-statement clause ordering optimized
    if( current.head->is_call() && !current.head->is_full() ) {
	SD_PROFILE(pop_call_nfull);
	pop_call();
	return true;
    }
    current = deque.pop_back();
    if( current.head ) {
	SD_PROFILE(pop_success);
	// Move this code into the if( current ) case because the else case
	// is covered by the lock/unlock sequence in deque.reset().
	if( deque.empty() && current.head == current.tail ) {
	    SD_PROFILE(pop_success_lock);
	    // We could scan from current (if !0) until top (oops, race) and
	    // check if last frame is full. If so, then we don't need to go
	    // through this sequence. We could also scan until frame at
	    // previous tail index instead of top, because that is safe.
	    // OR we simply check difference between head and tail to see
	    // that there is at least one other stack that is not being stolen.
	    // If so, we are not yet hindered by the conversion to full.
#if TIME_STEALING
	    pp_time_start( &try_pop_lock_time ); // Measures blocked time
#endif
	    deque.lock_self(); // Wait until the steal is done such that we are sure
	    deque.unlock_self(); // that the last frame on the current stack is full.
#if TIME_STEALING
	    pp_time_end( &try_pop_lock_time ); // Measures blocked time
#endif
	}

	current.head->set_state( fs_executing );
	assert( current.head->get_owner() == this );
	return true;
    } else {
	SD_PROFILE(pop_fail);
	// after unlock(), we know that the steal has finished
	// so old_current is guaranteed to be full
#if TIME_STEALING
	pp_time_start( &try_pop_lock_time ); // Measures blocked time
#endif
	deque.lock_self();
	deque.reset();
	top_parent = 0;
	top_parent_maybe_suspended = false;
	deque.unlock_self();
#if TIME_STEALING
	pp_time_end( &try_pop_lock_time ); // Measures blocked time
#endif
	assert( old_current.head->is_full() && "Last frame on deque must be full" );
	popped = old_current.head->get_full();
	return false;
    }
}

// Circular dependencies between header files...
#if FF_MCS_MUTEX
full_frame::sf_mutex::node_ *
full_frame::sf_mutex::magically_get_node( const spawn_deque * D ) {
    // return (long)D < 0x10 ? &N_one : D->get_ff_tag();
    return D->get_ff_tag();
}
#endif


#endif // SPAWN_DEQUE_H

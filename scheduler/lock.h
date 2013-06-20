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
#ifndef LOCK_H
#define LOCK_H

#include "swan_config.h"

// Prefer to use std::mutex, if available.
#include <pthread.h>

#include <cassert>

#include "debug.h"
#include "platform.h"
#include "padding.h"

class mutex {
    pthread_mutex_t m;
public:
    mutex() { pthread_mutex_init( &m, NULL ); }
    ~mutex() { pthread_mutex_destroy( &m ); }

    void lock() { pthread_mutex_lock( &m ); }
    void unlock() { pthread_mutex_unlock( &m ); }
    bool try_lock() { return pthread_mutex_trylock( &m ) == 0; }
};

template<typename T>
class cas_mutex_v {
    typedef T value_type;

public:
    // m is a 1-bit locked/unlocked field and a (eg) 63-bit counter to
    // check for ABA races.
    volatile value_type m;
public:
    cas_mutex_v() : m( 0 ) {
    	assert( (((intptr_t)&m) & (sizeof(value_type)-1)) == 0
		&& "cas_mutex_v alignment violated" );
    }

    void lock() {
	// errs() << "CAS intend lock " << this << '\n';
	while( 1 ) {
	    value_type old = m;
	    while( old & 1 ) { // busy wait if locked
		old = *(volatile value_type *)&m;
	    }
	    assert( ((old+3)&1) );
	    // set locked bit, add 1 to counter.
	    if( __sync_bool_compare_and_swap( &m, old, old+3 ) )
		break;
	}
	assert( (m&1) && "Mutex lock failed" );
	// errs() << "CAS lock " << this << '\n';
    }
    bool try_lock() {
	value_type old = m;
	if( old & 1 )
	    return false;
	// set locked bit, add 1 to counter.
	if( !__sync_bool_compare_and_swap( &m, old, old+3 ) )
	    return false;
	// errs() << "CAS lock " << this << '\n';
	return true;
    }
    void unlock() {
	value_type old = m;
	assert( (old & 1) && "Mutex must be locked when unlocking it!" );
	if( !__sync_bool_compare_and_swap( &m, old, old+1 ) ) {
	    assert( 0 && "Cannot unlock mutex" );
	}
	// errs() << "CAS unlock " << this << '\n';
    }

    bool test_lock() const { return m & 1; }
};

typedef cas_mutex_v<long> cas_mutex;

class mcs_mutex {
public:
    class node {
	static const intptr_t NodeAlignment = CACHE_ALIGNMENT/2;
	friend class mcs_mutex; // only we access this stuff

	volatile node * next;
#if DBG_MCS_MUTEX || 1
	mcs_mutex * inuse;
#endif
	volatile bool blocked;

	char pad1[NodeAlignment-sizeof(node*volatile)
		  -sizeof(volatile bool)
#if DBG_MCS_MUTEX || 1
		  -sizeof(mcs_mutex*)
#endif
	    ];

    public:
	node()
#if DBG_MCS_MUTEX
	    : inuse( 0 )
#endif
	    {
		// assert( (intptr_t(this) & (NodeAlignment-1)) == 0
		        // && "Alignment of mcs_mutex::node not respected" );
		assert( (intptr_t(&next) & (sizeof(next)-1)) == 0
		        && "Alignment of mcs_mutex::node::next not respected" );
	    }

	void unuse() {
#if DBG_MCS_MUTEX
	    inuse = 0;
#endif
	}

    private:
	void validate_padding() {
	    static_assert( sizeof(node) == NodeAlignment,
			   "Incorrect padding calculated" );
	}
    };
    
private:
    volatile node * L;

public:
    mcs_mutex() : L( 0 ) {
	assert( (intptr_t(&L) & (sizeof(L)-1)) == 0
		&& "Alignment of mcs_mutex::L not respected" );
    }

    void lock( volatile node * I ) volatile {
	volatile node * pred;

#if DBG_MCS_MUTEX 
	assert( !I->inuse );
#endif
#if DBG_MCS_MUTEX || 1
	// This statement improves performance due to the
	// assembly code sequence generated!
	I->inuse = (mcs_mutex *)this;
#endif

	I->next = 0;

	__vasm__("lock; \n\t"
		 "xchgq %0,%1 \n\t"
		 : "=r" (pred), "=m"(L)
		 : "0" (I), "m" (L)
		 : "memory" );

	if( pred ) {
	    I->blocked = true;
	    pred->next = I;
	    // __vasm__( "mfence\n\t" : : : "memory" );
	    while( I->blocked ); // spin
	} else
	    I->blocked = false;
	assert( L && "acquiring lock failed" );
    }
    void unlock( volatile node * I ) volatile {
#if DBG_MCS_MUTEX
	assert( I->inuse == this );
#endif
	assert( L != 0 && "It's locked!" );
	if( !I->next ) {
	    if( __sync_bool_compare_and_swap( &L, I, 0 ) ) {
#if DBG_MCS_MUTEX
		I->inuse = 0;
#endif
		return;
	    }
	    // __vasm__( "mfence\n\t" : : : "memory" );
	    while( !I->next ); // spin
	}
	assert( L != I && "I am not supposed to be the last in chain" );
	I->next->blocked = false;
#if DBG_MCS_MUTEX
	I->inuse = 0;
#endif
    }
    bool try_lock( volatile node * I ) {
#if DBG_MCS_MUTEX
	assert( !I->inuse );
#endif
	I->next = 0;
	volatile node * pred = L;
	if( pred )
	    return false;
	if( !__sync_bool_compare_and_swap( &L, pred, I ) )
	    return false;
#if DBG_MCS_MUTEX
	I->inuse = this;
#endif
	assert( L && "acquiring lock failed" );
	return true;
    }
};

// ---------------------------------------------------------------------
// What follows is a way to unify access to different types of
// locks (defined above), in such a way that it becomes easier to
// turn on/off different properties of the locks.
// ---------------------------------------------------------------------
template<typename T>
struct is_mutex_with_node : public std::false_type { };

template<>
struct is_mutex_with_node<mcs_mutex> : public std::true_type { };

template<typename Holder>
class holder_traits {
    Holder * H;
public:
    holder_traits() : H( 0 ) { }

    void set_holder( Holder * H_ ) {
	assert( H == 0 && "holder is not nil when acquiring a lock" );
	H = H_;
    }
    void clear_holder( Holder * H_ ) {
	assert( H == H_ && "holder_traits: holder differs on unlock" );
	H = 0;
    }
    Holder * get_holder() const { return H; }
    bool test_lock( Holder * H_ ) const { return H == H_; }
    bool test_lock() const { return H != 0; }
};

template<typename Holder>
class noholder_traits {
public:
    noholder_traits() { }

    void set_holder( Holder * H_ ) { }
    void clear_holder( Holder * H_ ) { }
    Holder * get_holder() const { return 0; }
    bool test_lock( Holder * H_ ) const { return true; } // We don't know
    bool test_lock() const { return true; } // We don't know
};

template<typename Holder, bool value>
class select_holder_traits
    : public noholder_traits<Holder> {
};

template<typename Holder>
class select_holder_traits<Holder,true>
    : public holder_traits<Holder> {
};

template<typename Mutex, typename Holder, typename HolderTraits>
class holder_mutex_without_node {
    Mutex M;
    HolderTraits H;
public:
    holder_mutex_without_node() { }

    void lock( Holder * H_ ) {
	M.lock();
	H.set_holder( H_ );
    }
    bool try_lock( Holder * H_ ) {
	if( M.try_lock() ) {
	    H.set_holder( H_ );
	    return true;
	} else
	    return false;
    }
    void unlock( Holder * H_ ) {
	H.clear_holder( H_ );
	M.unlock();
    }

    Holder * get_holder() const { return H.get_holder(); }
    bool test_lock( Holder * H_ ) const { return H.test_lock( H_ ); }
    bool test_lock() const { return H.test_lock(); }
};

template<typename Mutex, typename Holder, typename HolderTraits>
class holder_mutex_with_node {
    Mutex M;
    HolderTraits H;
public:
    holder_mutex_with_node() { }

    void lock( Holder * H_, typename Mutex::node * N_ ) {
	M.lock( N_ );
	H.set_holder( H_ );
    }
    bool try_lock( Holder * H_, typename Mutex::node * N_ ) {
	if( M.try_lock( N_ ) ) {
	    H.set_holder( H_ );
	    return true;
	} else
	    return false;
    }
    void unlock( Holder * H_, typename Mutex::node * N_ ) {
	H.clear_holder( H_ );
	M.unlock( N_ );
    }

    Holder * get_holder() const { return H.get_holder(); }
    bool test_lock( Holder * H_ ) const { return H.test_lock( H_ ); }
    bool test_lock() const { return H.test_lock(); }
};

template<typename Mutex, typename Holder, bool Holding,
	 bool with_node=is_mutex_with_node<Mutex>::value >
class holder_mutex : public holder_mutex_without_node<
    Mutex, Holder, select_holder_traits<Holder, Holding> > { };

template<typename Mutex, typename Holder, bool Holding>
class holder_mutex<Mutex, Holder, Holding, true>
    : public holder_mutex_with_node<
    Mutex, Holder, select_holder_traits<Holder, Holding> > { };

typedef PREFERRED_MUTEX pref_mutex;

#endif // LOCK_H

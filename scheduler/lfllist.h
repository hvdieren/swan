// -*- c++ -*-
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

#ifndef LFLLIST_H
#define LFLLIST_H

#include <iterator>

#include "platform.h"
#include "lock.h"
#include "wf_task.h"
#include "wf_task.h"

// This lock-free linked-list implementation is a simplified implementation
// because insertion occurs only at the head of the list and there can be
// only one thread deleting elements by design of the run-time system.

// T contains a field volatile T * next;
// T contains a function bool is_ready() const;
template<typename T>
class lfllist {
    T * head;
public:
    lfllist() : head( 0 ) {
	assert( (intptr_t(&head) & (sizeof(head)-1)) == 0
		&& "Alignment of head pointer violated" );
    }

    bool empty() const { return head == 0; }

    void prepend( T * elm ) {
	while( 1 ) {
	    T * old_head = head;
	    elm->next = old_head;
	    if( __sync_bool_compare_and_swap( &head, old_head, elm ) )
		break;
	}
    }

    T * get_ready() {
	bool retry = false;
	do {
	    T * p = head, * q = 0;
	    if( !p )
		return 0;
	    if( p->is_ready() ) {
		if( __sync_bool_compare_and_swap( &head, p, p->next ) )
		    return p;
		p = head;
	    } else {
		q = p;
		p = p->next;
	    }
	    for( ; p; q=p, p=p->next ) {
		if( p->is_ready() ) {
		    if( q ) {
			q->next = p->next;
			return p;
		    } else {
			if( __sync_bool_compare_and_swap( &head, p, p->next ) )
			    return p;
			retry = true;
		    }
		}
	    }
	} while( retry );
	return 0;
    }
};

// TODO: split out get_depth() and is_ready() on pending_frame/full_frame type
//       and prev/next accessors on link_metadata type. Define latter in tickets.h
//       and former should remain here, but based on template (e.g. by defining
//       a stack_frame concept so that we can conditionally instantiate it).
//       Also, this would require the dl_list class to use two traits
//       specializations, depending on the desired functionality. Too much work
//       for the same assembly code ;-)
template<typename T>
struct dl_list_traits {
    typedef T ValueType;

    static size_t get_depth( T const * elm );
    static bool is_ready( T const * elm );

    static void set_prev( T * elm, T * prev );
    static T * get_prev( T const * elm );
    static void set_next( T * elm, T * next );
    static T * get_next( T const * elm );

    static ValueType * get_value( T const * elm );
};

template<typename T, typename IT, typename V>
class dl_list_iterator;

template<typename T>
class dl_list {
    typedef dl_list_traits<T> traits;

    T * head;
    T * tail;

public:
    template<typename U, typename IU, typename V>
    friend class dl_list_iterator;

    typedef dl_list_iterator<T, T, typename traits::ValueType> iterator;
    typedef dl_list_iterator<T, T const, typename traits::ValueType> const_iterator;

public:
    dl_list() : head( 0 ), tail( 0 ) { }

    bool empty() const { return head == 0; }

    void dump(size_t d) {
	for( T *p=head; p; p=traits::get_next(p) ) {
	    errs() << "     task " << p
		   << " at depth " << d
		   << " ready? "
		   << ( traits::is_ready(p) ? "yes\n" : "no\n" );
	}
    }

    void clear() { head = tail = 0; }

    void erase( iterator it ) {
	T * elm = it.cur;

	if( head == elm ) {
	    head = traits::get_next( elm );
	    if( head == 0 )
		tail = 0;
	} else if( tail == elm ) {
	    tail = traits::get_prev( tail );
	    traits::set_next( tail, 0 );
	} else {
	    // These checks on the validity of prev and next in elm are here
	    // because we need to support the use of erasing an element from
	    // the list that is not there, eg head and tail are null and we
	    // unlink elm. This is for the single-generation taskgraph
	    // (etaskgraph.h).
	    if( traits::get_prev(elm) )
		traits::set_next( traits::get_prev(elm), traits::get_next(elm) );
	    if( traits::get_next(elm) )
		traits::set_prev( traits::get_next(elm), traits::get_prev(elm) );
	}
    }
    void erase_half( iterator it ) {
	T * elm = it.cur;

	if( head == elm ) {
	    head = traits::get_next( elm );
	    if( head == 0 )
		tail = 0;
	} else if( tail == elm ) {
	    assert( 0 );
	    tail = traits::get_prev( tail );
	    traits::set_next( tail, 0 );
	}
	// don't do anything if it doesn't match the head or tail
    }

    void push( T * elm ) { prepend( elm ); }
    void push_back( T * elm ) { push( elm ); }

    void push_back( const_iterator * elm ) { push_back( elm.cur ); }

    void prepend( T * elm ) { // is an append operation but allow subst of class
	traits::set_next( elm, 0 );
	if( tail ) {
	    assert( head );
	    traits::set_prev( elm, tail );
	    traits::set_next( tail, elm );
	    tail = elm;
	} else {
	    assert( !head );
	    traits::set_prev( elm, 0 );
	    traits::set_next( elm, 0 );
	    head = tail = elm;
	}
    }

    T * get_ready() {
	if( empty() )
	    return 0;

	if( traits::is_ready( head ) ) {
	    T * ret = head;
	    head = traits::get_next( head );
	    if( head == 0 )
		tail = 0;
	    else
		traits::set_prev( head, 0 );
	    return ret;
	} else {
	    for( T *p=traits::get_next(head); p; p=traits::get_next(p) ) {
		if( traits::is_ready( p ) ) {
		    // Simplify control flow using end dummies.
		    traits::set_next( traits::get_prev(p), traits::get_next(p) );
		    if( traits::get_next( p ) == 0 )
			tail = traits::get_prev( p );
		    else
			traits::set_prev( traits::get_next(p), traits::get_prev(p) );
		    return p;
		}
	    }
	    return 0;
	}
    }

    T * front() const { return head; }
    void pop() {
	assert( !empty() && "List may not be empty on a pop" );
	head = traits::get_next( head );
	if( head == 0 )
	    tail = 0;
	else
	    traits::set_prev( head, 0 );
    }

public:
    iterator begin() { return iterator( head ); }
    iterator end() { return iterator( 0 ); }
    const_iterator begin() const { return const_iterator( head ); }
    const_iterator end() const { return const_iterator( 0 ); }
};

template<typename T, typename IT, typename V>
class dl_list_iterator
    : public std::iterator< std::forward_iterator_tag, T > {
    typedef dl_list_traits<T> traits;
    friend class dl_list<T>;

    IT * cur;

public:
    dl_list_iterator( IT * cur_ ) : cur( cur_ ) { }

public:
    bool operator == ( dl_list_iterator<T, IT, V> it ) const {
	return cur == it.cur;
    }
    bool operator != ( dl_list_iterator<T, IT, V> it ) const {
	return ! operator == ( it );
    }
    dl_list_iterator<T, IT, V> & operator ++ () {
	cur = traits::get_next( cur );
	return *this;
    }
    V * operator * () const { return traits::get_value( cur ); }

    operator dl_list_iterator<T, T const, typename traits::ValueType>() const { return *this; }

    T * get_node() const { return const_cast<T *>( cur ); }
};

// ----------------------------------------------------------------------
// Doubly linked list with head node.
// ----------------------------------------------------------------------
template<typename T, typename IT, typename V>
class dl_head_list_iterator;

template<typename T>
class dl_head_list {
    typedef dl_list_traits<T> traits;

    T head;

public:
    template<typename U, typename IU, typename V>
    friend class dl_head_list_iterator;

    typedef dl_head_list_iterator<T, T, typename traits::ValueType> iterator;
    typedef dl_head_list_iterator<T, T const, typename traits::ValueType> const_iterator;

public:
    dl_head_list() {
	traits::set_next( &head, &head );
	traits::set_prev( &head, &head );
    }

    bool empty() const { return traits::get_next( &head ) == &head; }

    void dump(size_t d) {
	for( T *p=traits::get_next(&head); p != &head; p=traits::get_next(p) ) {
	    errs() << "     task " << p
		   << " at depth " << d
		   << " ready? "
		   << ( traits::is_ready(p) ? "yes\n" : "no\n" );
	}
    }

    void clear() {
	traits::set_next( &head, &head );
	traits::set_prev( &head, &head );
    }

    void erase( iterator it ) {
	T * elm = it.cur;
	traits::set_next( traits::get_prev(elm), traits::get_next(elm) );
	traits::set_prev( traits::get_next(elm), traits::get_prev(elm) );
    }

    void push_back( const_iterator * elm ) { push_back( elm.cur ); }

    void push_front( T * elm ) {
	traits::set_prev( traits::get_next( &head ), elm );
	traits::set_next( elm, traits::get_next( &head ) );
	traits::set_prev( elm, &head );
	traits::set_next( &head, elm );
    }

    void push_back( T * elm ) {
	traits::set_next( traits::get_prev( &head ), elm );
	traits::set_prev( elm, traits::get_prev( &head ) );
	traits::set_next( elm, &head );
	traits::set_prev( &head, elm );
    }

    T * front() const { return empty() ? 0 : traits::get_next( &head ); }
    void pop() {
	assert( !empty() && "List may not be empty on a pop" );
	T * elm = traits::get_next( &head );
	traits::set_next( traits::get_prev(elm), traits::get_next(elm) );
	traits::set_prev( traits::get_next(elm), traits::get_prev(elm) );
    }

public:
    iterator begin() { return iterator( traits::get_next( &head ) ); }
    iterator end() { return iterator( &head ); }
    const_iterator begin() const {
	return const_iterator( traits::get_next( &head ) );
    }
    const_iterator end() const { return const_iterator( &head ); }
};

template<typename T, typename IT, typename V>
class dl_head_list_iterator
    : public std::iterator< std::forward_iterator_tag, T > {
    typedef dl_list_traits<T> traits;
    friend class dl_head_list<T>;

    IT * cur;

public:
    dl_head_list_iterator( IT * cur_ ) : cur( cur_ ) { }

public:
    bool operator == ( dl_head_list_iterator<T, IT, V> it ) const {
	return cur == it.cur;
    }
    bool operator != ( dl_head_list_iterator<T, IT, V> it ) const {
	return ! operator == ( it );
    }
    dl_head_list_iterator<T, IT, V> & operator ++ () {
	cur = traits::get_next( cur );
	return *this;
    }
    V * operator * () const { return traits::get_value( cur ); }

    operator dl_head_list_iterator<T, T const, typename traits::ValueType>() const { return *this; }

    T * get_node() const { return const_cast<T *>( cur ); }
};


// ----------------------------------------------------------------------
// Singly-linked list
// ----------------------------------------------------------------------
template<typename T>
struct sl_list_traits {
    typedef T ValueType;

    static void set_next( T * elm, T * next );
    static T * get_next( T const * elm );

    static ValueType * get_value( T const * elm );
};

template<typename T, typename IT, typename V>
class sl_list_iterator;

template<typename T>
class sl_list {
    typedef sl_list_traits<T> traits;

    T * head;
    T * tail;

public:
    template<typename U, typename IU, typename V>
    friend class sl_list_iterator;

    typedef sl_list_iterator<T, T, typename traits::ValueType> iterator;
    typedef sl_list_iterator<T, T const, typename traits::ValueType> const_iterator;

public:
    sl_list() : head( 0 ), tail( 0 ) { }

    bool empty() const { return head == 0; }

    void dump(size_t d) {
	for( T *p=head; p; p=traits::get_next(p) ) {
	    errs() << "     task " << p
		   << " at depth " << d
		   << " ready? "
		   << ( traits::is_ready(p) ? "yes\n" : "no\n" );
	}
    }

    void clear() { head = tail = 0; }

    // Put in for ecltaskgraph.h - special case of splice()
    void fastforward( T * it ) { head = it; }

    void push_back( const_iterator * elm ) { push_back( elm.cur ); }
    void push_back( T * elm ) {
	traits::set_next( elm, 0 );
	if( tail ) {
	    assert( head );
	    traits::set_next( tail, elm );
	    tail = elm;
	} else {
	    assert( !head );
	    traits::set_next( elm, 0 );
	    head = tail = elm;
	}
    }

    T * front() const { return head; }
    T * back() const { return tail; }
    void pop() {
	assert( !empty() && "List may not be empty on a pop" );
	head = traits::get_next( head );
	if( head == 0 )
	    tail = 0;
    }

public:
    iterator begin() { return iterator( head ); }
    iterator end() { return iterator( 0 ); }
    const_iterator begin() const { return const_iterator( head ); }
    const_iterator end() const { return const_iterator( 0 ); }
};

template<typename T, typename IT, typename V>
class sl_list_iterator
    : public std::iterator< std::forward_iterator_tag, T > {
    typedef sl_list_traits<T> traits;
    friend class sl_list<T>;

    IT * cur;

public:
    sl_list_iterator( IT * cur_ ) : cur( cur_ ) { }

public:
    bool operator == ( sl_list_iterator<T, IT, V> it ) const {
	return cur == it.cur;
    }
    bool operator != ( sl_list_iterator<T, IT, V> it ) const {
	return ! operator == ( it );
    }
    sl_list_iterator<T, IT, V> & operator ++ () {
	cur = traits::get_next( cur );
	return *this;
    }
    V * operator * () const { return traits::get_value( cur ); }

    operator sl_list_iterator<T, T const, typename traits::ValueType>() const { return *this; }

    T * get_node() const { return const_cast<T *>( cur ); }
};

// ----------------------------------------------------------------------
// locked_dl_list: a doubly linked list with a lock
// ----------------------------------------------------------------------
template<typename T>
class locked_dl_list {
    dl_list<T> list;
    // cas_mutex mutex;
    mcs_mutex mutex;

public:
    locked_dl_list() { }

    void lock( mcs_mutex::node * node ) { mutex.lock( node ); }
    bool try_lock( mcs_mutex::node * node ) { return mutex.try_lock( node ); }
    void unlock( mcs_mutex::node * node ) { mutex.unlock( node ); }

    bool empty() const { return list.empty(); }

    void dump( size_t d ) { list.dump( d ); }
    void clear() { list.clear(); }

    void push( T * elm ) { list.push( elm ); }
    void push_back( T * elm ) { list.push_back( elm ); }
    void prepend( T * elm ) { list.prepend( elm ); }

    T * get_ready() { return list.get_ready(); }

    T * front() { return list.front(); }

    void swap( locked_dl_list & l ) { std::swap( list, l.list ); }
};

template<typename T>
class hashed_list {
    typedef locked_dl_list<T> list_t;
    static const size_t SIZE = 2048;
    list_t table[SIZE];
    size_t min_occ, max_occ;
    // cas_mutex occ_mutex;
    mcs_mutex occ_mutex;

public:
    hashed_list() : min_occ( 0 ), max_occ( 0 ) { }

    // Possible areas of improvements
    // * lock contention: multiple threads waiting on same list
    //   - delays
    //   - may result in large number of empty list accessess
    // * 2.5 evals/ready task retrieved - why?
    // * less than 1/2 ready tasks retrieved from h0 and h1 - why?
    //   - this may be a side effect of the lock contention
    // Possible solutions to lock contention:
    //   - try_lock and move on if contended
    //   - measure contention and transfer to ready list if contended

#ifdef UBENCH_HOOKS
    void reset() {
	min_occ = 0;
	max_occ = 0;
    }
#endif

    void prepend( T * elm ) { // is an append operation but allow subst of class
	size_t d = dl_list_traits<T>::get_depth( elm );
	size_t h = hash(d);
	list_t & list = table[h];
	mcs_mutex::node node;
	list.lock( &node );
	list.prepend( elm );
	list.unlock( &node );

	update_bounds( h );
	// atomic_min( &min_occ, h );
	// atomic_max( &max_occ, h );
    }

    T * get_ready() { return scan(); }

    T * get_internal_ready() { return scan(); }

    T * get_ready( size_t prev_depth ) {
	if( !prev_depth ) // For some reason, this helps performance...
	    return scan();

	size_t h0 = hash( prev_depth );
	if( h0 >= min_occ && h0 < max_occ ) {
	    if( T * ret = probe( h0 ) ) {
#if PROFILE_WORKER
		extern __thread size_t num_h0_hits;
		num_h0_hits++;
#endif // PROFILE_WORKER
		update_bounds_found( h0 );
#if TRACING
		errs() << "h0: find ready " << ret << " depth " << prev_depth << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	size_t h1 = hash( prev_depth+1 );
	if( h1 >= min_occ && h1 < max_occ ) {
	    if( T * ret = probe( h1 ) ) {
#if PROFILE_WORKER
		extern __thread size_t num_h1_hits;
		num_h1_hits++;
#endif // PROFILE_WORKER
		update_bounds_found( h1 );
#if TRACING
		errs() << "h1: find ready " << ret << " depth "
		       << (prev_depth+1) << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	if( T * ret = scan( h0, h1 ) )
	    return ret;

/*
	errs() << "Dumping task graph at " << this << " [" << min_occ
	       << "," << max_occ << "]\n";
	for( size_t i=0; i < SIZE; ++i )
	    table[i].dump(i);
*/

	return 0;
    }

private:
    static size_t hash( size_t d ) {
	return d % SIZE;
    }

    T * probe( size_t h ) {
	list_t & list = table[h];
	if( list.empty() ) {
#if PROFILE_WORKER
	    extern __thread size_t num_hash_empty;
	    num_hash_empty++;
#endif // PROFILE_WORKER
	    // errs() << "empty hash at " << h << std::endl;
	    return 0;
	}

	mcs_mutex::node node;
	list.lock( &node );
	T * ret = list.get_ready();
	list.unlock( &node );
	return ret;
    }

#if 0
    void update_bounds() {
	volatile size_t pm;
	errs() << " ... update_bounds ...\n";
	while( table[min_occ].empty() && min_occ < max_occ ) {
	    do {
		pm = min_occ;
		if( !table[pm].empty() )
		    break;
	    } while( !__sync_bool_compare_and_swap( &min_occ, pm, pm+1 ) );
	}
	while( table[max_occ].empty() && min_occ < max_occ ) {
	    do {
		pm = max_occ;
		if( pm <= min_occ )
		    break;
		if( !table[pm].empty() )
		    break;
	    } while( !__sync_bool_compare_and_swap( &max_occ, pm, pm-1 ) );
	}
    }
#else
    void update_bounds( size_t h ) {
	mcs_mutex::node node;
	occ_mutex.lock( &node );
	if( min_occ == max_occ ) {
	    min_occ = h;
	    max_occ = h+1;
	} else {
	    if( min_occ > h )
		min_occ = h;
	    if( max_occ < h+1 )
		max_occ = h+1;
	}
	// errs() << "update_bounds/insert " << h << " " << min_occ << "-" << max_occ << std::endl;
	occ_mutex.unlock( &node );
    }
    void update_bounds_found( size_t h ) {
	if( h != min_occ || !table[h].empty() )
	    return;

	mcs_mutex::node node;
	if( occ_mutex.try_lock( &node ) ) {
	    size_t i;
	    for( i=min_occ; i < max_occ; ++i )
		if( !table[i].empty() )
		    break;
	    min_occ = i;
	    occ_mutex.unlock( &node );
	}
    }
    void update_bounds() {
	mcs_mutex::node node;
	if( !occ_mutex.try_lock( &node ) )
	    return;
	if( table[min_occ].empty() ) {
	    size_t i;
	    // not i=min_occ+1 because of race with late lock
	    for( i=min_occ; i < max_occ; ++i )
		if( !table[i].empty() )
		    break;
	    min_occ = i;
	}
	if( max_occ > min_occ && table[max_occ-1].empty() ) {
	    size_t i;
	    // not i=max_occ-1 because of race with late lock
	    for( i=max_occ; i > min_occ; --i )
		if( !table[i-1].empty() )
		    break;
	    max_occ = i;
	}
	occ_mutex.unlock( &node );
	// errs() << "update_bounds " << min_occ << "-" << max_occ << std::endl;
    }
#endif

    T * scan() {
	// errs() << "scan from " << min_occ << " to " << max_occ << "\n";
	for( size_t i=min_occ; i < max_occ; ++i ) {
	    if( T * ret = probe( i ) ) {
		// printf( "A0 %ld-%ld @%ld\n", min_occ, max_occ, i );
		update_bounds();
		// printf( "A1 %ld-%ld @%ld\n", min_occ, max_occ, i );
#if TRACING
		errs() << "scan: find ready " << ret << " depth " << i << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	// if( table[min_occ].empty() )
	    // atomic_max( &min_occ, min_occ+1 );
	// if( table[max_occ].empty() && max_occ > 0 )
	    // atomic_min( &max_occ, max_occ-1 );
	// printf( "A0 %ld-%ld\n", min_occ, max_occ );
	update_bounds();
	// printf( "A1 %ld-%ld\n", min_occ, max_occ );
	return 0;
    }
    T * scan( size_t h0, size_t h1 ) {
	for( size_t i=min_occ; i < max_occ; ++i ) {
	    if( i == h0 || i == h1 )
		continue;
	    if( T * ret = probe( i ) ) {
		// printf( "F%ld %ld-%ld @%ld\n", h0, min_occ, max_occ, i );
#if TRACING
		errs() << "bscan: find ready " << ret << " depth " << i << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	// if( table[min_occ].empty() )
	    // atomic_max( &min_occ, min_occ+1 );
	// if( table[max_occ].empty() && max_occ > 0 )
	    // atomic_min( &max_occ, max_occ-1 );
	// printf( "F0 %ld %ld-%ld\n", h0, min_occ, max_occ );
	update_bounds();
	// printf( "F1 %ld %ld-%ld\n", h0, min_occ, max_occ );
	return 0;
    }

    static void atomic_min( volatile size_t * min, size_t v ) {
	if( v < *min ) {
	    volatile size_t pm = *min;
	    do {
		pm = *min;
		if( v >= pm )
		    break;
	    } while( !__sync_bool_compare_and_swap( min, pm, v ) );
	}
    }
    static void atomic_max( volatile size_t * max, size_t v ) {
	if( v > *max ) {
	    volatile size_t pm = *max;
	    do {
		pm = *max;
		if( v <= pm )
		    break;
	    } while( !__sync_bool_compare_and_swap( max, pm, v ) );
	}
    }
};

// This is a mock-up resizing hash table. Should use something proven such
// as hopscotch hashing perhaps.
template<typename T>
class resizing_hashed_list {
    typedef locked_dl_list<T> list_t;
    size_t size;
    list_t * volatile table;
    size_t min_occ, max_occ;
    mcs_mutex occ_mutex;

public:
    resizing_hashed_list() : size( 2048 ), min_occ( 0 ), max_occ( 0 ) {
	// C++11 syntax for default initialization of array allocations
	table = new list_t[size]();
    }
    ~resizing_hashed_list() {
	delete[] (list_t*)table;
    }

#ifdef UBENCH_HOOKS
    void reset() {
	min_occ = 0;
	max_occ = 0;
    }
#endif

    void prepend( T * elm ) { // is an append operation but allow subst of class
	push( elm );
    }

    void push( T * elm ) {
	size_t d = dl_list_traits<T>::get_depth( elm );
	size_t h = hash(d);
	list_t & list = table[h];
	mcs_mutex::node node;
	list.lock( &node );
	if( T * f = list.front() ) {
	    if( dl_list_traits<T>::get_depth( f ) != d ) {
		list.unlock( &node );
		grow();
		push( elm );
		return;
	    }
	}

	list.prepend( elm );
	list.unlock( &node );

	update_bounds( h );
    }

    T * get_ready() { return scan(); }

    T * get_internal_ready() { return scan(); }

    T * get_ready( size_t prev_depth ) {
	if( !prev_depth ) // For some reason, this helps performance...
	    return scan();

	size_t h0 = hash( prev_depth );
	if( h0 >= min_occ && h0 < max_occ ) {
	    if( T * ret = probe( h0 ) ) {
#if PROFILE_WORKER
		extern __thread size_t num_h0_hits;
		num_h0_hits++;
#endif // PROFILE_WORKER
		update_bounds_found( h0 );
#if TRACING
		errs() << "h0: find ready " << ret << " depth " << prev_depth << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	size_t h1 = hash( prev_depth+1 );
	if( h1 >= min_occ && h1 < max_occ ) {
	    if( T * ret = probe( h1 ) ) {
#if PROFILE_WORKER
		extern __thread size_t num_h1_hits;
		num_h1_hits++;
#endif // PROFILE_WORKER
		update_bounds_found( h1 );
#if TRACING
		errs() << "h1: find ready " << ret << " depth "
		       << (prev_depth+1) << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	if( T * ret = scan( h0, h1 ) )
	    return ret;

/*
	errs() << "Dumping task graph at " << this << " [" << min_occ
	       << "," << max_occ << "]\n";
	for( size_t i=0; i < SIZE; ++i )
	    table[i].dump(i);
*/

	return 0;
    }

private:
    size_t hash( size_t d ) const {
	return d & (size-1);
    }

    T * probe( size_t h ) {
	list_t & list = table[h];
	if( list.empty() ) {
#if PROFILE_WORKER
	    extern __thread size_t num_hash_empty;
	    num_hash_empty++;
#endif // PROFILE_WORKER
	    // errs() << "empty hash at " << h << std::endl;
	    return 0;
	}

	mcs_mutex::node node;
	list.lock( &node );
	T * ret = list.get_ready();
	list.unlock( &node );
	return ret;
    }

    void update_bounds( size_t h ) {
	mcs_mutex::node node;
	occ_mutex.lock( &node );
	if( min_occ == max_occ ) {
	    min_occ = h;
	    max_occ = h+1;
	} else {
	    if( min_occ > h )
		min_occ = h;
	    if( max_occ < h+1 )
		max_occ = h+1;
	}
	// errs() << "update_bounds/insert " << h << " " << min_occ << "-" << max_occ << std::endl;
	occ_mutex.unlock( &node );
    }
    void update_bounds_found( size_t h ) {
	if( h != min_occ || !table[h].empty() )
	    return;

	mcs_mutex::node node;
	if( occ_mutex.try_lock( &node ) ) {
	    size_t i;
	    for( i=min_occ; i < max_occ; ++i )
		if( !table[i].empty() )
		    break;
	    min_occ = i;
	    occ_mutex.unlock( &node );
	}
    }
    void update_bounds() {
	mcs_mutex::node node;
	if( !occ_mutex.try_lock( &node ) )
	    return;
	if( table[min_occ].empty() ) {
	    size_t i;
	    // not i=min_occ+1 because of race with late lock
	    for( i=min_occ; i < max_occ; ++i )
		if( !table[i].empty() )
		    break;
	    min_occ = i;
	}
	if( max_occ > min_occ && table[max_occ-1].empty() ) {
	    size_t i;
	    // not i=max_occ-1 because of race with late lock
	    for( i=max_occ; i > min_occ; --i )
		if( !table[i-1].empty() )
		    break;
	    max_occ = i;
	}
	occ_mutex.unlock( &node );
	// errs() << "update_bounds " << min_occ << "-" << max_occ << std::endl;
    }

    T * scan() {
	// errs() << "scan from " << min_occ << " to " << max_occ << "\n";
	for( size_t i=min_occ; i < max_occ; ++i ) {
	    if( T * ret = probe( i ) ) {
		// printf( "A0 %ld-%ld @%ld\n", min_occ, max_occ, i );
		update_bounds();
		// printf( "A1 %ld-%ld @%ld\n", min_occ, max_occ, i );
#if TRACING
		errs() << "scan: find ready " << ret << " depth " << i << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	update_bounds();
	// printf( "A1 %ld-%ld\n", min_occ, max_occ );
	return 0;
    }
    T * scan( size_t h0, size_t h1 ) {
	for( size_t i=min_occ; i < max_occ; ++i ) {
	    if( i == h0 || i == h1 )
		continue;
	    if( T * ret = probe( i ) ) {
		// printf( "F%ld %ld-%ld @%ld\n", h0, min_occ, max_occ, i );
#if TRACING
		errs() << "bscan: find ready " << ret << " depth " << i << " in " << this << '\n';
#endif
		return ret;
	    }
	}
	update_bounds();
	return 0;
    }

private:
    // There is only one thread that can push tasks into the hash table
    // at any one time, so we don't need a critical section around grow.
    // This is a bottleneck: by the time we have copied all lists over,
    // the old hash will be empty, so task issue will block entirely.
    void grow() {
	size_t new_size = size * 2;
	list_t * new_table = new list_t[new_size]();

	min_occ = max_occ = 0; // Recalculate, avoids access to the table also

	size_t old_size = size;
	list_t * old_table = table;
	// Note: require sequential consistency in these stores!
	// Updating the table first allows a possibly erroneous but safe
	// (as in too small and within range) hash calculation.
	table = new_table;
	__vasm__( "sfence" : : : "memory" ); // memory barrier
	size = new_size;

	for( size_t i=0; i < old_size; ++i ) {
	    list_t & old_l = old_table[i];

	    mcs_mutex::node l_node;
	    old_l.lock( &l_node ); // just in case someone has a ref to the table...
	    if( T * f = old_l.front() ) {
		size_t h = hash( f->get_depth() );
		// Swap list only, not lock
		table[h].swap( old_l );
		update_bounds( h );
	    }
	    old_l.unlock( &l_node );
	}

	// When is it safe to do this? By acquiring the occ_mutex, we are
	// excluding the update_bound procedures. The scan/probe procedures will
	// reload the table pointer for every access due to the volatility.
	// We are assuming that the access is atomic due to alignment.
	delete[] old_table;
    }
};


#endif // LFLLIST_H

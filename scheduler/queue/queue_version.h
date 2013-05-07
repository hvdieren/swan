// -*- c++ -*-

// TODO/NOTES
// + !!! check whether all queue segments are properly deleted.
// + !!! implement pushpopdep
// + !!! factor queue_version in pushpop, pop, push versions?
// + !!! microbenchmark with only a pop task that does an empty() check on the queue
//   this will currently spin forever.
//

#ifndef QUEUE_QUEUE_VERSION_H
#define QUEUE_QUEUE_VERSION_H

#include <sched.h>
#include <iostream>
#include <vector>

#include "swan/queue/segmented_queue.h"

#define DBG_QUEUE_VERSION 0

namespace obj {

enum queue_flags_t { qf_push=1, qf_pop=2, qf_pushpop=3 };

std::ostream & operator << ( std::ostream & os, queue_flags_t f );

template<typename MetaData>
class queue_version
{
public:
    typedef MetaData metadata_t;
    enum qmode_t { qm_pushpop=3, qm_push=1, qm_pop=2, qm_prefix=6, qm_suffix=5 };

private:
    typedef queue_flags_t flags_t;

private:
    metadata_t metadata;
    // queue_version for pop only needs queue, user, right
    // queue_version for push only needs user, children, right
    // queue_t (and potentially pushpopdep) requires all 4
    segmented_queue children, right;
    segmented_queue_push user;
    segmented_queue_pop queue;

    // Link in spawn tree
    queue_version<MetaData> * chead, * ctail;
    queue_version<MetaData> * fleft, * fright;
    queue_version<MetaData> * const parent;
    cas_mutex mutex;
    flags_t flags;

    // Information for constructing queue segments.
    size_t max_size;
    size_t peekoff;

/*
    pad_multiple<CACHE_ALIGNMENT, sizeof(metadata_t)
		 + 3*sizeof(segmented_queue)
		 + 5*sizeof(queue_version<metadata_t>*)
		 + sizeof(cas_mutex)> padding;
*/

    template<typename MD>
    friend std::ostream & operator << ( std::ostream &, queue_version<MD> & );

private:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

public:
    template<typename T>
    class initializer {
	typedef T value_type;
    };

protected:
    // Normal constructor, called from queue_t constructor
    template<typename T>
    queue_version( long max_size_, size_t peekoff_,
		   initializer<T> init )
	: chead( 0 ), ctail( 0 ), fleft( 0 ), fright( 0 ), parent( 0 ),
	  flags( qf_pushpop ), max_size( max_size_ ), peekoff( peekoff_ ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	assert( peekoff < max_size && "Peek should not cross segment boundaries" );

	// Create an initial segment and share it between queue.head and user.tail
	user.push_segment<T>( max_size, peekoff, true );
	queue.take_head( user );
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
public:
    queue_version( queue_version<metadata_t> * qv, qmode_t qmode )
	: chead( 0 ), ctail( 0 ), fright( 0 ), parent( qv ),
	  flags( flags_t( qmode & qv->flags ) ),
	  max_size( qv->max_size ), peekoff( qv->peekoff ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	// Link in frame with siblings and parent
	parent->lock();

	user.take( parent->user );
	if( (flags & qf_push) && user.get_tail() ) {
	    user.get_tail()->set_producing();
	}

	if( flags & qf_pop ) {
	    // Only initialize queue if this is a pop or pushpop dep.
	    // Sometimes pops are created before they can execute. In that case,
	    // we don't know what queue.head should be and we may set a NULL
	    // pointer here.
	    queue.take( parent->queue );
	}

	// Link in chain with siblings
	lock();
	if( parent->ctail ) {
	    parent->ctail->lock();
	    fleft = parent->ctail;
	    fleft->fright = this;
	    parent->ctail = this;
	    fleft->unlock();
	} else {
	    fleft = 0;
	    parent->chead = parent->ctail = this;
	}
	unlock();

#if DBG_QUEUE_VERSION
	errs() << "QV nest constructor for: " << *this << std::endl;
	errs() << "                 parent: " << *parent << std::endl;
#endif

	parent->unlock();
    }

    ~queue_version() {
	// These assertions limit the work to perform in erase():
	// 1. Queue is head-only (tail is always 0), so cannot own queue
	// 2. Children must have been reduced into user and un-owned
	// 3. Right must have been reduced into user, so empty
	assert( !children.get_head()
		&& "QV::children must be un-owned on destruct" );
	assert( !right.get_head() && !right.get_tail()
		&& "QV::right must be empty on destruct" );

	user.erase();

	// Top-level queue_version stores last queue_segment(s) shared between
	// queue and children/user.
	if( !parent ) {
	    assert( queue.get_head() );
	    queue.get_head()->erase_all();
	}
    }

    void reduce_sync() {
	// user <- children REDUCE user
	user.reduce_reverse( children );
    }

    void reduce_hypermaps() {
	// Do conversion of hypermaps when a child finishes:
	//  * merge children - user - right
	//  * move children (where everything is reduced to) to left->right
	//    or parent->children

	// TODO: Need to randomize lock order! (see Cilk++ hyperobjects paper)
	if( parent )
	    parent->lock();
	if( fleft )
	    fleft->lock();
	lock();
	if( fright )
	    fright->lock();

	// Reducing everything into a single queue
	children.reduce( user.reduce( right ) );

	// Move up hierarchy. Get last segment after reduction because our local
	// list may potentially be nil if we selected not to push.
	queue_segment * last_seg = 0;
	if( fleft ) {
	    fleft->right.reduce( children );
	    last_seg = fleft->right.get_tail();
	} else {
	    assert( parent );
	    parent->children.reduce( children );
	    last_seg = parent->children.get_tail();
	}

	// Clear producing flag
	if( (flags & qf_push) && (parent->flags & qf_pushpop) == qf_pushpop
	    && ( !fright || (fright->flags & qf_pop) )
	    ) {
	    if( last_seg )
		last_seg->clr_producing();
	}


	if( flags & qf_pop ) {
	    // Swap queues between finishing child and parent or right sibling
	    queue_version<MetaData> * target = parent;
	    queue_version<MetaData> * rpop = fright;
	    while( rpop ) {
		if( rpop->flags & qf_pop ) {
		    target = rpop;
		    break;
		}
		rpop = rpop->fright;
	    }
	    target->queue.take( queue );
	    assert( target->queue.get_head() && "Queue head must be non-null" );

	}

#if DBG_QUEUE_VERSION
	errs() << "Reducing hypermaps DONE on " << *this << std::endl;
	errs() << "                    parent " << *parent << std::endl;
#endif

	unlink();

	if( parent )
	    parent->unlock();
	if( fleft )
	    fleft->unlock();
	if( fright )
	    fright->unlock();
	unlock();
    }


private:
    void unlink() {
	if( !parent )
	    return;

	if( fleft )
	    fleft->fright = fright;
	else
	    parent->chead = fright;

	if( fright )
	    fright->fleft = fleft;
	else
	    parent->ctail = fleft;
    }

    // The head is pushed up as far as possible, without affecting the order
    // of elements in the queue.
    void push_head( segmented_queue & q ) {
	lock();

	if( ctail ) { // and also chead
	    ctail->lock();
	    ctail->right.reduce_headonly( q );
	    ctail->unlock();
	    unlock();
	} else {
	    bool cont = !children.get_tail();
	    children.reduce_headonly( q );
	    unlock();
	    if( cont )
		push_head_up( children );
	}
    }
    void push_head_up( segmented_queue & q ) {
	// Note: lock parent even though we don't need the parent
	// to avoid deletion of fleft between if( fleft ) and fleft->lock()
	// Note: The parent cannot disappear while the child is executing.
	// errs() << "push_head: " << *this << "\n";
	if( parent )
	    parent->lock();
	if( fleft ) {
	    // We need the lock to avoid the left sibling from terminating
	    // while we update the right "hypermap".
	    fleft->lock();
	    lock();
	    parent->unlock();

	    fleft->right.reduce_headonly( q );

	    fleft->unlock();
	    unlock();
	} else if( parent ) {
	    // We do not need to lock the parent because the parent cannot
	    // terminate while we reduce the hypermap. Are there other reasons
	    // for concurrency issues?
	    lock();

	    bool cont = !parent->children.get_tail();
	    // stack/full distinction?
	    parent->children.reduce_headonly( q );

	    unlock();
            parent->unlock();

	    if( cont )
		parent->push_head_up( parent->children );
	} else { // ! parent
	    // If we do not have a parent, then the push terminates.
	    abort();
	}
    }


public:
    const metadata_t * get_metadata() const { return &metadata; }
    metadata_t * get_metadata() { return &metadata; }

    // For debugging
    const queue_version<metadata_t> * get_parent() const { return parent; }
    queue_version<metadata_t> * get_parent() { return parent; }

public:
    void push_bookkeeping( size_t npush ) {
	user.push_bookkeeping( npush );
    }

    void pop_bookkeeping( size_t npop ) {
	queue.pop_bookkeeping( npop );
    }

    // Only for tasks with pop privileges.
    template<typename T>
    T && pop() {
	assert( queue.get_head() );
	return queue.pop<T>();
    }

    // Only for tasks with pop privileges.
    template<typename T>
    const T & peek( size_t off ) {
	assert( off <= peekoff && "Peek outside requested range" );
	assert( queue.get_head() );
	return queue.peek<T>( off );
    }

    template<typename T>
    write_slice<MetaData,T> get_write_slice( size_t length ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>( std::max(length+peekoff,max_size), peekoff, false );
	    segmented_queue q = user.split();
	    push_head( q );
	}
	write_slice<MetaData,T> slice
	    = user.get_write_slice<MetaData,T>( length+peekoff );
	slice.set_version( this );
	return slice;
    }

    template<typename T>
    read_slice<MetaData,T> get_read_slice_upto( size_t npop_max, size_t npeek ) {
	assert( npeek <= peekoff && "Peek outside requested range" );
	assert( !empty() );
	assert( queue.get_head() );
	read_slice<MetaData,T> slice
	    = queue.get_read_slice_upto<MetaData,T>( npop_max, npeek );
	slice.set_version( this );
	return slice;
    }

    // Potentially differentiate const T & t versus T && t
    template<typename T>
    void push( T && t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>( max_size, peekoff, false );
	    segmented_queue q = user.split();
	    push_head( q );
	}
	user.push<T>( std::move( t ), max_size, peekoff );
    }

    template<typename T>
    void push( const T & t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>( max_size, peekoff, false );
	    segmented_queue q = user.split();
	    push_head( q );
	}
	user.push<T>( t, max_size, peekoff );
    }

    // Only for tasks with pop privileges.
    bool empty() {
	if( !queue.get_head() )
	    return true;
	return queue.empty();
    }
};

template<typename MD>
std::ostream & operator << ( std::ostream & os, queue_version<MD> & v ) {
    os << "queue_version " << &v
       << " queue=" << v.queue
       << " children=" << v.children
       << " user=" << v.user
       << " right=" << v.right
       << " flags=" << v.flags;
    return os;
}

} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

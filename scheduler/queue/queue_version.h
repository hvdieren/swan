// -*- c++ -*-

// TODO/NOTES
// + push - pop - push - pop: advertising
//   the queue from the second push will not send it to the second pop. 
//   Instead, it is sent to the 1st pop/right. The reason is that the first
//   pop is not obliged to consume all elements from the queue, in which case
//   the second pop must see them. So the second pop is serialized wrt the
//   first.
//   CONSEQUENCE: pop has a RIGHT field
// + pop has a USER field, when initializing the hypermaps, it inherits parent's
//   USER
// + !!! check whether all queue segments are properly deleted.
// + !!! implement pushpopdep
// + !!! factor queue_version in pushpop, pop, push versions?
// + !!! microbenchmark with only a pop task that does an empty() check on the queue
//   this will currently spin forever.
// + !!! remove logical_head from queue_version
// + should we have an iterator interface rather than empty/pop?
//
// For prefix dependence types
// + distinguish circular/non-circular usage of queue segment
// + wait on all prior pops
// + starting head is always known when prefixdep is ready (arg_ready in tickets)
// + 
//
// BUG
// + The push/pop/push/pop scenario does not work correctly yet. This would be
//   useful in dedup (e.g.)
// 
// For dedup benchmark:
// + We don't know in advance how many sub-fragments will be identified, so
//   we need to provide an upper bound and send less items + an end of stream
//   token (probably a null pointer). Non-existant entries will not be pushed
//   neither popped, although they will be "accounted for" by the hyperqueue.
// + Moreover, the division in large fragments does not seem appropriate for
//   for the native input. Should we do it differently, eg simply based on data
//   size? Would recursive decomposition be of any help to emulate to some
//   extent the application of fine fragmentation to the whole stream?

#ifndef QUEUE_QUEUE_VERSION_H
#define QUEUE_QUEUE_VERSION_H

#include <sched.h>
#include <iostream>
#include <vector>

#include "swan/queue/segmented_queue.h"

#define DBG_QUEUE_VERSION 1

namespace obj {

template<typename MetaData>
class queue_base;

enum queue_flags_t { qf_push=1, qf_pop=2, qf_pushpop=3 };

inline std::ostream & operator << ( std::ostream & os, queue_flags_t f );

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
    // queue_version for pop only needs queue
    // queue_version for push only needs user, children, right
    // queue_t (and potentially pushpopdep) requires all 4
    segmented_queue children, right;
    segmented_queue_push user;
    segmented_queue_pop queue;

    // New fields
    queue_version<MetaData> * chead, * ctail;
    queue_version<MetaData> * fleft, * fright;
    queue_version<MetaData> * const parent;
    cas_mutex mutex;
    flags_t flags;

    // Index structure: where to find segments at certain logical offsets.
    // TODO: store this in queue_t and pass pointer to it in here, copy on
    // nesting. That will make the storage of the index unique per queue.
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

public:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

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
	  flags( qf_pushpop ),
	  // logical_head( 0 ),
	  max_size( max_size_ ), peekoff( peekoff_ ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	assert( peekoff < max_size && "Peek only across one segment boundary" );

	// Create an initial segment and share it between queue.head and user.tail
	user.push_segment<T>( max_size, peekoff, true );
	queue.take_head( user );

	// errs() << "QV queue_t constructor for: " << this << "\n";
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
    }

    void reduce_sync() {
	// user <- children REDUCE user
	user.reduce_reverse( children );
    }

    // TODO: replace push argument by checking flags
    // Length only required for prefixdep and suffixdep.
    template<typename T>
    void reduce_hypermaps( bool is_stack, size_t length=0 ) {
	// Do conversion of hypermaps when a child finishes:
	//  * merge children - user - right
	//  * check ownership and deallocate if owned and empty.
	//  * move children (where everything is reduced to) to left->right
	//    or parent->children

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
	    if( is_stack )
		parent->user.reduce( parent->children );
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

	// TODO: Need to randomize lock order! (see Cilk++ hyperobjects paper)
	// parent->lock();
	// if( fleft )
	    // fleft->lock();
	// lock();
	// if( fright )
	    // fright->lock();

	if( fleft )
	    fleft->fright = fright;
	else
	    parent->chead = fright;

	if( fright )
	    fright->fleft = fleft;
	else
	    parent->ctail = fleft;

	// parent->unlock();
	// if( fleft )
	    // fleft->unlock();
	// unlock();
	// if( fright )
	    // fright->unlock();
    }

    // The head is pushed up as far as possible, without affecting the order
    // of elements in the queue.
    void push_head( segmented_queue & q ) {
	// Note: lock parent even though we don't need the parent
	// to avoid deletion of fleft between if( fleft ) and fleft->lock()
	// errs() << "push_head: " << *this << "\n";
	// if( parent )
	    // errs() << "   parent: " << *parent << "\n";
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

	    // The parent cannot disappear while the child is executing.
	    // We need to lock the grandparent while pushing because a
	    // consumer may be accessing the segmented_queue as we are
	    // initializing it.
	    // Do we really need this once we have the queue_index?
	    // Yes. It helps to pro-actively link as much of the chain
	    // as possible together without resorting to the index (which
	    // wouldn't happen as all segments would remain "producing").
	    if( cont )
		parent->push_head( parent->children );
	} else { // ! parent
	    // If we do not have a parent, then the push terminates.
	    // Do nothing here.
	    // Evaporate the segment pointer. It is registered in the index
	    // and will be accessed that way. We do this because there may be
	    // multiple "heads" of traces of the queue.
	    assert( children.get_head() );
	    assert( !children.get_tail() );
	    children.set_head( 0 ); // Moved to index
	}
	// if( parent )
	    // errs() << "   result: " << *parent << "\n";
    }


public:
    const metadata_t * get_metadata() const { return &metadata; }
    metadata_t * get_metadata() { return &metadata; }

    // For debugging
    const queue_version<metadata_t> * get_parent() const { return parent; }
    queue_version<metadata_t> * get_parent() { return parent; }

private:
    void ensure_queue_head() {
	if( !queue.get_head() ) {
	    // Find the head of the queue for concurrently popping results.
	    // Simple case: there are no other popdep tasks on the same queue
	    // executing, so the other running queue-dep tasks, if any, have
	    // pushdep usage. For this reason, we do not look at the left
	    // sibling (that is the youngest pushdep task), but we look at the
	    // common parent, where the head segment has been pushed up, if it
	    // has been created already.
	    // More complicated case (current): use the queue_index to figure
	    // out what queue segment starts at the required logical position.
#if PROFILE_QUEUE
	    pp_time_start( &get_profile_queue().qv_qhead );
#endif // PROFILE_QUEUE

	    while( !queue.get_head() ) {
		// If there are no older producing push tasks, then bail out.
		queue_version<MetaData> * qv = this;
		bool all_left = true;
		do {
		    if( qv->fleft ) {
			all_left = false;
			break;
		    }
		    qv = qv->parent;
		} while( !(qv->flags & qf_push) ); // goto just below parent
		if( all_left ) {
		    // errs() << "ensure_queue_head bailout due to lack of left tasks" << std::endl;
		    break;
		}

		sched_yield();
	    }
/*
	    if( queue.get_head() )
		errs() << "ensure_queue_head: QV=" << this << " set qhead=" 
		       << *queue.get_head() << " queue=" << queue << std::endl;
	    else
		errs() << "ensure_queue_head: QV=" << this
		       << " set qhead=null queue=" << queue << std::endl;
*/
#if PROFILE_QUEUE
	    pp_time_end( &get_profile_queue().qv_qhead );
#endif // PROFILE_QUEUE
	}
    }

public:
    void push_bookkeeping( size_t npush ) {
	user.push_bookkeeping( npush );
    }

    void pop_bookkeeping( size_t npop ) {
	// errs() << "QV " << *this << " pop bookkeeping " << npop << "\n";
	queue.pop_bookkeeping( npop, false );
    }

    template<typename T>
    T & pop() {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	ensure_queue_head(); // should be done by empty()
	assert( queue.get_head() );
	T & r = queue.pop<T>();
	// errs() << "pop QV=" << this << " queue="
	       // << queue << " value=" << r << "\n";
	return r;
    }

    template<typename T>
    T & peek( size_t off ) {
	assert( off <= peekoff && "Peek outside requested range" );
	ensure_queue_head(); // should be done by empty()
	assert( queue.get_head() );
	return queue.peek<T>( off );
    }

    template<typename T>
    write_slice<MetaData,T> get_write_slice( size_t length ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>(
		std::max(length+peekoff,max_size),
		peekoff, false );

	    segmented_queue q = user.split();
	    push_head( q );
	}
	write_slice<MetaData,T> slice
	    = user.get_write_slice<MetaData,T>( length+peekoff );
	slice.set_version( this );
	return slice;
    }

    template<typename T>
    read_slice<MetaData,T> get_slice_upto( size_t npop_max, size_t npeek ) {
	assert( npeek <= peekoff && "Peek outside requested range" );

	// empty() involves count == 0 check.
	assert( !empty() );
	ensure_queue_head(); // should be done by empty()
	assert( queue.get_head() );
	read_slice<MetaData,T> slice
	    = queue.get_slice_upto<MetaData,T>( npop_max, npeek );
	slice.set_version( this );
	return slice;
    }
    template<typename T>
    read_slice<MetaData,T> get_slice( size_t npop, size_t npeek ) {
	abort(); // deprecated
	assert( npeek-npop <= peekoff && "Peek outside requested range" );
	assert( !empty() );
	ensure_queue_head();
	read_slice<MetaData,T> slice
	    = queue.get_slice<MetaData,T>( npop, npeek );
	slice.set_version( this );
	return slice;
    }

    template<typename T>
    const T & pop_fixed( const T & dflt ) {
	// errs() << "pop QV=" << this << " queue=" << queue << "\n";

	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	ensure_queue_head(); // should be done by empty()
	assert( queue.get_head() );
	if( queue.empty() )
	    return dflt;
	else
	    return queue.pop<T>();
	// errs() << "prefix pop " << this << " count=" << count << "\n";
    }

    // Potentially differentiate const T & t versus T && t
    template<typename T>
    void push( const T & t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>( max_size, peekoff, false );

	    segmented_queue q = user.split();
	    push_head( q );
	}
	// errs() << "push QV=" << this << " user="
	       // << user << " value=" << t << "\n";
	user.push<T>( &t, max_size, peekoff );
    }

    template<typename T>
    void push_fixed( const T & t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment<T>(
		max_size, peekoff, false );

	    segmented_queue q = user.split();
	    push_head( q );
	}
	// errs() << "push-fixed QV=" << this << " user="
	       // << user << " value=" << t << "\n";

	user.push<T>( &t, max_size, peekoff );
    }


    // Only for pop!
    bool empty() {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	ensure_queue_head();
	// errs() << "QV empty check: QV=" << *this << " queue=" << queue << std::endl;
	if( !queue.get_head() ) {
	    // errs() << "QV empty due to non-producing final segment" << std::endl;
	    return true;
	}
	bool r = queue.empty();
	// errs() << "QV empty? " << r << std::endl;
	return r;
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

std::ostream & operator << ( std::ostream & os, queue_flags_t f ) {
    char const * sep = "";
    if( f & qf_push ) {
	os << sep << "push";
	sep = "|";
    }
    if( f & qf_pop ) {
	os << sep << "pop";
	sep = "|";
    }
    return os;
}


} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

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
// + !!! parameterize queue_t with fixed-size-queue length
// + !!! microbenchmark with only a pop task that does an empty() check on the queue
//   this will currently spin forever.
// + !!! remove logical_head and tail from queue_version
//
// For prefix dependence types
// + distinguish circular/non-circular usage of queue segment
// + wait on all prior pops
// + starting head is always known when prefixdep is ready (arg_ready in tickets)
// + 
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

namespace obj {

template<typename MetaData>
class queue_base;

enum queue_flags_t { qf_push=1, qf_pop=2, qf_pushpop=3, qf_fixed=4,
		     qf_knhead=8, qf_kntail=16 };

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

    // The logical head and tail fields indicate where this procedure is
    // producing or consuming in the whole queue, if known. These variables
    // act as temporary variables in case an actual queue_segment has not
    // been allocated yet.
    // TODO: strive to remove these variables (they are copied in the 
    // segmented_queue's (queue and user).
    long logical_head, logical_tail;

    // Index structure: where to find segments at certain logical offsets.
    // TODO: store this in queue_t and pass pointer to it in here, copy on
    // nesting. That will make the storage of the index unique per queue.
    queue_index & qindex;
    size_t max_size;

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

protected:
    // Normal constructor, called from queue_t constructor
    queue_version( queue_index & qindex_ )
	: chead( 0 ), ctail( 0 ), fleft( 0 ), fright( 0 ), parent( 0 ),
	  flags( flags_t( qf_pushpop | qf_knhead | qf_kntail ) ),
	  logical_head( 0 ), logical_tail( 0 ), qindex( qindex_ ), max_size( 128 ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	user.set_logical( 0 );
	queue.set_logical( 0 );

	// errs() << "QV queue_t constructor for: " << this << "\n";
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
public:
    // This code is written with pushdep in mind; may need to be specialized
    // to popdep
    queue_version( queue_version<metadata_t> * qv,
		   qmode_t qmode, size_t fixed_length )
	: chead( 0 ), ctail( 0 ), fright( 0 ), parent( qv ),
	  flags( flags_t(qmode | ( qv->flags & (qf_knhead | qf_kntail) )) ),
	  logical_head( qv->logical_head ),
	  logical_tail( qv->logical_tail ), qindex( qv->qindex ),
	  max_size( qv->max_size ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	assert( flags_t(qmode & qv->flags) == flags_t(qmode & ~qf_fixed)
		&& "increasing set of permissions on a spawn" );

	// Link in frame with siblings and parent
	parent->lock();

	// Update parent flags and logical. Flags require lock due to
	// non-atomic update.
	if( flags & qf_pop ) {
	    if( !(qmode & qf_fixed) )
		qv->flags = flags_t( qv->flags & ~qf_knhead );
	    if( qv->flags & qf_knhead )
		qv->logical_head += fixed_length;
	    else
		qv->logical_head = -1;
	}
	if( flags & qf_push ) {
	    if( !(qmode & qf_fixed) )
		qv->flags = flags_t( qv->flags & ~qf_kntail );
	    if( qv->flags & qf_kntail )
		qv->logical_tail += fixed_length;
	    else
		qv->logical_tail = -1;
	}

	// TODO: logical has to come in through the user hypermap
	// the parent hypermap is either null (unknown) or has a logical as well

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

	// Move over the parent's USER hypermap to the
	// child's USER hypermap. The child's other hypermaps remain empty.
	user.take( parent->user );
	if( user.get_tail() )
	    user.get_tail()->set_producing();
	parent->user.set_logical( parent->logical_tail );

	if( qmode & qf_pop ) {
	    // queue.set_logical( logical_tail );
	    queue.take( parent->queue );
	    parent->queue.set_logical( parent->logical_head );
	}

	parent->unlock();

/*
	errs() << "QV nest constructor for: " << *this << "\n";
	errs() << "                 parent: " << *parent << "\n";
*/
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

	user.erase( qindex );
    }

    void reduce_sync() {
	// user <- children REDUCE user
	user.reduce_reverse( children, qindex );
    }

    void reduce_hypermaps( bool push, bool is_stack ) {
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

/*
	errs() << "Reducing hypermaps on " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << "\n";
*/

	// Reducing everything into a single queue
	children.reduce( user.reduce( right, qindex ), qindex );

	// Clear producing flag on user tail, but only after reducing lists.
	// Segment becomes final when
	// * move into parent->children (it is children's tail)
	// * move into fleft->right (it is right's tail)
	//      .. but there is potentially a path to a parent's user tail???
	// * it is user's tail and has a next segment???
/*
	errs() << "Reducing hypermaps on " << this
	       << " result is: children=" << children << "\n";
*/

	// Move up hierarchy
	if( push ) {
	    if( fleft ) {
		// fleft->lock();
		fleft->right.reduce( children, qindex );
		// errs() << "Reduce hypermaps on " << this << " left: " << fleft
		       // << " right=" << fleft->right << "\n";
		// fleft->unlock();
	    } else {
		assert( parent );
		parent->children.reduce( children, qindex );
		if( is_stack )
		    parent->user.reduce( parent->children, qindex );
/*
		errs() << "Reduce hypermaps on " << this
		       << " parent: " << parent
		       << " user=" << parent->user
		       << " children=" << parent->children
		       << " right=" << parent->right
		       << "\n";
*/
	    }
	} else {
	    parent->children.reduce( children, qindex ); // assert children == 0?
	    if( is_stack )
		parent->user.reduce( parent->children, qindex );
	}

/*
	errs() << "Reducing hypermaps DONE on " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << "\n";
*/

	unlink();

	// ???
	if( push && (parent->flags & qf_pushpop) == qf_pushpop
	    && ( !parent->chead
		 || (unsigned(parent->chead->flags) & unsigned(qf_pop) ) ) ) {
	    if( is_stack ) {
		if( parent->user.get_tail() )
		    parent->user.get_tail()->clr_producing();
	    } else {
		if( parent->children.get_tail() )
		    parent->children.get_tail()->clr_producing();
	    }
	}

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
	if( parent )
	    parent->lock();
	if( fleft ) {
	    // We need the lock to avoid the left sibling from terminating
	    // while we update the right "hypermap".
	    fleft->lock();
	    lock();
	    parent->unlock();

	    fleft->right.reduce_headonly( q, qindex );

	    fleft->unlock();
	    unlock();
	} else if( parent ) {
	    // We do not need to lock the parent because the parent cannot
	    // terminate while we reduce the hypermap. Are there other reasons
	    // for concurrency issues?
	    lock();

	    bool cont = !parent->children.get_tail();
	    // stack/full distinction?
	    parent->children.reduce_headonly( q, qindex );

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
	    // and will be accessed that way.
	    assert( children.get_head() );
	    assert( !children.get_tail() );
	    if( queue_segment * seg = children.get_head() ) {
		if( seg->get_slot() >= 0 )
		    children.set_head( 0 ); // Moved to index
	    }
	}
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
	    assert( queue.get_logical() >= 0 && "logical index of head queue"
		    " segment must be known" );

	    // Find the head of the queue for concurrently popping results.
	    // Simple case: there are no other popdep tasks on the same queue
	    // executing, so the other running queue-dep tasks, if any, have
	    // pushdep usage. For this reason, we do not look at the left
	    // sibling (that is the youngest pushdep task), but we look at the
	    // common parent, where the head segment has been pushed up, if it
	    // has been created already.
	    // More complicated case (current): use the queue_index to figure
	    // out what queue segment starts at the required logical position.
	    while( !queue.get_head() ) {
		sched_yield();
		// Search the index
		queue.set_head( qindex.lookup( queue.get_logical() ) );
	    }
	}
    }

public:
    // Debugging - pop interface
    size_t get_index() const {
	return queue.get_index();
    }

    template<typename T>
    void pop( T & t ) {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	// errs() << "pop QV=" << this << " queue=" << queue
	  //      << " logical_head=" << logical_head << "\n";
	ensure_queue_head();
	queue.pop( t, qindex );
    }

    // Potentially differentiate const T & t versus T && t
    template<typename T>
    void push( T t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    // errs() << "QV push ltail=" << logical_tail << "\n";
	    user.push_segment<T>( logical_tail, max_size, qindex );

	    segmented_queue q = user.split();
	    push_head( q );
	}
	user.push<T>( &t, max_size, qindex );
    }

    // Only for pop!
    bool empty() {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	ensure_queue_head();
	return queue.empty( qindex );
    }
};

template<typename MD>
std::ostream & operator << ( std::ostream & os, queue_version<MD> & v ) {
    os << "queue_version " << &v
       << " queue=" << v.queue
       << " children=" << v.children
       << " user=" << v.user
       << " right=" << v.right
       << " flags=" << v.flags
       << " logical_head=" << v.logical_head
       << " logical_tail=" << v.logical_tail;
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
    if( f & qf_fixed ) {
	os << sep << "fixed";
	sep = "|";
    }
    if( f & qf_knhead ) {
	os << sep << "knhead";
	sep = "|";
    }
    if( f & qf_kntail ) {
	os << sep << "kntail";
	sep = "|";
    }
    return os;
}


} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

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
// + check whether all queue segments are properly deleted.
// + implement pushpopdep
// + parameterize queue_t with fixed-size-queue length
// + microbenchmark with only a pop task that does an empty() check on the queue
//   this will currently spin forever.
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
    segmented_queue user, children, right, queue;
    // TODO: SPECIALIZE case for queue_t and deps where queue_t holds tinfo
    // and deps have only a pointer to it (for compactness).
    q_typeinfo tinfo;

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
    queue_index idx;

/*
    pad_multiple<CACHE_ALIGNMENT, sizeof(metadata_t)
		 + 3*sizeof(segmented_queue)
		 + sizeof(q_typeinfo)
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
    queue_version( q_typeinfo tinfo_ )
	: tinfo( tinfo_ ),
	  chead( 0 ), ctail( 0 ), fleft( 0 ), fright( 0 ), parent( 0 ),
	  flags( flags_t( qf_pushpop | qf_knhead | qf_kntail ) ),
	  logical_head( 0 ), logical_tail( 0 ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	user.set_logical( 0 );
	queue.set_logical( 0 );

	errs() << "QV queue_t constructor for: " << this << "\n";
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
public:
    // This code is written with pushdep in mind; may need to be specialized
    // to popdep
    queue_version( queue_version<metadata_t> * qv,
		   qmode_t qmode, size_t fixed_length )
	: tinfo( qv->tinfo ), chead( 0 ), ctail( 0 ), fright( 0 ), parent( qv ),
	  flags( flags_t(qmode | ( qv->flags & (qf_knhead | qf_kntail) )) ),
	  logical_head( qv->logical_head ),
	  logical_tail( qv->logical_tail ) {
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

	// In case of a push, move over the parent's USER hypermap to the
	// child's USER hypermap. The child's other hypermaps remain empty.
	// if( unsigned(flags) & unsigned(qf_push) ) {
	// }
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

	errs() << "QV nest constructor for: " << *this << "\n";
	errs() << "                 parent: " << *parent << "\n";
/*
*/
    }

    ~queue_version() {
	queue.erase( get_index_ref() ); // TODO: should be 0,0
	children.erase( get_index_ref() ); // TODO: should be 0,0
	user.erase( get_index_ref() );
	right.erase( get_index_ref() ); // TODO: should be 0,0
	errs() << "QV destruct:  " << *this << "\n";
    }

    void reduce_sync() {
	children.reduce( user, get_index_ref() );
	children.swap( user );
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
	children.reduce( user.reduce( right, get_index_ref() ), get_index_ref() );

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
		fleft->right.reduce( children, get_index_ref() );
		// errs() << "Reduce hypermaps on " << this << " left: " << fleft
		       // << " right=" << fleft->right << "\n";
		// fleft->unlock();
	    } else {
		assert( parent );
		parent->children.reduce( children, get_index_ref() );
		if( is_stack )
		    parent->user.reduce( parent->children, get_index_ref() );
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
	    // Nothing to do for pop
	    // Put back the queue on the parent. As pops are fully serialized,
	    // this should correctly re-initialize the queue on the parent
	    // (the pop is always the oldest when it executes).
	    // parent->queue.take_head( queue );

	    parent->children.reduce( children, get_index_ref() ); // assert children == 0?
	    if( is_stack )
		parent->user.reduce( parent->children, get_index_ref() );
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
	// --- locking bug detected: race with delete of fleft - lock parent as well even though we don't need the parent in this case to avoid deletion of fleft between if( fleft ) and fleft->lock()
	if( parent ) // lock parent to avoid deletion of fleft
	    parent->lock();
	if( fleft ) {
	    // We need the lock to avoid the left sibling from terminating
	    // while we update the right "hypermap".
	    fleft->lock();
	    lock();
	    parent->unlock();

	    fleft->right.reduce_trailing( q, get_index_ref() );

	    fleft->unlock();
	    unlock();
	} else if( parent ) {
	    // We do not need to lock the parent because the parent cannot
	    // terminate while we reduce the hypermap. Are there other reasons
	    // for concurrency issues?
	    lock();

	    bool cont = !parent->children.get_tail();
	    // stack/full distinction?
	    parent->children.reduce( q, get_index_ref() );
/*
	    bool cont = false;
	    if( parent->children.get_tail() )
		parent->children.reduce_trailing( q );
	    else {
		parent->queue.reduce_trailing( q );
		cont = true;
	    }
*/

	    unlock();
            parent->unlock();

	    // The parent cannot disappear while the child is executing.
	    // We need to lock the grandparent while pushing because a
	    // consumer may be accessing the segmented_queue as we are
	    // initializing it.
	    // Recursion must stop at first encountered frame with push and pop
	    // possibilities, ie, queue_t or pushpopdep if we introduce it.
	    // Do we really need this once we have the queue_index?
	    if( cont /* && parent->parent */ )
		parent->push_head( parent->children );
	} else {
	    // ! parent
	    // If we do not have a parent, then the push terminates.
	    // Do nothing here.
	    // Evaporate the segment pointer. It is registered in the index
	    // and will be accessed that way.
	    assert( children.get_head() );
	    assert( !children.get_tail() );
	    if( queue_segment * seg = children.get_head() ) {
		errs() << "push_head QV=" << *this << " final push_head seg=" << *seg
		       << "\n";
		if( seg->get_slot() >= 0 )
		    children.set_head( 0 ); // Moved to index
	    }
	}
    }

    // Find the head of the queue for concurrently popping results.
    // We assume that there are no other popdep tasks on the same queue
    // executing, so the other running queue-dep tasks, if any, have pushdep
    // usage. For this reason, we do not look at the left sibling (that is
    // the youngest pushdep task), but we look at the common parent, where
    // the head segment has been pushed up, if it has been created already.
    void pop_head( segmented_queue & q ) {
	// Search the index
	if( parent ) {
	    parent->pop_head( q );
	} else {
	    q.set_head( idx.lookup( q.get_logical() ) );
	}
#if 0
	assert( parent );

	// We do not need to lock the parent because the parent cannot
	// terminate while we reduce the hypermap. Are there other reasons
	// for concurrency issues?

	parent->lock();
	// We should not take the parent->queue if there is an older
	// outstanding pop. It is more efficient to test using metadata,
	// but that is not easily accessible here.
	// Moreover, it makes sense to label a popdep as not ready based
	// on metadata in the dependency tracking code.
	// TODO: also check on child pop's: spawn popdep; q.pop();
	// The following code would be useful only when supporting pushpopdeps
#if 0
	for( queue_version * qv = parent->chead; qv; qv=qv->fright ) {
	    if( qv == this )
		break;
	    else if( unsigned(qv->flags) & unsigned(qf_pop) ) {
		parent->unlock();
		return;
	    }
	}
#endif

	lock();
	// TODO: remove queue, go through children...
	q.take_head( parent->queue );
	queue_segment * seg = q.get_head();

/*
	errs() << "pop_head on " << this << " seg1=" << seg
	       << " parent=" << parent
	       << "\n";
*/

	parent->unlock();
	unlock();

	if( !seg && parent->parent )
	    // TODO: restrict recursion to pop-only frames, or
	    //       if oldest pop in pop+push frame
	    parent->pop_head( q );
	else {
	    assert( !q.get_tail() && "tail must be NULL when popping head" );
	}
#endif
    }

/*
    void set_index( queue_segment * seg, long logical ) {
	if( parent )
	    parent->set_index( seg, logical );
	else {
	    seg->set_logical_pos( logical );
	    idx.insert( seg );
	}
    }
*/

public:
    const metadata_t * get_metadata() const { return &metadata; }
    metadata_t * get_metadata() { return &metadata; }

    // For debugging
    const queue_version<metadata_t> * get_parent() const { return parent; }
    queue_version<metadata_t> * get_parent() { return parent; }

private:
    void ensure_queue_head() {
	if( !queue.get_head() ) {
	    if( queue.get_logical() < 0 ) {
		errs() << "ERROR: logical index of head queue segment "
		    "is unknown. QV=" << this << "\n";
		errs() << "\n";
		abort();
	    }
	    while( !queue.get_head() ) {
		pop_head( queue );
		errs() << "ensure sched_yield\n";
		sched_yield();
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
	errs() << "pop QV=" << this << " queue=" << queue
	       << " logical_head=" << logical_head << "\n";
	ensure_queue_head();
	queue.pop( t, get_index_ref() );
    }
    template<typename T>
    void push( T t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    errs() << "QV push ltail=" << logical_tail << "\n";
	    user.push_segment( tinfo, logical_tail, get_index_ref() );

	    segmented_queue q;
	    q.take_head( user );
	    push_head( q );
	}
	user.push( reinterpret_cast<void *>( &t ), tinfo, get_index_ref() );
    }

    const q_typeinfo & get_tinfo() { return tinfo; }
	
    // Only for pop!
    bool empty() {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	ensure_queue_head();
	return queue.empty( get_index_ref() );
    }

    queue_index & get_index_ref() {
	if( parent )
	    return parent->get_index_ref();
	else
	    return idx;
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

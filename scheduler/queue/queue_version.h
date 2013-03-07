// -*- c++ -*-

#ifndef QUEUE_QUEUE_VERSION_H
#define QUEUE_QUEUE_VERSION_H

#include <sched.h>

#include "swan/queue/segmented_queue.h"

namespace obj {

template<typename MetaData>
class queue_base;

template<typename MetaData>
class queue_version
{
public:
    typedef MetaData metadata_t;

private:
    enum qmode_t { qm_push=1, qm_pop=2, qm_pushpop=3 };

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
    qmode_t qmode;

/*
    pad_multiple<CACHE_ALIGNMENT, sizeof(metadata_t)
		 + 3*sizeof(segmented_queue)
		 + sizeof(q_typeinfo)
		 + 5*sizeof(queue_version<metadata_t>*)
		 + sizeof(cas_mutex)> padding;
*/

public:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

protected:
    // Normal constructor, called from queue_t constructor
    queue_version( q_typeinfo tinfo_ )
	: tinfo( tinfo_ ),
	  chead( 0 ), ctail( 0 ), fleft( 0 ), fright( 0 ), parent( 0 ),
	  qmode( qm_pushpop ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	errs() << "QV queue_t constructor for: " << this << "\n";
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
public:
    // This code is written with pushdep in mind; may need to be specialized
    // to popdep
    queue_version( queue_version<metadata_t> * qv, bool push )
	: tinfo( qv->tinfo ), chead( 0 ), ctail( 0 ), fright( 0 ), parent( qv ),
	  qmode( push ? qm_push : qm_pop ) {
	// static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       // "padding failed" );

	// Link in frame with siblings and parent
	parent->lock();
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

	// init_hypermaps( push );

	parent->unlock();

	if( !push )
	    pop_head( queue );

	errs() << "QV nest constructor for: " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << " queue=" << queue
	       << " parent=" << parent
	       << " parent->queue=" << parent->queue
	       << " push=" << push
	       << "\n";
    }

    void init_hypermaps( bool push ) {
	// Update "hypermaps". Depends on push/pop distinction
	// This must be done when spawned. If the parent->user hypermap is
	// empty, then that is fine, we will try to initialize it agains as
	// we execute a push or a pop.
	// Take a lock on parent's user because parent body may be executing
	// and modifying user hypermap concurrently.
	// Do not lock this (child) because user is private.
	// parent->lock();
	if( push ) {
	    // TODO: initialize user from parent->user (move it over)

	    // user.take_tail( parent->user );
	    // children = segmented_queue( 0, 0 ); -- defaults ok
	    // right = segmented_queue( 0, 0 ); -- defaults ok
	    // queue = segmented_queue( 0, 0 ); -- defaults ok
	} else { // pop
	    // user.take_head( parent->user );
	    // TODO: possible race if multiple pops, first pop sees NULL,
	    // then queue head posted, then second pop takes it.
	    // queue.take_head( parent->queue );
	    pop_head( queue );
	    // queue = parent->queue;
	    // user = segmented_queue( 0, 0 ); -- defaults ok
	    // children = segmented_queue( 0, 0 ); -- defaults ok
	    // right = segmented_queue( 0, 0 ); -- defaults ok
	}
	// parent->unlock();
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

	// Get user tail segment to know which producing flag to clear.
	queue_segment * seg = user.get_tail();

	errs() << "Reducing hypermaps on " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << " queue=" << queue
	       << "\n";

	// Reducing everything into a single queue
	children.full_reduce( user.full_reduce( right ) );

	// Clear producing flag on user tail, but only after reducing lists.
	// Segment becomes final when
	// * move into parent->children (it is children's tail)
	// * move into fleft->right (it is right's tail)
	//      .. but there is potentially a path to a parent's user tail???
	// * it is user's tail and has a next segment???
	/*
	if( seg && ( seg->get_next() || !parent->parent ) )
	    seg->clr_producing();
	*/

	errs() << "Reducing hypermaps on " << this
	       << " result is: children=" << children << "\n";

	// Deallocate if possible
	children.cleanup();

	// Move up hierarchy
	if( push ) {
	    if( fleft ) {
		// fleft->lock();
		fleft->right.merge_reduce( children );
		errs() << "Reduce hypermaps on " << this << " left: " << fleft
		       << " right=" << fleft->right << "\n";
		// fleft->unlock();
	    } else {
		assert( parent );
		parent->children.merge_reduce( children );
		if( is_stack )
		    parent->user.merge_reduce( parent->children );
		// if( seg && seg != parent->user.get_tail() ) {
		// seg->next != 0 only if no tail pointing to it, therefore,
		// cannot extend it further after reduction
/*
		if( seg && seg->get_next() ) {
		    errs() << "clear producing " << seg << "\n";
		    seg->clr_producing();
		}
*/
		errs() << "Reduce hypermaps on " << this
		       << " parent: " << parent
		       << " user=" << parent->user
		       << " children=" << parent->children
		       << " right=" << parent->right
		       << " queue=" << parent->queue
		       << " seg=" << seg
		       << "\n";
	    }
	} else {
	    // Nothing to do for pop
	    // Put back the queue on the parent. As pops are fully serialized,
	    // this should correctly re-initialize the queue on the parent
	    // (the pop is always the oldest when it executes).
	    parent->queue.take_head( queue );

	    parent->children.merge_reduce( children ); // assert children == 0?
	    if( is_stack )
		parent->user.merge_reduce( parent->children );
	}

	errs() << "Reducing hypermaps DONE on " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << " queue=" << queue
	       << "\n";

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
	errs() << "push_head on " << this << " head=" << q.get_head() << "\n";
	if( parent ) // lock parent to avoid deletion of fleft
	    parent->lock();
	if( fleft ) {
	    // We need the lock to avoid the left sibling from terminating
	    // while we update the right "hypermap".
	    fleft->lock(); // --- locking bug detected: race with delete of fleft - lock parent as well even though we don't need the parent in this case to avoid deletion of fleft between if( fleft ) and fleft->lock()
	lock();
	    parent->unlock();
	    fleft->right.reduce_head( q );
	    fleft->unlock();
	unlock();
	    errs() << "NOOP to left: " << fleft << " right=" << fleft->right << "\n";
	} else if( parent ) {
	    // We do not need to lock the parent because the parent cannot
	    // terminate while we reduce the hypermap. Are there other reasons
	    // for concurrency issues?
	lock();
	    errs() << "to parent: " << parent
		   << " user=" << parent->user
		   << " children=" << parent->children
		   << " right=" << parent->right
		   << " queue=" << parent->queue
		   << "\n";
	    bool cont = false;
	    if( parent->children.get_tail() )
		parent->children.reduce_tail( q );
	    else if( parent->user.get_tail() )
		parent->user.reduce_tail( q );
	    else {
		parent->queue.reduce_head( q );
		cont = true;
	    }
	    errs() << "now parent is: " << parent
		   << " user=" << parent->user
		   << " children=" << parent->children
		   << " right=" << parent->right
		   << " queue=" << parent->queue
		   << "\n";
	    parent->unlock();
	unlock();

	    // The parent cannot disappear while the child is executing.
	    // We need to lock the grandparent while pushing because a
	    // consumer may be accessing the segmented_queue as we are
	    // initializing it.
	// Recursion must stop at first encountered frame with push and pop
	// possibilities, ie, queue_t or pushpopdep if we introduce it.
	    if( cont && parent->parent )
		parent->push_head( parent->queue );
	    // else if( !parent->fleft )
	    // parent->queue.reduce_head( parent->children );
	    errs() << "to parent: " << parent << " queue=" << parent->queue << "\n";
	} else {
	    // If we do not have a parent, then the push terminates.
	    // Do nothing here.
	}
    }

    // Find the head of the queue for concurrently popping results.
    // We assume that there are no other popdep tasks on the same queue
    // executing, so the other running queue-dep tasks, if any, have pushdep
    // usage. For this reason, we do not look at the left sibling (that is
    // the youngest pushdep task), but we look at the common parent, where
    // the head segment has been pushed up, if it has been created already.
    void pop_head( segmented_queue & q ) {
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
	for( queue_version * qv = parent->chead; qv; qv=qv->fright ) {
	    if( qv == this )
		break;
	    else if( unsigned(qv->qmode) & unsigned(qm_pop) ) {
		parent->unlock();
		return;
	    }
	}

	lock();
	// queue_segment * seg = parent->queue.get_head();
	q.take_head( parent->queue );
	queue_segment * seg = q.get_head();

	errs() << "pop_head on " << this << " seg1=" << seg
	       << " parent=" << parent
	       << " parent.queue=" << parent->queue
	       << "\n";

	parent->unlock();
	unlock();

	if( !seg && parent->parent )
	    // TODO: restrict recursion to pop-only frames, or
	    //       if oldest pop in pop+push frame
	    parent->pop_head( q );
	else {
	    // this creates a second copy of the head, but it is not owned
	    // because the tail is NULL.
	    // q.set_head( seg );
	    assert( !q.get_tail() && "tail must be NULL when popping head" );
	}
    }

public:
    ~queue_version() {
	errs() << "QV destructor for: " << this
	       << " user=" << user
	       << " children=" << children
	       << " right=" << right
	       << " queue=" << queue
	       << "\n";

	// only for queue_t and to enable dealloc of segments ?
	// queue.merge_reduce( user );

	// lock(); // avoid anyone updating while we destruct by leaving locked
	// unlink();
	// TODO: what if no release_deps() called?
    }
	
    const metadata_t * get_metadata() const { return &metadata; }
    metadata_t * get_metadata() { return &metadata; }

    // For debugging
    const queue_version<metadata_t> * get_parent() const { return parent; }
    queue_version<metadata_t> * get_parent() { return parent; }

    template<typename T>
    void pop( T & t ) {
	// Make sure we have a local, usable queue. Busy-wait if necessary
	// until we have made contact with the task that pushes.
	while( !queue.get_head() ) {
	    pop_head( queue );
	    sched_yield();
	}
	queue.pop( t );
    }
    template<typename T>
    void push( T t ) {
	// Make sure we have a local, usable queue
	if( !user.get_tail() ) {
	    user.push_segment( tinfo );
	    push_head( user );
	}
	user.push( reinterpret_cast<void *>( &t ), tinfo );
    }

    const q_typeinfo & get_tinfo() { return tinfo; }
	
    // bool empty() { return this->payload->empty(); } How to do this?
};

} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

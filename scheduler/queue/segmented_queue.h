// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include <iostream>
#include "swan/queue/queue_segment.h"

namespace obj {

class queue_index {
    std::vector<queue_segment *> idx;
    size_t used;
    size_t size;
    cas_mutex mutex;

private:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

public:
    queue_index() : used( 0 ), size( 0 ) { }

private:
    size_t get_free_position( queue_segment * seg ) {
	lock();
	for( size_t i=0; i < used; ++i ) {
	    if( idx[i] == 0 ) {
		idx[i] = seg;
		unlock();
		return i;
	    }
	}
	if( used >= size ) {
	    size += 16;
	    idx.reserve( size );
	}
	size_t slot = used++;
	idx[slot] = seg;
	unlock();
	return slot;
    }

public:
    size_t insert( queue_segment * seg ) {
	assert( seg->get_logical_pos() >= 0 );
	size_t slot = get_free_position( seg );
	seg->set_slot( slot );
	errs() << "Index " << this << " insert logical="
	       << seg->get_logical_head() << '-'
	       << seg->get_logical_tail()
	       << " seg " << seg << " at slot " << slot
	       << "\n";
	return slot;
    }

    // Lock is required in case vector resizes.
    void erase( size_t slot ) {
	lock();
	errs() << "Index " << this << " erase " << idx[slot]
	       << " slot " << slot << "\n";
	idx[slot] = 0;
	unlock();
    }

    queue_segment * lookup( long logical ) const {
	errs() << "Index " << this << " lookup logical="
	       << logical << "\n";
	for( size_t i=0; i < used; ++i ) {
	    if( queue_segment * seg = idx[i] ) {
		errs() << "Index entry " << *seg << " logical="
		       << seg->get_logical_head() << '-'
		       << seg->get_logical_tail() << "\n";
		if( seg && seg->get_logical_head() <= logical
		    && seg->get_logical_tail() > logical )
		    return seg;
	    }
	}
	return 0;
    }

    void replace( int slot, long logical, queue_segment * new_seg ) {
	queue_segment * seg = idx[slot];
	assert( seg && "Segment not indexed" );
	errs() << "Index replace " << seg << " @" << slot
	       << " for new_seg=" << *new_seg
	       << " at logical=" << logical << "\n";


	if( new_seg->get_slot() >= 0 ) {
	    idx[slot] = 0;
	} else {
	    idx[slot] = new_seg;
	    new_seg->set_slot( slot );
	}
	// Premature: must check that first half of segment is left unconsumed
	seg->set_slot( -1 );
    }
};


class segmented_queue_base {
protected:
    // Place-holder until we have allocated a queue_segment to hold
    // this value.
    long logical;

public:
    segmented_queue_base() : logical( -1 ) { }

    long get_logical() const { return logical; }
    void set_logical( long logical_ ) { logical = logical_; }

};

class segmented_queue;
class segmented_queue_headonly;
class segmented_queue_pop;
class segmented_queue_push;

inline std::ostream &
operator << ( std::ostream & os, const segmented_queue & seg );
inline std::ostream &
operator << ( std::ostream & os, const segmented_queue_push & seg );
inline std::ostream &
operator << ( std::ostream & os, const segmented_queue_pop & seg );


// TODO: create base class with reduce functionality and subclasses
// with either push or pop functionality. We will never push and pop from
// the same segmented_queue. Rather, a pushpopdep procedure will push to user
// and pop from queue (in queue_version). These subclasses are our views on
// the queue and may limit the number of pushes and pops performed.
// Note: pop functionality applies to a segmented_queue where only the head
// is known. Push may operate on segmented_queues with a head and a tail.
// Note that there is a slight asymmetry between push and pop as concurrent
// pushes to the same hyperqueue will push to different segments by
// construction. Pops may occur concurrently on the same segments, as may
// pop and push.
class segmented_queue : public segmented_queue_base {
protected:
    queue_segment * head, * tail;

    // head may be NULL and tail non-NULL. In this case, the list has been
    // reduced but we know that the current segmented_queue is the last such
    // one (the right-most in the reduction) and therefore any addition
    // in the queue goes to the tail and growing the segment grows the tail.
    // if head is NULL, the segments should not be deallocated?
    // NOTE: there may be multiple links to a tail segment. How do we update
    // them?

public: 
    segmented_queue() : head( 0 ), tail( 0 ) { }
    void erase( queue_index & idx ) {
	// Ownership is determined when both head and tail are non-NULL.
	/*
	errs() << "destruct qseg: head=" << head << " tail=" << tail << "\n";
	if( head ) errs() << "         head: " << *head << "\n"; 
	if( tail ) errs() << "         tail: " << *tail << "\n"; 
	*/

	if( head != 0 && tail != 0 ) {
	    for( queue_segment * q=head, * q_next; q != tail; q = q_next ) {
		q_next = q->get_next();
		if( q->get_slot() >= 0 ) {
		    idx.erase( q->get_slot() );
		    // errs() << "ERROR: cannot free local segment if slotted\n";
		    // abort();
		}
		delete q;
	    }
	}
    }

    queue_segment * get_tail() { return tail; }
    queue_segment * get_head() { return head; }
    const queue_segment * get_tail() const { return tail; }
    const queue_segment * get_head() const { return head; }
    void set_head( queue_segment * seg ) { head = seg; }

    void reset() {
	head = tail = 0;
	logical = -1;
    }

    void take( segmented_queue & from ) {
	std::swap( *this, from );
    }

    // Retain from.logical as we are keeping the from queue segment at the
    // same logical position!
    segmented_queue split() {
	segmented_queue h;
	h.head = head;
	h.logical = logical;
	head = 0;
	return h;
    }

public:
    segmented_queue &
    reduce( segmented_queue & right, queue_index & idx ) {
	if( !tail ) {
	    if( !head ) {
		head = right.get_head();
		logical = right.get_logical();
	    }
	    tail = right.get_tail();
	    right.reset();
	} else if( right.get_head() ) {
	    if( tail->get_logical_tail() >= 0 )
		set_logical_seq( right.get_head(),
				 tail->get_logical_tail(), idx );
	    tail->set_next( right.get_head() );
	    tail = right.get_tail(); // may be 0
	    right.reset();
	} else {
	    assert( !right.get_tail() );
	}
	return *this;
    }

    segmented_queue &
    reduce_reverse( segmented_queue & left, queue_index & idx ) {
	left.reduce( *this, idx );
	std::swap( left, *this );
	return *this;
    }

    segmented_queue &
    reduce_headonly( segmented_queue & right, queue_index & idx ) {
	assert( right.get_head() && !right.get_tail() );

	if( !tail ) {
	    if( !head ) {
		head = right.get_head();
		logical = right.get_logical();
	    }
	    right.reset();
	} else {
	    if( tail->get_logical_tail() >= 0 )
		set_logical_seq( right.get_head(),
				 tail->get_logical_tail(), idx );
	    tail->set_next( right.get_head() );
	    tail = 0;
	    right.reset();
	}
	return *this;
    }

private:
    // Beware of race condition between propagating logical position
    // versus pushing new segment and updating logical position.
    // Should be ok if link before update position.
    static void
    set_logical_seq( queue_segment * seg, long logical, queue_index & idx ) {
	assert( seg->get_logical_pos() < 0
		&& "logical position must be unknown when updating" );
	seg->set_logical_pos( logical );
	idx.insert( seg );
	if( queue_segment * nxt = seg->get_next() )
	    set_logical_seq( nxt, seg->get_logical_tail(), idx );
    }
};

class segmented_queue_headonly : public segmented_queue_base {
protected:
    queue_segment * head;

public: 
    segmented_queue_headonly() : head( 0 ) { }

    queue_segment * get_tail() { return 0; }
    queue_segment * get_head() { return head; }
    const queue_segment * get_tail() const { return 0; }
    const queue_segment * get_head() const { return head; }
    void set_head( queue_segment * seg ) { head = seg; }

    void reset() {
	head = 0;
	logical = -1;
    }

    void take( segmented_queue_headonly & from ) {
	std::swap( *this, from );
    }
};

class segmented_queue_push : public segmented_queue {
    size_t volume_push;

public: 
    segmented_queue_push() : volume_push( 0 ) { }

    size_t get_volume_push() const { return volume_push; }

    void push_segment( const q_typeinfo & tinfo, long logical_pos,
		       queue_index & idx ) {
	logical = logical_pos; // tail ? tail->get_logical_tail() : -1;
	queue_segment * seg = queue_segment::create( tinfo, logical );
	// seg->set_producing();
	if( tail ) {
	    tail->clr_producing();
	    tail->set_next( seg );
	} else // if tail == 0, then also head == 0
	    head = seg;
	tail = seg;

	if( logical >= 0 )
	    idx.insert( tail );
    }

    void push( void * value, const q_typeinfo & tinfo, queue_index & idx ) {
	assert( tail );
	if( tail->is_full() )
	    push_segment( tinfo, tail->get_logical_tail(), idx );
	errs() << "push on queue segment " << *tail << " SQ=" << *this << "\n";
	tail->push( value );
	volume_push++;
    }
};

class segmented_queue_pop : public segmented_queue_headonly {
    size_t volume_pop;

public: 
    segmented_queue_pop() : volume_pop( 0 ) { }

    size_t get_volume_pop() const { return volume_pop; }

private:
    void await( queue_index & idx ) {
	assert( head );

	// As long as nothing has appeared in the queue and the producing
	// flag is on, we don't really know if the queue is empty or not.
	// Spin until something appears at the next index we will read.
	size_t pos = get_index();
	do {
	    while( head->is_empty( pos ) && head->is_producing() ) {
		// busy wait
		sched_yield();
	    }
	    if( !head->is_empty( pos ) )
		return;
	    if( !head->is_producing() ) {
		if( queue_segment * seg = head->get_next() ) {
		    // Note: this case is executed only once per segment,
		    // namely for the task that pops the tail of this segment.

		    // errs() << "head " << head << " runs out, pop segment (empty)\n";
		    // Are we totally done with this segment?
		    // TODO: this may introduce a data race and not deallocate
		    // some segments as a result. Or even dealloc more than
		    // once...
		    bool erase
			= head->get_volume_pop() == head->get_volume_push();

		    if( seg->get_logical_pos() < 0 ) {
			// Every segment with known logical position should
			// be slotted??
			assert( seg->get_slot() >= 0 );
			// Redundant: set_logical_seq( seg, logical, idx );
		    }

		    // Compute our new position based on logical_pos, which is
		    // constant (as opposed to head and tail which differ by
		    // the number of pushes and pops performed).
		    logical = seg->get_logical_pos();
		    volume_pop = pos - logical;

		    // TODO: when to delete a segment in case of multiple
		    // consumers?
		    if( erase ) {
			if( erase && head->get_slot() >= 0 ) {
			    idx.erase( head->get_slot() );
			    head->set_slot( -1 );
			}
			delete head;
		    }

		    head = seg;
		} else {
		    // In this case, we know the queue is empty.
		    // This may be an error or not, depending on whether
		    // we are polling the queue for emptiness, or trying
		    // to pop.
		    return;
		}
	    }
	} while( true );
    }

public:
    bool empty( queue_index & idx ) {
	assert( head );

	// Is there anything in the queue? If so, return not empty
	if( likely( !head->is_empty( get_index() ) ) )
	    return false;

	// Spin until we are sure about emptiness (nothing more to be produced
	// for sure).
	await( idx );

	// Now check again. This result is for sure.
	return head->is_empty( get_index() );
    }

    size_t get_index() const {
	return logical + volume_pop;
    }

    template<typename T>
    void pop( T & t, queue_index & idx ) {
	// Spin until the desired information appears in the queue.
	await( idx );

	// We must be able to pop now.
	size_t pos = get_index();
	if( !head->is_empty( pos ) ) {
	    head->pop( t, pos );
	    volume_pop++;
	    // errs() << "pop from queue " << head << ": value="
		   // << std::dec << t << ' ' << *head
		   // << " SQ=" << *this
		   // << " position=" << pos
		   // << "\n";
	    return;
	}

	assert( !head->is_producing() );
	abort();
    }
};

std::ostream &
operator << ( std::ostream & os, const segmented_queue & seg ) {
    return os << '{' << seg.get_head()
	      << ", " << seg.get_tail()
	      << ", @" << seg.get_logical()
	      << '}';
}

std::ostream &
operator << ( std::ostream & os, const segmented_queue_push & seg ) {
    return os << '{' << seg.get_head()
	      << ", " << seg.get_tail()
	      << ", @" << seg.get_logical()
	      << "+" << seg.get_volume_push()
	      << '}';
}

std::ostream &
operator << ( std::ostream & os, const segmented_queue_pop & seg ) {
    return os << '{' << seg.get_head()
	      << ", " << seg.get_tail()
	      << ", @" << seg.get_logical()
	      << "-" << seg.get_volume_pop()
	      << '}';
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

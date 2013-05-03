// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include <iostream>
#include "swan/queue/queue_segment.h"

// Note: pop functionality applies to a segmented_queue where only the head
// is known. Push may operate on segmented_queues with a head and a tail.
// Note that there is a slight asymmetry between push and pop as concurrent
// pushes to the same hyperqueue will push to different segments by
// construction. Pops may occur concurrently on the same segments, as may
// pop and push.

namespace obj {

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

class segmented_queue {
protected:
    queue_segment * head, * tail;

    friend class segmented_queue_pop;

    // head may be NULL and tail non-NULL. In this case, the list has been
    // reduced but we know that the current segmented_queue is the last such
    // one (the right-most in the reduction) and therefore any addition
    // in the queue goes to the tail and growing the segment grows the tail.
    // if head is NULL, the segments should not be deallocated.

public: 
    segmented_queue() : head( 0 ), tail( 0 ) { }
    void erase() {
	// Ownership is determined when both head and tail are non-NULL.
	if( head != 0 && tail != 0 )
	    head->erase_all();
    }

    queue_segment * get_tail() { return tail; }
    queue_segment * get_head() { return head; }
    const queue_segment * get_tail() const { return tail; }
    const queue_segment * get_head() const { return head; }
    void set_head( queue_segment * seg ) { head = seg; }

    void reset() {
	head = tail = 0;
    }

    segmented_queue split() {
	segmented_queue h;
	h.head = head;
	head = 0;
	return h;
    }

public:
    segmented_queue &
    reduce( segmented_queue & right ) {
	if( !tail ) {
	    if( !head )
		head = right.get_head();
	    tail = right.get_tail();
	    right.reset();
	} else if( right.get_head() ) {
	    tail->set_next( right.get_head() );
	    tail = right.get_tail(); // may be 0
	    right.reset();
	} else {
	    assert( !right.get_tail() );
	}
	return *this;
    }

    segmented_queue &
    reduce_reverse( segmented_queue & left ) {
	left.reduce( *this );
	std::swap( left, *this );
	return *this;
    }

    segmented_queue &
    reduce_headonly( segmented_queue & right ) {
	assert( right.get_head() && !right.get_tail() );

	if( !tail ) {
	    if( !head ) {
		head = right.get_head();
	    }
	    right.reset();
	} else {
	    tail->set_next( right.get_head() );
	    tail = 0;
	    right.reset();
	}
	return *this;
    }
};

class segmented_queue_headonly {
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
    }

    void take( segmented_queue_headonly & from ) {
	std::swap( *this, from );
    }
};

class segmented_queue_push : public segmented_queue {
public: 
    segmented_queue_push() { }

    void take( segmented_queue_push & from ) {
	std::swap( *this, from );
    }

    template<typename T>
    void push_segment( size_t max_size, size_t peekoff, bool is_head ) {
	queue_segment * seg
	    = queue_segment::template create<T>( max_size, peekoff, is_head );
	if( tail ) {
	    tail->set_next( seg );
	    tail->clr_producing();
	} else {
	    assert( !head && "if tail == 0, then also head == 0" );
	    if( is_head )
		seg->rewind();
	    head = seg;
	}
	tail = seg;
    }

    template<typename T>
    void push( T && value, size_t max_size, size_t peekoff ) {
	assert( tail );
	// TODO: could introduce a delay here, e.g. if concurrent pop exists,
	// then just wait a bit for the pop to catch up and avoid inserting
	// a new segment.
	if( tail->is_full() )
	    push_segment<T>( max_size, peekoff, false );
	// errs() << "push on queue segment " << *tail << " SQ=" << *this << "\n";
	tail->push<T>( std::move( value ) );
    }

    template<typename T>
    void push( const T & value, size_t max_size, size_t peekoff ) {
	assert( tail );
	// TODO: could introduce a delay here, e.g. if concurrent pop exists,
	// then just wait a bit for the pop to catch up and avoid inserting
	// a new segment.
	if( tail->is_full() )
	    push_segment<T>( max_size, peekoff, false );
	// errs() << "push on queue segment " << *tail << " SQ=" << *this << "\n";
	tail->push<T>( value );
    }

    void push_bookkeeping( size_t npush ) {
	tail->push_bookkeeping( npush );
	// errs() << "push_bookkeeping on queue " << tail << ": " << *tail
	       // << " SQ=" << *this << std::endl;
    }

    template<typename MetaData, typename T>
    write_slice<MetaData,T> get_write_slice( size_t length ) {
	// Push a fresh segment if we don't have enough room on the
	// current one. Subtract again peek distance from length. Rationale:
	// we have already reserved this space in the current segment.
	if( !tail->has_space( length-tail->get_peek_dist() ) )
	    push_segment<T>( length, tail->get_peek_dist(), false );
	return tail->get_write_slice<MetaData,T>( length );
    }
};

class segmented_queue_pop : public segmented_queue_headonly {
public: 
    segmented_queue_pop() { }

private:
    void await() {
	assert( head );

	if( likely( !head->is_empty() ) )
	    return;

#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().sq_await );
#endif // PROFILE_QUEUE

	// errs() << "await0 " << *this << " head=" << *head << std::endl;

	// As long as nothing has appeared in the queue and the producing
	// flag is on, we don't really know if the queue is empty or not.
	// Spin until something appears at the next index we will read.
	do {
	    assert( head );
	    // errs() << "await " << *this << " head=" << *head << std::endl;

	    while( head->is_empty() && head->is_producing() ) {
		// busy wait
		sched_yield();
	    }
	    if( !head->is_empty() )
		break;
	    if( !head->is_producing() ) {
		if( queue_segment * seg = head->get_next() ) {
		    delete head;
		    head = seg;
		} else {
		    // In this case, we know the queue is empty.
		    // This may be an error or not, depending on whether
		    // we are polling the queue for emptiness, or trying
		    // to pop.
		    break;
		}
	    }
	} while( true );

#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().sq_await );
#endif // PROFILE_QUEUE
    }

public:
/*
    void advance_to_end( size_t length ) {
	if( head )
	    head->advance_to_end( length );
    }
*/

    void take_head( segmented_queue & from ) {
	head = from.head;
	from.head = 0;
    }

    void take( segmented_queue_pop & from ) {
	std::swap( *this, from );
    }

    bool empty() {
	assert( head );

	// Is there anything in the queue? If so, return not empty
	if( likely( !head->is_empty() ) )
	    return false;

	// Spin until we are sure about emptiness (nothing more to be produced
	// for sure).
	await();

	// Now check again. This result is for sure.
	return head->is_empty();
    }

    template<typename T>
    T && pop() {
	// Spin until the desired information appears in the queue.
	await();

	// errs() << "pop from queue " << head << ": " << *head
	// << " SQ=" << *this << std::endl;

	assert( !head->is_empty() );

	return head->pop<T>();
    }

    void pop_bookkeeping( size_t npop ) {
	head->pop_bookkeeping( npop );
    }

    template<typename T>
    T & peek( size_t off ) {
	// Spin until the desired information appears in the queue.
	await();

	// errs() << "peek from queue " << head << ": value="
	       // << std::dec << t << ' ' << *head
	       // << " SQ=" << *this
	       // << " offset=" << off
	       // << " position=" << pos << "\n";

#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().sq_peek );
#endif // PROFILE_QUEUE

	queue_segment * seg = head;
	while( unlikely( seg->is_empty() ) ) {
	    if( !seg->is_producing() )
		seg = head->get_next();
	    if( !seg->is_empty() )
		break;
	    sched_yield();
	}
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().sq_peek );
#endif // PROFILE_QUEUE

	assert( !seg->is_empty() );
	return seg->peek<T>();
    }

    // Get access to a part of the buffer that allow to peek npeek elements
    // and pop npop, where it is assumed that npeek >= npop, i.e., the peeked
    // elements include the popped ones.
    template<typename MetaData, typename T>
    read_slice<MetaData,T> get_read_slice_upto( size_t npop_max, size_t npeek ) {
	long npop;
	await();
	do { 
	    long available = 0; // head->???();
	    npop = std::min( (long)npop_max, available );
	    if( npop > 0 )
		break;
	    await();
	} while( true );

	return head->get_slice<MetaData,T>( npop );
    }
};

std::ostream &
operator << ( std::ostream & os, const segmented_queue & seg ) {
    return os << '{' << seg.get_head()
	      << ", " << seg.get_tail()
	      << '}';
}

std::ostream &
operator << ( std::ostream & os, const segmented_queue_push & seg ) {
    return os << '{' << seg.get_head()
	      << ", " << seg.get_tail()
	      << '}';
}

std::ostream &
operator << ( std::ostream & os, const segmented_queue_pop & seg ) {
    return os << '{' << seg.get_head()
	      << '}';
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include <iostream>
#include "swan/queue/queue_segment.h"

namespace obj {

class segmented_queue_base {
public:
    segmented_queue_base() { }
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
    void erase() {
	// Ownership is determined when both head and tail are non-NULL.
	if( head != 0 && tail != 0 ) {
	    for( queue_segment * q=head, * q_next; q != tail; q = q_next ) {
		q_next = q->get_next();
		queue_segment::deallocate( q, this );
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
	    if( !head ) {
		head = right.get_head();
	    }
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
    void push_segment( size_t max_size, size_t peekoff, size_t seqno, bool is_head ) {
	queue_segment * seg
	    = queue_segment::template create<T>( max_size, peekoff, seqno, is_head );
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
    void push( const T * value, size_t max_size, size_t peekoff, size_t seqno ) {
	assert( tail );
	// TODO: could introduce a delay here, e.g. if concurrent pop exists,
	// then just wait a bit for the pop to catch up and avoid inserting
	// a new segment.
	if( tail->is_full() ) {
	    // tail->lock();
	    push_segment<T>( max_size, peekoff, seqno, false );
	    // tail->unlock();
	}
	// errs() << "push on queue segment " << *tail << " SQ=" << *this << "\n";
	tail->push<T>( value );
    }

    void push_bookkeeping( size_t npush ) {
	tail->push_bookkeeping( npush );
	// errs() << "push_bookkeeping on queue " << tail << ": " << *tail
	       // << " SQ=" << *this << std::endl;
    }

    template<typename MetaData, typename T>
    write_slice<MetaData,T> get_write_slice( size_t length, size_t seqno ) {
	// Push a fresh segment if we don't have enough room on the
	// current one. Subtract again peek distance from length. Rationale:
	// we have already reserved this space in the current segment.
	if( !tail->has_space( length-tail->get_peek_dist() ) ) {
	    queue_segment * old_tail = tail;
	    old_tail->lock();
	    push_segment<T>( length, tail->get_peek_dist(), seqno, false );
	    old_tail->unlock();
	}
	return tail->get_write_slice<MetaData,T>( length );
    }
};

class segmented_queue_pop : public segmented_queue_headonly {
    size_t volume_pop;

public: 
    segmented_queue_pop() : volume_pop( 0 ) { }

    size_t get_volume_pop() const { return volume_pop; }

private:
    void pop_head() {
	// errs() << "pop_head head=" << *head << std::endl;
	if( queue_segment * seg = head->get_next() ) {
	    head->lock();

	    // Note: this case is executed only once per segment,
	    // namely for the task that pops the tail of this segment.

	    // errs() << "head " << head << " runs out, pop segment (empty)\n";

	    // Are we totally done with this segment?
	    // TODO: this may introduce a data race and not deallocate
	    // some segments as a result. Or even dealloc more than
	    // once...
	    // TODO: situation got worse with peeking
	    // IDEA: incorporate peeked condition as add one to
	    // push and pop volume rather than separate boolen condition
	    if( head->all_done() ) {
		queue_segment::deallocate( head, this );
	    } else {
		head->unlock();
	    }

	    head = seg;
	}
	assert( head );
    }

    void await() {
	assert( head );

	head->check_hash();

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

	    head->check_hash();

	    while( head->is_empty() && head->is_producing() ) {
		// busy wait
		sched_yield();
	    }
	    if( !head->is_empty() )
		break;
	    if( !head->is_producing() ) {
		if( head->get_next() ) {
		    pop_head();
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

    void take( segmented_queue_pop & from ) {
	std::swap( *this, from );
	volume_pop = 0;
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
    T & pop() {
	// Spin until the desired information appears in the queue.
	await();

	// errs() << "pop from queue " << head << ": " << *head
	// << " SQ=" << *this << std::endl;

	assert( !head->is_empty() );

	T & r = head->pop<T>();
	volume_pop++;
	return r;
    }

    void pop_bookkeeping( size_t npop, bool dealloc ) {
	// errs() << *this << " pop bookkeeping " << npop << std::endl;
	volume_pop += npop;
	head->pop_bookkeeping( npop );
	// errs() << "pop_bookkeeping on queue " << head << ": " << *head
	// << " SQ=" << *this
	// << " position=" << pos << std::endl;
	if( dealloc && head->is_empty() && !head->is_producing() )
	    pop_head();
/*
	while( npop-- > 0 )  {
	    volume_pop++;
	    head->pop_bookkeeping( 1 );
	    await();
	}
*/
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
	head->check_hash();
	while( unlikely( seg->is_empty() ) ) {
	    seg->check_hash();
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
    read_slice<MetaData,T> get_slice_upto( size_t npop_max, size_t npeek ) {
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

    template<typename MetaData, typename T>
    read_slice<MetaData,T> get_slice( size_t npop, size_t npeek ) {
	assert( npeek >= npop );
	// If we know that npeek <= peekoff, then we should be fine with npop > 1
	// except when the pops span segments, i.e., all npeek values in current
	// segment except that 2nd popped value is here only for peek reasons
	// and should be popped from the next segment.
	// assert( npop == 1 );

	// Spin until the desired information appears in the queue.
	await();

	// Make sure we have all elements available
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
	      << "-" << seg.get_volume_pop()
	      << '}';
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

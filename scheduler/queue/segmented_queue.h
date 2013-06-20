// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include <iostream>
#include "swan/queue/queue_segment.h"

namespace obj {

class segmented_queue
{
private:
    queue_segment * head, * tail;
    // TODO: and probably a lock as well for prod/consumer parallelism
    // Can we avoid the lock if the consumer will leave at least one segment,
    // ie it will never read/modify tail, and only modify head if gaining access
    // at which time the producer will not modify head anymore, ie, first set
    // head to non-zero, then never touch again, and on the other side, wait
    // until head is non-zero, only then modify
    // (Sounds like a good match with temporal logic.)

    // head may be NULL and tail non-NULL. In this case, the list has been
    // reduced but we know that the current segmented_queue is the last such
    // one (the right-most in the reduction) and therefore any addition
    // in the queue goes to the tail and growing the segment grows the tail.
    // if head is NULL, the segments should not be deallocated?
    // NOTE: there may be multiple links to a tail segment. How do we update
    // them?

public: 
    segmented_queue() : head( 0 ), tail( 0 ) { }
    ~segmented_queue() {
	// Ownership is determined when both head and tail are non-NULL.
	// errs() << "destruct qseg: head=" << head << " tail=" << tail << "\n";
	if( head != 0 && tail != 0 ) {
	    for( queue_segment * q=head, * q_next; q != tail; q = q_next ) {
		q_next = q->get_next();
		delete q;
	    }
	}
    }

    queue_segment * get_tail() { return tail; }
    queue_segment * get_head() { return head; }
    const queue_segment * get_tail() const { return tail; }
    const queue_segment * get_head() const { return head; }
    void set_head( queue_segment * seg ) { head = seg; }

    void take( segmented_queue & from ) {
	*this = from;
	from.head = from.tail = 0;
    }

    void take_head( segmented_queue & from ) {
	head = *const_cast<queue_segment * volatile * >( &from.head );
	from.head = 0;
    }

    void take_tail( segmented_queue & from ) {
	tail = from.tail;
	from.tail = 0;
    }

    segmented_queue & reduce( segmented_queue & right ) {
	if( !tail ) {
	    if( !head )
		head = right.head;
	    tail = right.tail;
	    right.head = 0;
	    right.tail = 0;
	} else if( right.head ) {
	    // assert( right.tail );
	    tail->set_next( right.head );
	    tail = right.tail; // may be 0
	    right.head = 0;
	    right.tail = 0;
	} else {
	    assert( !right.tail );
	}
	return *this;
    }
 
    // Special case of reduce for publishing queue head:
    // We know that right.head != 0 and right.tail == 0
    segmented_queue & reduce_trailing( segmented_queue & right ) {
	assert( right.head != 0 && "assumption of special reduction case" );
	assert( right.tail == 0 && "assumption of special reduction case" );

	// Reduce fresh pop-user into parent's children when tail != 0
	if( !tail ) {
	    if( !head )
		head = right.head;
	    right.head = 0;
	    right.tail = 0;
	} else {
	    tail->set_next( right.head ); // link to next first - race cond!
	    tail = 0;
	    right.head = 0;
	}
	return *this;
    }

    void swap( segmented_queue & right ) {
	queue_segment * h = right.head, * t = right.tail;
	right.head = head;
	right.tail = tail;
	head = h;
	tail = t;
    }

public:
    void push_segment( const q_typeinfo & tinfo ) {
	queue_segment * seg = queue_segment::create( tinfo );
	seg->set_producing();
	if( tail ) {
	    tail->clr_producing();
	    tail->set_next( seg );
	} else // if tail == 0, then also head == 0
	    head = seg;
	tail = seg;
    }

public:
    bool empty() {
	assert( head );

	// Is there anything in the queue? If so, return not empty
	if( likely( !head->is_empty() ) )
	    return false;

	// TODO: split remainder off in non-inlineable procedure?

	// As long as nothing has appeared in the queue and the producing
	// flag is on, we don't really know if the queue is empty or not.
	// Spin.
	do {
	    while( head->is_empty() && head->is_producing() ) {
		// busy wait
		sched_yield();
	    }
	    if( !head->is_empty() )
		return false;
	    else if( !head->is_producing() ) {
		if( queue_segment * seg = head->get_next() ) {
		    delete head;
		    head = seg;
		} else {
		    return true;
		}
	    }
	} while( true );
    }

    template<typename T>
    void pop( T & t ) {
	// TODO: merge code with empty()?

	do {
	    while( !head || ( head->is_empty() && head->is_producing() ) ) {
		// busy wait
		sched_yield();
	    }
	    if( !head->is_empty() ) {
		head->pop( t );
		/*
		errs() << "pop from queue " << head << ": value="
		       << std::dec << t << ' ' << *head << "\n";
		*/
		return;
	    }
	    if( !head->is_producing() ) {
		// Always retain at least one segment in the queue,
		// because we own the head, but we do not always own the tail,
		// so we cannot change the tail.
		if( queue_segment * seg = head->get_next() ) {
		    delete head;
		    head = seg;
		} else
		    break; 
	    }
	} while( true );
/*
	errs() << "No more data for consumer!!! head = "<< head
	       << " tail=" << tail
	       << ' ' << *head <<"\n";
	errs() << "newline\n";
*/
	abort();
    }
	
    void push( void * value, const q_typeinfo & tinfo ) {
	if( !tail || tail->is_full() )
	    push_segment( tinfo );
	// errs() << "push on queue segment " << tail << "\n";
	tail->push( value );
    }
};

inline std::ostream &
operator << ( std::ostream & os, const segmented_queue & seg ) {
    return os << '{' << seg.get_head() << ", " << seg.get_tail() << '}';
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

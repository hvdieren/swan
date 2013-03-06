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
	errs() << "destruct qseg: head=" << head << " tail=" << tail << "\n";
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

    void reduce_head( segmented_queue & right ) {
	// lock(); right.lock();
	if( !head ) {
	    head = right.head;
	    right.head = 0;
	} else {
	    assert( tail );
	    tail->set_next( right.head );
	    tail = right.tail;
	    right.head = 0;
	    right.tail = 0;
	}
#if 0
	if( !head ) {
	    // assert( tail == 0 && "tail must be nil when head is nil" );
	    if( tail ) {
		tail->set_next( right.head );
		tail = right.tail;
	    } else
		head = right.head;
	    right.head = 0;
	} else {
	    // When merging a new head, the previous contending tasks must
	    // have finished completely, and therefore they must have reduced
	    // the tail pointer also.
	    assert( tail != 0 && "invalid tail in reduce_head" );
	    tail->set_next( right.head );
	    tail = right.tail;
	    right.head = 0;
	}
#endif
	// unlock(); right.unlock();
    }

    void reduce_tail( segmented_queue & right ) {
	// Reduce fresh pop-user into parent's children when tail != 0
	tail->set_next( right.head );
	tail = 0;
	right.head = 0;
#if 0
	// When head is NULL on tail reduction, it means the head was
	// reduced upwards of the current frame. We expect the same to
	// happen for the reduction of the tail.
	if( !tail ) {
	    tail = right.tail;
	    right.tail = 0;
	} else {
	    assert( head != 0 && "head must be non-nil when tail is non-nil" );
	    right.tail = 0;
	}
#endif
    }

    void take_head( segmented_queue & from ) {
	head = from.head;
	from.head = 0;
    }

    void take_tail( segmented_queue & from ) {
	tail = from.tail;
	from.tail = 0;
    }

    segmented_queue & full_reduce( segmented_queue & right ) {
/*
	if( !tail ) {
	    assert( !head );
	    *this = right;
	} else if( right.head ) {
	    // assert( right.tail );
	    tail->set_next( right.head );
	    tail = right.tail; // may be 0
	    right.head = 0;
	    right.tail = 0;
	}
	return *this;
*/
	return merge_reduce( right );
    }

    segmented_queue & merge_reduce( segmented_queue & right ) {
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
 
    // Check if queue is owned locally (head and tail != 0). Deallocate all
    // empty/non-producing segments.
    void cleanup() {
	// TODO
    }

public:
    // Always retain at least one segment in the queue, because we own
    // the head, but we do not always own the tail, so we cannot change the
    // tail.
    void pop_segment() {
	queue_segment * seg = head->get_next();
	if( seg ) {
	    delete head;
	    head = seg;
	}
    }
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
    bool empty() const volatile {
	return !head
	    || ( head->is_empty() && !head->is_producing() && head == tail );
    }

    void* pop() {
	do {
	    while( !head || ( head->is_empty() && head->is_producing() ) ) {
		// busy wait
	    }
	    if( !head->is_empty() ) {
		void * value = head->pop();
		/*
		*/
		errs() << "pop from queue " << head << ": value="
		       << std::dec << *(int*)value << ' ' << *head << "\n";
		assert( value );
		return value;
	    }
	    if( !head->is_producing() ) {
		if( head->get_next() ) {
		    pop_segment(); 
		} else
		    break; 
	    }
	} while( true );
	errs() << "No more data for consumer!!! head = "<< head
	       << " tail=" << tail
	       << ' ' << *head <<"\n";
	errs() << "newline\n";
	return 0;
    }
	
    void push( void * value, const q_typeinfo & tinfo ) {
	if( !tail || tail->is_full() )
	    push_segment( tinfo );
	errs() << "push on queue segment " << tail << "\n";
	tail->push( value );
    }
};

inline std::ostream &
operator << ( std::ostream & os, const segmented_queue & seg ) {
    return os << '{' << seg.get_head() << ", " << seg.get_tail() << '}';
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

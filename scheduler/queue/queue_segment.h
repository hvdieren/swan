// -*- c++ -*-

#ifndef QUEUE_QUEUE_SEGMENT_H
#define QUEUE_QUEUE_SEGMENT_H

#include <stdio.h>
#include <iostream>
#include "swan/queue/typeinfo.h"
#include "swan/queue/fixed_size_queue.h"

namespace obj {

class queue_segment
{
    fixed_size_queue q;
    queue_segment * next;
    volatile bool producing;

    // Pad to 16 bytes because this should suite all data types.
    // There is no guarantee to naturally align any element store in the queue,
    // but 16 bytes should be good performance-wise for nearly all data types.
    pad_multiple<16, sizeof(fixed_size_queue)
		 + sizeof(queue_segment *)
		 + sizeof(volatile bool) > padding;

private:
    queue_segment( q_typeinfo tinfo, char * buffer )
	: q( tinfo, buffer ), next( 0 ), producing( false ) {
	static_assert( sizeof(queue_segment) % 16 == 0, "padding failed" );
    }
public:
    ~queue_segment() {
	// errs() << "queue_segment destruct: " << this << "\n";
	// errs() << "newline\n";
    }
	
public:
    // Allocate control fields and data buffer in one go
    static queue_segment * create( q_typeinfo tinfo ) {
	size_t buffer_size = fixed_size_queue::get_buffer_space( tinfo );
	char * memory = new char [sizeof(queue_segment) + buffer_size];
	char * buffer = &memory[sizeof(queue_segment)];
	return new (memory) queue_segment( tinfo, buffer );
    }

    // Accessor functions for control (not exposed to user API)
    bool is_full()  const volatile { return q.full(); }
    bool is_empty() const volatile { return q.empty(); }
    bool is_producing()  const volatile { return producing && !next; } // !!!
    void set_producing( bool p = true ) volatile { producing = p; }
    void clr_producing() volatile { producing = false; }
	
    // Linking segments in a list
    queue_segment * get_next() const { return next; }
    void set_next( queue_segment * next_ ) { next = next_; }
	
    // Queue pop and push methods
    template<typename T>
    void pop( T & t ) {
	while( q.empty() )
	    sched_yield();
	q.pop( t  );
    }
	
    void push( void * value ) {
	while( !q.push( value ) )
	    sched_yield();
    }

    friend std::ostream & operator << ( std::ostream & os, queue_segment & seg );
};

inline std::ostream & operator << ( std::ostream & os, queue_segment & seg ) {
    return os << "Segment: @" << &seg << " producing=" << seg.producing
	      << " next=" << seg.next << ' ' << seg.q;
}

}//namespace obj

#endif // QUEUE_QUEUE_SEGMENT_H

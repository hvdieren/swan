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
    queue_segment *next;
    q_typeinfo tinfo;
    volatile bool busy;
    pad_multiple<64, sizeof(fixed_size_queue)+sizeof(volatile bool) + sizeof(queue_segment*) + sizeof(q_typeinfo)> padding;
public:
    queue_segment(q_typeinfo tinfo_):q(tinfo_), tinfo(tinfo_) {
	static_assert( sizeof(queue_segment) % 64 == 0, "padding failed" );
	next = NULL;
	busy = false;
    }
	
    ~queue_segment() { 
	static_assert( sizeof(queue_segment) % 64 == 0, "padding failed" );
	next = NULL;
    }
	
    inline void * operator new ( size_t size );
    inline void operator delete( void * p );
	
    bool isFull()	volatile  { return q.full(); }
    bool isempty() volatile	  { return q.empty(); }
    bool isBusy() volatile	  { return busy; }
    void setBusy(bool busy_ = true) volatile { busy = busy_; }
    void clearBusy() volatile { busy = false; }
	
    queue_segment *getNext() {
	return next;
    }
	
    void setNext(queue_segment *next_) { 
	this->next = next_; 
    }
	
    void* conc_pop() {
	while( q.empty() ) {
	    sched_yield();
	}
	char *pop_value = q.pop();
	assert( pop_value && "pop failed" );
	return pop_value;
    }
	
    void conc_push(void* value) {
	while( !q.push( value ) )
	    sched_yield();
    }
	
    void push(void* value)	{ q.push(value); }
};

namespace queue_segment_allocator_ns {
typedef alc::mmap_alloc_policy<queue_segment, sizeof(queue_segment)> mmap_align_pol;
typedef alc::freelist_alloc_policy<queue_segment, mmap_align_pol, 64> list_align_pol;
typedef alc::allocator<queue_segment, list_align_pol> alloc_type;
}

typedef queue_segment_allocator_ns::alloc_type queue_segment_allocator;

extern __thread queue_segment_allocator * qs_allocator;
void * queue_segment::operator new ( size_t size ) {
    if( !qs_allocator )
	qs_allocator = new queue_segment_allocator();
    return qs_allocator->allocate( 1 );
}
void queue_segment::operator delete( void * p ) {
    if( !qs_allocator ) // we may deallocate blocks allocated elsewhere
	qs_allocator = new queue_segment_allocator();
    return qs_allocator->deallocate( (queue_segment *)p, 1 );
}

}//namespace obj

#endif // QUEUE_QUEUE_SEGMENT_H

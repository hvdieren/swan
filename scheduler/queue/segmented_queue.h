// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include "swan/queue/queue_segment.h"
// #include "swan/object.h" // proxy for tickets.h

namespace obj {

class segmented_queue
{
    typedef tkt_metadata queue_tkt_metadata;
    queue_tkt_metadata metadata;
    typedef uint32_t ctr_t;
    ctr_t refcnt;
    ctr_t forpad;
    queue_segment *head;
    queue_segment *tail;
    q_typeinfo	   tinfo;
	
    pad_multiple<64, sizeof(tkt_metadata)+sizeof(uint32_t)+sizeof(uint32_t)
		 + 2*sizeof(queue_segment*) + sizeof(q_typeinfo)> padding;

public: 
    segmented_queue( q_typeinfo tinfo_ ) : refcnt( 1 ), tinfo( tinfo_ ) {
	static_assert( sizeof(segmented_queue) % 64 == 0, "padding failed");
	queue_segment *q = new queue_segment(tinfo);
	head = q;
	tail = q;
    }
    ~segmented_queue() {
	//errs()<<"destructing SEGM QUEUE!!!!\n";
	static_assert( sizeof(segmented_queue) % 64 == 0, "padding failed" );
	delete head;
	head = NULL;
	delete tail;
	tail = NULL;
    }
    inline void del_ref_delete()__attribute__((noinline));
    inline void * operator new ( size_t size );
    inline void operator delete( void * p );

    queue_segment * getTail() {	return tail; }
	
    void push_segment()	{
	queue_segment	*new_q = new queue_segment(tinfo);
	tail->setNext(new_q);
	tail = new_q;
    }
	
    queue_segment* privatize_segment() {
	// errs()<<"-------------------------------privatizing segment\n";
	if(tail->isBusy() || tail->isFull())
	    push_segment();
	tail->setBusy();
	return tail;
    }
	
    queue_segment* insert_segment(queue_segment* qs) {
	errs()<<"inserting segment--------------------------- function for recursion!\n";
	queue_segment	*new_q = new queue_segment(tinfo);
	if(qs == tail)	tail = new_q;
	new_q->setNext(qs->getNext());
	qs->setNext(new_q);
	qs->clearBusy();
	new_q->setBusy();
	return new_q;
    }
	
    void pop_segment() {
	if( head ) {
	    queue_segment * p = head->getNext();
	    delete head;
	    head = p;
	    if( !p )  tail = NULL;
	}
    }
	
    bool empty() {
	return (head->isempty() && (!head->isBusy())) && (head == tail);
    }

    void* pop() {
	do {
	    while( head->isempty() && head->isBusy() ) ;
	    if( !head->isempty() )	{
		return head->conc_pop();
	    }
	    if( !head->isBusy() ) {
		if( head->getNext() != NULL ) {
		    pop_segment(); 
		} else
		    break; 
	    }
	} while( true );
	errs()<<"No more data for consumer!!! head = "<<head<<"\n";
	return 0;
    }
	
    void push(void* value, queue_segment** qs) {
	if((*qs)->isFull()) {
	    queue_segment *new_q = new queue_segment(tinfo);
			
	    if(*qs == this->tail){	//pushing a segment
		errs()<<"inserting segment--------------------------- at the end!\n";
		tail->setNext(new_q);
		tail = new_q;
	    }
	    else {
		errs()<<"inserting segment---------------------------2\n";
		new_q->setNext((*qs)->getNext()); //inserting a segment
		(*qs)->setNext(new_q);
	    }
	    new_q->setBusy((*qs)->isBusy());
	    (*qs)->clearBusy();
	    (*qs) = new_q;
	}
	(*qs)->conc_push(value);
    }
	
    void add_ref() { 
	__sync_fetch_and_add( &(refcnt), 1 ); 
    } // atomic!
    void del_ref() {
	assert( refcnt > 0 );
	if(refcnt <= 0) {
	    errs()<<"Assertion failed! segmented queue refcnt = "<<refcnt<<"\n";
	    exit(1);
	}
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &(refcnt), -1 ) == 1 ) { // atomic!
	    del_ref_delete();
	}
    }
};

void segmented_queue::del_ref_delete() {
    errs()<<"deleting payload the right way!!!\n";
    delete this;
}

namespace segmented_queue_allocator_ns {
typedef alc::mmap_alloc_policy<segmented_queue, sizeof(segmented_queue)> mmap_align_pol;
typedef alc::freelist_alloc_policy<segmented_queue, mmap_align_pol, 64> list_align_pol;
typedef alc::allocator<segmented_queue, list_align_pol> alloc_type;
}
typedef segmented_queue_allocator_ns::alloc_type segmented_queue_allocator;

extern __thread segmented_queue_allocator * segm_queue_allocator;
void * segmented_queue::operator new ( size_t size ) {
    if( !segm_queue_allocator )
	segm_queue_allocator = new segmented_queue_allocator();
    return segm_queue_allocator->allocate( 1 );
}
void segmented_queue::operator delete( void * p ) {
    if( !segm_queue_allocator ) // we may deallocate blocks allocated elsewhere
	segm_queue_allocator = new segmented_queue_allocator();
    return segm_queue_allocator->deallocate( (segmented_queue *)p, 1 );
}

}//namespace obj

#endif // QUEUE_SEGMENTED_QUEUE_H

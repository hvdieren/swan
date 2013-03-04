// -*- c++ -*-

#ifndef QUEUE_SEGMENTED_QUEUE_H
#define QUEUE_SEGMENTED_QUEUE_H

#include "swan/queue/queue_segment.h"

namespace obj {

class segmented_queue
{
public:
    typedef uint32_t ctr_t;

private:
    q_typeinfo tinfo;
    ctr_t refcnt;
    queue_segment * head, * tail;
	
    pad_multiple<CACHE_ALIGNMENT, 2*sizeof(uint32_t)
		 + 2*sizeof(queue_segment*) + sizeof(q_typeinfo)> padding;

private:
    inline void del_ref_delete() __attribute__((noinline));

public: 
    segmented_queue( q_typeinfo tinfo_ ) : tinfo( tinfo_ ), refcnt( 1 ) {
	static_assert( sizeof(segmented_queue) % CACHE_ALIGNMENT == 0,
		       "padding failed");
	head = tail = queue_segment::create( tinfo );
    }
    ~segmented_queue() {
	for( queue_segment * q=head, * q_next; q != tail; q = q_next ) {
	    q_next = q->get_next();
	    delete q;
	}
    }

    inline void * operator new ( size_t size );
    inline void operator delete( void * p );

    queue_segment * get_tail() const { return tail; }
    queue_segment * get_head() const { return head; }
	
private: // TODO: Make private
public:
    void push_segment()	{
	queue_segment * seg = queue_segment::create( tinfo );
	tail->set_next( seg );
	tail = seg;
    }
	
    queue_segment * privatize_segment() {
	if( tail->is_producing() || tail->is_full() )
	    push_segment();
	tail->set_producing();
	return tail;
    }
	
    queue_segment * insert_segment( queue_segment * after ) {
	queue_segment * seg = queue_segment::create( tinfo );
	if( after == tail ) tail = seg;
	seg->set_next( after->get_next() );
	after->set_next( seg );
	// after->clear_producing();
	seg->set_producing();
	return seg;
    }
	
    void pop_segment() {
	if( head != tail ) { // Always retain at least one segment
	    queue_segment * p = head->get_next();
	    delete head;
	    head = p;
	}
    }
	
public:
    bool empty() const volatile {
	return head->is_empty() && !head->is_producing() && head == tail;
    }

    void* pop() {
	do {
	    while( head->is_empty() && head->is_producing() ) {
		// busy wait
	    }
	    if( !head->is_empty() ) {
		void * value = head->pop();
		/*
		errs() << "pop from queue " << head << ": value="
		       << std::dec << *(int*)value << ' ' << *head << "\n";
		*/
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
	return 0;
    }
	
    void push( void * value, queue_segment** qs ) {
	if((*qs)->is_full()) {
	    queue_segment *new_q = queue_segment::create( tinfo );
			
	    if(*qs == tail){	//pushing a segment
		errs()<<"inserting segment--------------------------- at the end!\n";
		tail->set_next(new_q);
		tail = new_q;
	    }
	    else {
		errs()<<"inserting segment---------------------------2\n";
		new_q->set_next((*qs)->get_next()); //inserting a segment
		(*qs)->set_next(new_q);
	    }
	    new_q->set_producing((*qs)->is_producing());
	    (*qs)->clear_producing();
	    (*qs) = new_q;
	}
	errs() << "push on queue " << *qs << "\n";
	/*
	errs() << "push on queue " << *qs << ": value=" << std::dec
	       << *(int*)value << ' ' << **qs << "\n";
	*/
	(*qs)->push(value);
    }
	
public:
    void add_ref() { __sync_fetch_and_add( &(refcnt), 1 ); } // atomic!
    void del_ref() {
	assert( refcnt > 0 );
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &(refcnt), -1 ) == 1 ) { // atomic!
	    del_ref_delete();
	}
    }
};

void segmented_queue::del_ref_delete() {
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

// -*- c++ -*-

#ifndef QUEUE_QUEUE_VERSION_H
#define QUEUE_QUEUE_VERSION_H

#include "swan/queue/segmented_queue.h"

namespace obj {

class parent;
class queue_version
{
    typedef tkt_metadata queue_tkt_metadata;
    queue_tkt_metadata	metadata;
    typedef uint32_t ctr_t;
    ctr_t refcnt;
    segmented_queue 	*payload;
    queue_segment	 	**private_queue_segment;
    q_typeinfo 			tinfo;
    parent *			instance;
	
    inline void del_ref_delete() __attribute__((noinline));
	
    pad_multiple<64, sizeof(queue_tkt_metadata)+sizeof(ctr_t)
		 +sizeof(parent*) + sizeof(segmented_queue*) + sizeof(queue_segment*) + sizeof(queue_segment**) + sizeof(q_typeinfo)> padding;

public:
    queue_version( q_typeinfo tinfo_, int dummy )
	: refcnt( 1 ), tinfo( tinfo_ ) {
	static_assert( sizeof(queue_version) % 64 == 0, "padding failed" );
	payload = NULL;
	private_queue_segment = NULL;
    }
	
    queue_version( q_typeinfo tinfo_, parent * obj_ )
	: refcnt( 1 ), tinfo( tinfo_ ) {
	static_assert( sizeof(queue_version) % 64 == 0, "padding failed" );
	payload = new segmented_queue(tinfo); 
	//payload->add_ref();
	private_queue_segment = new queue_segment *; // HV !?!?
	*private_queue_segment = NULL;
	instance = obj_;
    }
	
    queue_version( q_typeinfo tinfo_, queue_version * qv )
	: refcnt( 1 ), tinfo( tinfo_ ) {
	static_assert( sizeof(queue_version) % 64 == 0, "padding failed" );
		
	payload = qv->get_queue();
	payload->add_ref();
	metadata = *(qv->get_metadata());
	private_queue_segment = new queue_segment *; // HV !?!?
	instance = qv->get_instance();
    }
	
    ~queue_version() { 
	payload->del_ref(); 
    }
	
    void destruct() {
	if(private_queue_segment != NULL) {
	    free(*private_queue_segment);
	    *private_queue_segment = NULL;
	}
	free(private_queue_segment);
	private_queue_segment = NULL;
	// tinfo.~q_typeinfo();
	// metadata.~queue_tkt_metadata();
	// delete payload;
	// payload = NULL;
    }
	
    inline void * operator new ( size_t size );
    inline void operator delete( void * p );
    queue_tkt_metadata* get_metadata() {
	return &metadata;
    }
    ctr_t get_refcnt() {
	return refcnt;
    }
    segmented_queue* get_queue() {
	return this->payload;
    }

    parent* get_instance() {
	return instance;
    }
	
    void set_instance(parent* p) {
	instance = p;
    }
	
    void* pop()	{
	return payload->pop();
    }
	
    void set_private_queue_segment(queue_segment *qs) {
	*private_queue_segment = qs;
    }
	
    queue_segment** get_private_queue_segment()	{
	return private_queue_segment;
    }
	
    queue_segment * get_tail() {
	return payload->getTail();
    }
	
    queue_segment* privatize_segment() {
	if((*private_queue_segment) == NULL) {
	    //new producer, privatize a segment at the end of the queue
	    return payload->privatize_segment();
	} else {
	    //insert a segment AFTER qs because we belong to the same producer
	    return payload->insert_segment(*private_queue_segment);	
	}
    }

    q_typeinfo get_tinfo() { return tinfo; }
	
    bool empty() { return this->payload->empty(); }
    void add_reader()   { metadata.add_reader(); }
    void add_ref() { 
	__sync_fetch_and_add( &(refcnt), 1 ); 
	payload->add_ref();
    } // atomic!
	
    void del_ref() {
	assert( refcnt > 0 );
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &(refcnt), -1 ) == 1 ) { // atomic!
	    del_ref_delete();
	}
		
    }
    bool is_used_in_reduction() { return false; }
};

void queue_version::del_ref_delete() {
    delete this;
}

namespace queue_version_allocator_ns {
typedef alc::mmap_alloc_policy<queue_version, sizeof(queue_version)> mmap_align_pol;
typedef alc::freelist_alloc_policy<queue_version, mmap_align_pol, 64> list_align_pol;
typedef alc::allocator<queue_version, list_align_pol> alloc_type;
}

typedef queue_version_allocator_ns::alloc_type queue_version_allocator;
extern __thread queue_version_allocator * queue_v_allocator;
void * queue_version::operator new ( size_t size ) {
    if( !queue_v_allocator )
	queue_v_allocator = new queue_version_allocator();
    return queue_v_allocator->allocate( 1 );
}
void queue_version::operator delete( void * p ) {
    if( !queue_v_allocator ) // we may deallocate blocks allocated elsewhere
	queue_v_allocator = new queue_version_allocator();
    return queue_v_allocator->deallocate( (queue_version *)p, 1 );
}

} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

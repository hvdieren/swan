// -*- c++ -*-

#ifndef QUEUE_QUEUE_VERSION_H
#define QUEUE_QUEUE_VERSION_H

#include "swan/queue/segmented_queue.h"

namespace obj {

template<typename MetaData>
class queue_base;

template<typename MetaData>
class queue_version
{
public:
    typedef MetaData metadata_t;
    typedef uint32_t ctr_t;

private:
    metadata_t metadata;
    ctr_t refcnt; // TODO: Must disappear
    segmented_queue * payload;
    queue_segment * private_queue_segment;
    // TODO: SPECIALIZE case for queue_t and deps where queue_t holds all this
    // info and deps have only a pointer to it (for compactness).
    // This could include the segmented queue (or pointer to it) and the tinfo
    q_typeinfo tinfo;
    queue_base<metadata_t> * instance; // TODO: this must go

    // New fields
    // queue_version<MetaData> * chead, * ctail;
    // queue_version<MetaData> * left, * right;
    // queue_version<MetaData> * parent;
	
    pad_multiple<CACHE_ALIGNMENT, sizeof(metadata_t) + 2*sizeof(ctr_t)
		 + sizeof(segmented_queue*)
		 + sizeof(queue_segment*)
		 + sizeof(q_typeinfo)
		 + sizeof(queue_base<metadata_t>*) > padding;

private:
    inline void del_ref_delete() __attribute__((noinline));
	
private:
    // unused?
    queue_version( q_typeinfo tinfo_ )
	: refcnt( 1 ), payload( 0 ), private_queue_segment( 0 ),
	  tinfo( tinfo_ ) {
	static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       "padding failed" );
    }
	
private:
    // Normal constructor, called from queue_t constructor
    queue_version( q_typeinfo tinfo_, queue_base<metadata_t> * obj_ )
	: refcnt( 1 ), payload( new segmented_queue( tinfo_ ) ),
	  private_queue_segment( payload->get_tail() ),
	  tinfo( tinfo_ ), instance( obj_ ) {
	static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       "padding failed" );
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
    queue_version( queue_version<metadata_t> * qv,
		   queue_base<metadata_t> * inst )
	: refcnt( 1 ), payload( qv->payload ),
	  private_queue_segment( qv->private_queue_segment ),
	  tinfo( qv->tinfo ), instance( inst ) {
	static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       "padding failed" );
	payload->add_ref();
    }
	
public:
    // Normal queue_version creation method for instantiating a fresh queue_t
    template<typename T>
    static queue_version<metadata_t> * create( queue_base<metadata_t> * obj_ ) {
	return new queue_version<metadata_t>( q_typeinfo::create<T>(), obj_ );
    }

    // Passing a queue as an argument to a function.
    // Installs a new version of an existing queue, utilizing the same
    // segment, or pushes a new segment.
    static queue_version<metadata_t> * nest( queue_version<metadata_t> * v_old,
					     queue_base<metadata_t> * obj ) {
	queue_version<metadata_t> * v_new = new queue_version( v_old, obj );
	v_old->del_ref(); // Old version now detached from its instance
	v_new->instance->set_version( v_new ); // New version attached
	v_new->private_queue_segment = v_new->payload->insert_segment( v_old->private_queue_segment );
	errs() << "Queue Nesting: old_v=" << v_old
	       << " new_v=" << v_new
	       << " old seg=" << v_old->private_queue_segment
	       << " new seg=" << v_new->private_queue_segment
	       << " old next=" << v_old->private_queue_segment->get_next()
	       << " new next=" << v_new->private_queue_segment->get_next()
	       << " instance=" << v_new->instance
	       << " visible version=" << *(queue_version<metadata_t>**)(v_new->instance)
	       << "...\n";
	errs() << "Segment list:";
	for( queue_segment * s = v_new->payload->get_head(); s; s=s->get_next() )
	    errs() << " " << s;
	errs() << "\n";
	return v_new;
    }

    ~queue_version() { 
	payload->del_ref(); 
    }
	
    const metadata_t * get_metadata() const { return &metadata; }
    metadata_t * get_metadata() { return &metadata; }

    template<typename T>
    void pop( T & t ) {
	void * ptr = payload->pop();
	t = *reinterpret_cast<T *>( ptr );
    }
    template<typename T>
    void push( T t ) {
	payload->push( reinterpret_cast<void *>( &t ), &private_queue_segment );
    }
    // template<typename T>
    // void push( T & t ) {
	// payload->push( reinterpret_cast<void *>( &t ), &private_queue_segment );
    // }
	
    // DO WE REALLY NEED THE FOLLOWING INTERFACE?
    segmented_queue * get_queue() {
	return this->payload;
    }

    queue_base<metadata_t>* get_instance() {
	return instance;
    }
	
    void set_instance(queue_base<metadata_t>* p) {
	instance = p;
    }
	
    void set_private_queue_segment(queue_segment *qs) {
	private_queue_segment = qs;
    }
	
    queue_segment** get_private_queue_segment_ptr()	{
	return &private_queue_segment;
    }
    queue_segment* get_private_queue_segment()	{
	return private_queue_segment;
    }
	
    queue_segment * get_tail() {
	return payload->get_tail();
    }
	
    queue_segment* privatize_segment() {
	if(private_queue_segment == NULL) {
	    //new producer, privatize a segment at the end of the queue
	    return payload->privatize_segment();
	} else {
	    //insert a segment AFTER qs because we belong to the same producer
	    return payload->insert_segment(private_queue_segment);	
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
/* HV: TODO
	assert( refcnt > 0 );
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &(refcnt), -1 ) == 1 ) { // atomic!
	    del_ref_delete();
	}
*/
		
    }
    bool is_used_in_reduction() { return false; }
};

template<typename MetaData>
void queue_version<MetaData>::del_ref_delete() {
    delete this;
}

} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

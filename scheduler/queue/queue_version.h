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

private:
    metadata_t metadata;
    segmented_queue * payload;
    queue_segment * private_queue_segment;
    // TODO: SPECIALIZE case for queue_t and deps where queue_t holds all this
    // info and deps have only a pointer to it (for compactness).
    // This could include the segmented queue (or pointer to it) and the tinfo
    q_typeinfo tinfo;

    // New fields
    queue_version<MetaData> * chead, * ctail;
    queue_version<MetaData> * left, * right;
    queue_version<MetaData> * parent;
    cas_mutex mutex;
	
    pad_multiple<CACHE_ALIGNMENT, sizeof(metadata_t)
		 + sizeof(segmented_queue*)
		 + sizeof(queue_segment*)
		 + sizeof(q_typeinfo)
		 + 5*sizeof(queue_version<metadata_t>*)
		 + sizeof(cas_mutex)> padding;


    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

protected:
    // Normal constructor, called from queue_t constructor
    queue_version( q_typeinfo tinfo_ )
	: payload( new segmented_queue( tinfo_ ) ),
	  private_queue_segment( payload->get_tail() ),
	  tinfo( tinfo_ ),
	  chead( 0 ), ctail( 0 ), left( 0 ), right( 0 ), parent( 0 ) {
	static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       "padding failed" );

	errs() << "QV queue_t constructor for: " << this << "\n";
    }
	
    // Argument passing constructor, called from grab/dgrab interface.
    // Does a shallow copy
public:
    // This code is written with pushdep in mind; may need to be specialized
    // to popdep
    queue_version( queue_version<metadata_t> * qv )
	: payload( qv->payload ),
	  private_queue_segment( qv->private_queue_segment ),
	  tinfo( qv->tinfo ), chead( 0 ), ctail( 0 ), right( 0 ), parent( qv ) {
	static_assert( sizeof(queue_version) % CACHE_ALIGNMENT == 0,
		       "padding failed" );

	// Link in frame with siblings and parent
	parent->lock();
	lock();
	if( parent->ctail ) {
	    parent->ctail->lock();
	    left = parent->ctail;
	    left->right = this;
	    parent->ctail = this;
	    left->unlock();
	} else {
	    left = 0;
	    parent->chead = parent->ctail = this;
	}
	unlock();
	parent->unlock();

	errs() << "QV nest constructor for: " << this << "\n";
    }

private:
    void unlink() {
	if( !parent )
	    return;

	// TODO: Need to randomize lock order! (see Cilk++ hyperobjects paper)
	parent->lock();
	if( left )
	    left->lock();
	if( right )
	    right->lock();

	if( left )
	    left->right = right;
	else
	    parent->chead = right;

	if( right )
	    right->left = left;
	else
	    parent->ctail = left;

	parent->unlock();
	if( left )
	    left->unlock();
	if( right )
	    right->unlock();
    }

public:
    ~queue_version() {
	errs() << "QV destructor for: " << this << "\n";
	unlink();
    }
	
public:
#if 0
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
	v_new->private_queue_segment = v_new->payload->insert_segment( v_old->private_queue_segment );
	errs() << "Queue Nesting: old_v=" << v_old
	       << " new_v=" << v_new
	       << " old seg=" << v_old->private_queue_segment
	       << " new seg=" << v_new->private_queue_segment
	       << " old next=" << v_old->private_queue_segment->get_next()
	       << " new next=" << v_new->private_queue_segment->get_next()
	       << "...\n";
	errs() << "Segment list:";
	for( queue_segment * s = v_new->payload->get_head(); s; s=s->get_next() )
	    errs() << " " << s;
	errs() << "\n";
	return v_new;
    }
#endif

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
    bool is_used_in_reduction() { return false; }
};

} //namespace obj

#endif // QUEUE_QUEUE_VERSION_H

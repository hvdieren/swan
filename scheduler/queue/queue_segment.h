// -*- c++ -*-

#ifndef QUEUE_QUEUE_SEGMENT_H
#define QUEUE_QUEUE_SEGMENT_H

#include <stdio.h>
#include <iostream>
#include "swan/queue/fixed_size_queue.h"

namespace obj {

class queue_segment
{
    fixed_size_queue q;
    queue_segment * next;
    queue_segment * child[2];
    short balance;
    volatile bool producing;
    bool copied_peek;

    // Pad to 16 bytes because this should suite all data types.
    // There is no guarantee to naturally align any element store in the queue,
    // but 16 bytes should be good performance-wise for nearly all data types.
    pad_multiple<16, sizeof(fixed_size_queue)
		 + 3*sizeof(queue_segment *)
		 + sizeof(short)
		 + sizeof(volatile bool)
		 + sizeof(bool) > padding;

    friend std::ostream & operator << ( std::ostream & os, const queue_segment & seg );

private:
    queue_segment( typeinfo_array tinfo, char * buffer,
		   size_t elm_size, size_t max_size, size_t peekoff_,
		   bool is_head )
	: q( tinfo, buffer, elm_size, max_size, peekoff_ ),
	  next( 0 ), producing( true ), copied_peek( is_head ) {
	static_assert( sizeof(queue_segment) % 16 == 0, "padding failed" );
	// errs() << "queue_segment create " << *this << std::endl;
    }

public:
    ~queue_segment() {
#if PROFILE_QUEUE
	get_profile_queue().num_segment_dealloc++;
#endif
    }

    // Allocate control fields and data buffer in one go
    template<typename T>
    static queue_segment * create( size_t seg_size, size_t peekoff, bool is_head ) {
	typeinfo_array tinfo = typeinfo_array::create<T>();
	size_t buffer_size = fixed_size_queue::get_buffer_space<T>( seg_size );
	char * memory = new char [sizeof(queue_segment) + buffer_size];
	char * buffer = &memory[sizeof(queue_segment)];
	size_t step = fixed_size_queue::get_element_size<T>();
	tinfo.construct<T>( buffer, &buffer[buffer_size], step );
#if PROFILE_QUEUE
	get_profile_queue().num_segment_alloc++;
#endif
	return new (memory) queue_segment( tinfo, buffer, step,
					   seg_size, peekoff, is_head );
    }

    void erase_all() {
	for( queue_segment * q=this, * q_next; q; q = q_next ) {
	    q_next = q->get_next();
	    delete q;
	}
    }

    // Accessor functions for control (not exposed to user API)
    bool is_full()  const volatile { return q.full(); }
    // bool is_empty() const volatile { return q.empty(); }
    bool is_producing()  const volatile { return !copied_peek || ( producing && !next ); } // !!!
    void set_producing( bool p = true ) volatile { producing = p; }
    void clr_producing() volatile { producing = false; }

    size_t get_peek_dist() const { return q.get_peek_dist(); }

    // Linking segments in a list
    queue_segment * get_next() const { return next; }

    void set_next( queue_segment * next_ ) {
	// We have assured that we will not wrap-around when peekoff != 0
	next_->q.copy_peeked( q.get_peek_suffix() );
	next_->copied_peek = true;
	// TODO: How to avoid memory de-allocation here?

	next = next_;
    }

    bool is_empty() const { return q.empty(); }

    void rewind() { q.rewind(); }

    // Queue pop and push methods
    void pop_bookkeeping( size_t npop ) {
	q.pop_bookkeeping( npop );
    }

    void push_bookkeeping( size_t npush ) {
	q.push_bookkeeping( npush );
    }

    bool has_space( size_t length ) const {
	return q.has_space( length );
    }
	
    template<typename T>
    T && pop() {
#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	while( q.empty() )
	    sched_yield();
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	return q.pop<T>();
    }

    template<typename T>
    T & peek( size_t off ) {
#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().qs_peek );
#endif // PROFILE_QUEUE
	while( q.empty() )
	    sched_yield();
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().qs_peek );
#endif // PROFILE_QUEUE

	return q.peek<T>( off );
    }
	
    template<typename T>
    void push( T && value ) {
	while( q.full() )
	    sched_yield();
	q.push( std::move( value ) );
    }

    template<typename T>
    void push( const T & value ) {
	while( q.full() )
	    sched_yield();
	q.push( value );
    }

    template<typename MetaData, typename T>
    write_slice<MetaData,T> get_write_slice( size_t length ) {
	return q.get_write_slice<MetaData,T>( length );
    }

    template<typename MetaData,typename T>
    read_slice<MetaData,T> get_read_slice( size_t npop ) {
	return q.get_read_slice<MetaData,T>( npop );
    }

private:
};

inline std::ostream &
operator << ( std::ostream & os, const queue_segment & seg ) {
    return os << "Segment: @" << &seg
	      << " producing=" << seg.producing
	      << " next=" << seg.next
	      << " child=" << seg.child[0] << "," << seg.child[1]
	      << " B=" << seg.balance
	      << ' ' << seg.q;
}

} // end namespace obj

#endif // QUEUE_QUEUE_SEGMENT_H

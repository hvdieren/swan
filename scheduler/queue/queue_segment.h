// -*- c++ -*-

#ifndef QUEUE_QUEUE_SEGMENT_H
#define QUEUE_QUEUE_SEGMENT_H

#include <stdio.h>
#include <iostream>
#include "swan/queue/fixed_size_queue.h"

#define DBG_ALLOC 0

namespace obj {

class queue_segment
{
    fixed_size_queue q;
    size_t volume_pop, volume_push;
#if DBG_ALLOC
    long hash;
#endif
    size_t dflag;
    size_t seqno;
    queue_segment * next;
    queue_segment * child[2];
    short balance;
    volatile bool producing;
    bool copied_peek;
    cas_mutex mux;

    // Pad to 16 bytes because this should suite all data types.
    // There is no guarantee to naturally align any element store in the queue,
    // but 16 bytes should be good performance-wise for nearly all data types.
    pad_multiple<16, sizeof(fixed_size_queue)
		 + sizeof(long)
		 + 3*sizeof(size_t)
#if DBG_ALLOC
		 + sizeof(long)
#endif
		 + sizeof(size_t)
		 + 3*sizeof(queue_segment *)
		 + sizeof(short)
		 + sizeof(bool)
		 + sizeof(volatile bool)
		 + sizeof(int)
		 + sizeof(cas_mutex) > padding;

    friend std::ostream & operator << ( std::ostream & os, const queue_segment & seg );

private:
    queue_segment( typeinfo_array tinfo, char * buffer,
		   size_t elm_size, size_t max_size, size_t peekoff_,
		   size_t seqno_, bool is_head )
	: q( tinfo, buffer, elm_size, max_size, peekoff_ ),
	  volume_pop( 0 ), volume_push( peekoff_ ),
	  dflag( 0 ),
	  seqno( seqno_ ),
	  next( 0 ), producing( true ), copied_peek( is_head ) {
#if DBG_ALLOC
	hash = 0xbebebebe;
#endif
	/// static_assert( sizeof(queue_segment) % 16 == 0, "padding failed" );
	// errs() << "queue_segment create " << *this << std::endl;
    }
private:
    ~queue_segment() { }
public:
    void lock() { mux.lock(); }
    void unlock() { mux.unlock(); }

    void check_hash() const {
#if DBG_ALLOC
	assert( hash == 0xbebebebe );
#endif
	assert( !dflag );
    }

    static void deallocate( queue_segment * seg ) {
	// errs() << "deallocate " << *seg << " by " << d << std::endl;
#if DBG_ALLOC
#endif
	if( __sync_fetch_and_add( &seg->dflag, 1 ) == 0 ) {
	    assert( seg->dflag == 1 );
#if DBG_ALLOC
	    assert( seg->hash == 0xbebebebe );
	    seg->hash = 0xdeadbeef;
#endif
	    if( 1 ) // set to 0 to avoid memory reuse (debugging)
		delete seg;
	}
    }
	
public:
    // Allocate control fields and data buffer in one go
    template<typename T>
    static queue_segment * create( size_t seg_size, size_t peekoff, size_t seqno, bool is_head ) {
	typeinfo_array tinfo = typeinfo_array::create<T>();
	size_t buffer_size = fixed_size_queue::get_buffer_space<T>( seg_size );
	char * memory = new char [sizeof(queue_segment) + buffer_size];
	char * buffer = &memory[sizeof(queue_segment)];
	size_t step = fixed_size_queue::get_element_size<T>();
	tinfo.construct<T>( buffer, &buffer[buffer_size], step );
	return new (memory) queue_segment( tinfo, buffer, step,
					   seg_size, peekoff, seqno, is_head );
	return (queue_segment*)memory;
    }

    // Accessor functions for control (not exposed to user API)
    bool is_full()  const volatile { return q.full(); }
    // bool is_empty() const volatile { return q.empty(); }
    bool is_producing()  const volatile { return !copied_peek || ( producing && !next ); } // !!!
    void set_producing( bool p = true ) volatile { producing = p; }
    void clr_producing() volatile { producing = false; }

    size_t get_peek_dist() const { return q.get_peek_dist(); }

    // size_t get_volume_pop() const { return volume_pop; }
    // size_t get_volume_push() const { return volume_push; }
    bool all_done() const {
	check_hash();
	return copied_peek && volume_pop + q.get_peek_dist() == volume_push;
    }

    // Linking segments in a list
    queue_segment * get_next() const { return next; }

    void set_next( queue_segment * next_ ) {
	// We have assured that we will not wrap-around when peekoff != 0
	next_->q.copy_peeked( q.get_peek_suffix() );
	next_->copied_peek = true;
	// TODO: How to avoid memory de-allocation here?

	assert( next_->volume_pop + ( next_->copied_peek ? next_->q.get_peek_dist() : 0 ) <= next_->volume_push );
	// assert( volume_push > q.get_peek_dist() );

	next = next_;
    }

/* UNUSED
    void advance_to_end( size_t length ) { 
	// Some pops did not get done. Tamper with the counters such
	// that it appears as if we did...
	if( length > 0 ) {
	    volume_pop += length+1;
	    if( volume_push < volume_pop ) {
		assert( !is_producing() );
		volume_push = volume_pop;
	    }
	}
    }
*/

    bool is_empty() const { return q.empty(); }

    void rewind() { q.rewind(); volume_push = 0; } // very first segment has no copied-in peek area

    // Queue pop and push methods
    void pop_bookkeeping( size_t npop ) {
	check_hash();
	__sync_fetch_and_add( &volume_pop, npop );
	assert( volume_pop <= volume_push );
    }

    void push_bookkeeping( size_t npush ) {
	check_hash();
	q.push_bookkeeping( npush );
	volume_push += npush;
    }

    bool has_space( size_t length ) const {
	return q.has_space( length );
    }
	
    template<typename T>
    T & pop() {
#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	while( q.empty() )
	    sched_yield();
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	// Translate global position we're popping from to local queue position
	// Two behaviors of the fixed_size_queue:
	// * As a real queue, round-robin when used by one popper
	// * As an array, when concurrent pops occur
	T & r = q.pop<T>();
	__sync_fetch_and_add( &volume_pop, 1 );
	return r;
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
    void push( const T * value ) {
	while( !q.push( value ) )
	    sched_yield();
	volume_push++;
    }

    template<typename MetaData, typename T>
    write_slice<MetaData,T> get_write_slice( size_t length ) {
	return q.get_write_slice<MetaData,T>( length );
    }

    template<typename MetaData,typename T>
    read_slice<MetaData,T> get_slice( size_t npop ) {
	return q.get_slice<MetaData,T>( npop );
    }

private:
};

inline std::ostream &
operator << ( std::ostream & os, const queue_segment & seg ) {
    return os << "Segment: @" << &seg
	      << " volume-pop=" << seg.volume_pop
	      << " volume-push=" << seg.volume_push
	      << " producing=" << seg.producing
	      << " next=" << seg.next
	      << " child=" << seg.child[0] << "," << seg.child[1]
	      << " B=" << seg.balance
	      << " seqno=" << seg.seqno
	      << ' ' << seg.q;
}

} // end namespace obj

#endif // QUEUE_QUEUE_SEGMENT_H

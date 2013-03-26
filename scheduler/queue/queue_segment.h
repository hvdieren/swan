// -*- c++ -*-

#ifndef QUEUE_QUEUE_SEGMENT_H
#define QUEUE_QUEUE_SEGMENT_H

#include <stdio.h>
#include <iostream>
#include "swan/queue/typeinfo.h"
#include "swan/queue/fixed_size_queue.h"

namespace obj {

// NOTE:
// If pushes occur concurrently on the same queue, then each will operate on
// a distinct queue_segment. Pops may occur concurrently on the same segment.
class queue_segment
{
    fixed_size_queue q;
    queue_segment * next;
    long logical_pos; // -1 is unknown
    size_t volume_pop, volume_push;
    int slot;
    volatile bool producing;

    // Pad to 16 bytes because this should suite all data types.
    // There is no guarantee to naturally align any element store in the queue,
    // but 16 bytes should be good performance-wise for nearly all data types.
    pad_multiple<16, sizeof(fixed_size_queue)
		 + sizeof(queue_segment *)
		 + sizeof(long)
		 + sizeof(size_t)
		 + sizeof(size_t)
		 + sizeof(int)
		 + sizeof(volatile bool) > padding;

private:
    queue_segment( typeinfo tinfo, long logical_, char * buffer,
		   size_t elm_size, size_t max_size )
	: q( tinfo, buffer, elm_size, max_size ), next( 0 ), logical_pos( logical_ ),
	  volume_pop( 0 ), volume_push( 0 ),
	  slot( -1 ), producing( true ) {
	static_assert( sizeof(queue_segment) % 16 == 0, "padding failed" );
	errs() << "queue_segment create " << *this << "\n";
    }
public:
    ~queue_segment() {
	errs() << "queue_segment destruct: " << *this << "\n";
	errs() << "newline\n";
	assert( slot < 0 && "queue_segment slotted when destructed" );
	assert( logical_pos >= 0 && "logical position unknown when destructed" );
    }
	
public:
    // Allocate control fields and data buffer in one go
    template<typename T>
    static queue_segment * create( long logical, size_t seg_size ) {
	typeinfo tinfo = typeinfo::create<T>();
	size_t buffer_size = fixed_size_queue::get_buffer_space<T>( seg_size );
	char * memory = new char [sizeof(queue_segment) + buffer_size];
	char * buffer = &memory[sizeof(queue_segment)];
	size_t step = fixed_size_queue::get_element_size<T>();
	for( char * p=buffer; p < &memory[buffer_size]; p += step ) {
	    T * tp = reinterpret_cast<T *>( p );
	    new (tp) T;
	}
	return new (memory) queue_segment( tinfo, logical, buffer,
					   fixed_size_queue::get_element_size<T>(),
					   seg_size );
    }

    // Accessor functions for control (not exposed to user API)
    bool is_full()  const volatile { return q.full(); }
    // bool is_empty() const volatile { return q.empty(); }
    bool is_producing()  const volatile { return producing && !next; } // !!!
    void set_producing( bool p = true ) volatile { producing = p; }
    void clr_producing() volatile { producing = false; }

    int  get_slot() const { return slot; }
    void set_slot( int slot_ ) { slot = slot_; }
    long get_logical_head() const {
	return logical_pos < 0 ? -1 : logical_pos + volume_pop;
    }
    // void set_logical_head( int logical_ ) { logical_head = logical_; }
    long get_logical_tail() const {
	return logical_pos < 0 ? -1 : logical_pos + volume_push;
    }
    long get_logical_pos() const { return logical_pos; }
    void set_logical_pos( int logical_ ) {
	errs() << "Update logical position of " << this
	       << " from " << logical_pos << " to " << logical_ << "\n";
	logical_pos = logical_;
    }

    size_t get_volume_pop() const { return volume_pop; }
    size_t get_volume_push() const { return volume_push; }

    // Linking segments in a list
    queue_segment * get_next() const { return next; }
    void set_next( queue_segment * next_ ) { next = next_; }
	
    bool is_empty( size_t logical ) const {
	return !q.is_produced( logical - logical_pos );
    }

    // Queue pop and push methods
    template<typename T>
    void pop( T & t, long logical ) {
	while( q.empty() )
	    sched_yield();
	// Translate global position we're popping from to local queue position
	// Two behaviors of the fixed_size_queue:
	// * As a real queue, round-robin when used by one popper
	// * As an array, when concurrent pops occur
	errs() << "queue_segment: pop @" << logical << " seg=" << *this << "\n";
	q.pop( t, logical - logical_pos );
	__sync_fetch_and_add( &volume_pop, 1 );
    }
	
    template<typename T>
    void push( T * value ) {
	while( !q.push( value ) )
	    sched_yield();
	volume_push++;
    }

    friend std::ostream & operator << ( std::ostream & os, queue_segment & seg );
};

inline std::ostream & operator << ( std::ostream & os, queue_segment & seg ) {
    return os << "Segment: @" << &seg << " producing=" << seg.producing
	      << " slot=" << seg.slot
	      << " @" << seg.logical_pos
	      << " volume-pop=" << seg.volume_pop
	      << " volume-push=" << seg.volume_push
	      << " next=" << seg.next << ' ' << seg.q;
}

}//namespace obj

#endif // QUEUE_QUEUE_SEGMENT_H

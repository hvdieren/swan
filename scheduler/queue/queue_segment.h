// -*- c++ -*-

#ifndef QUEUE_QUEUE_SEGMENT_H
#define QUEUE_QUEUE_SEGMENT_H

#include <stdio.h>
#include <iostream>
#include "swan/queue/fixed_size_queue.h"
#include "swan/queue/avl.h"

#define DBG_ALLOC 0

namespace obj {

class queue_segment;

struct queue_key_t {
    size_t logical;
    size_t seqno;
};

class queue_index {
    avl_tree<queue_segment, queue_key_t> idx;
    long head_pop, tail_pop;
    size_t head_seqno, tail_seqno;
    size_t the_end;
    cas_mutex mutex;

private:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

public:
    queue_index() : head_pop( 0 ), tail_pop( 0 ), head_seqno( 0 ), tail_seqno( 0 ),
		    the_end( ~0 ) { }

public:
    void insert( queue_segment * seg );
    queue_segment * lookup( size_t logical, size_t push_seqno );
    void erase( queue_segment * seg );

    size_t get_head_pop() const { return head_pop; }

    void pop1( size_t num ) {
	lock();
	head_pop += num;
	unlock();
    }
    void pop( size_t num ) {
	lock();
	head_pop += num;
	head_seqno++;
	if( head_seqno == tail_seqno ) {
	    assert( tail_pop == head_pop || tail_pop < 0 );
	    tail_pop = head_pop;
	}
	unlock();
    }
    void prefix( long num ) {
	lock();
	if( num < 0 )
	    tail_pop = -1;
	else
	    tail_pop += num;
	tail_seqno++;
	unlock();
    }

    void set_end( size_t ending ) {
	if( the_end == size_t(~0) || the_end < ending )
	    the_end = ending;
    }
    void unset_end() { the_end = ~0; }
    size_t get_end() const { return the_end; }
};

// NOTE:
// If pushes occur concurrently on the same queue, then each will operate on
// a distinct queue_segment. Pops may occur concurrently on the same segment.
class segmented_queue_base;

class queue_segment
{
    fixed_size_queue q;
    long logical_pos; // -1 is unknown
    size_t volume_pop, volume_push;
#if DBG_ALLOC
    long hash;
    segmented_queue_base * dseg;
#endif
    size_t dflag;
    size_t seqno;
    queue_segment * next;
    queue_segment * child[2];
    short balance;
    volatile bool producing;
    bool copied_peek;
    cas_mutex mux;

    friend struct avl_traits<queue_segment, queue_key_t>;

    // Pad to 16 bytes because this should suite all data types.
    // There is no guarantee to naturally align any element store in the queue,
    // but 16 bytes should be good performance-wise for nearly all data types.
    pad_multiple<16, sizeof(fixed_size_queue)
		 + sizeof(long)
		 + 3*sizeof(size_t)
#if DBG_ALLOC
		 + sizeof(long)
		 + sizeof(segmented_queue_base *)
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
    queue_segment( typeinfo_array tinfo, long logical_, char * buffer,
		   size_t elm_size, size_t max_size, size_t peekoff_,
		   size_t seqno_ )
	: q( tinfo, buffer, elm_size, max_size, peekoff_ ),
	  // If logical known and not first buffer, then subtract peek length
	  logical_pos( logical_ ),
	  volume_pop( 0 ), volume_push( peekoff_ ),
#if DBG_ALLOC
	  dseg( 0 ),
#endif
	  dflag( 0 ),
	  seqno( seqno_ ),
	  next( 0 ), producing( true ), copied_peek( logical_pos == 0 ) {
#if DBG_ALLOC
	hash = 0xbebebebe;
#endif
	static_assert( sizeof(queue_segment) % 16 == 0, "padding failed" );
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

    static void deallocate( queue_segment * seg, segmented_queue_base * d ) {
	// errs() << "deallocate " << *seg << " by " << d << std::endl;
#if DBG_ALLOC
#endif
	if( __sync_fetch_and_add( &seg->dflag, 1 ) == 0 ) {
	    assert( seg->logical_pos >= 0 && "logical position unknown when destructed" );
	    assert( seg->dflag == 1 );
#if DBG_ALLOC
	    assert( seg->hash == 0xbebebebe );
	    assert( !seg->dseg );
	    seg->hash = 0xdeadbeef;
	    seg->dseg = d;
#endif
	    if( 1 ) // set to 0 to avoid memory reuse (debugging)
		delete seg;
	}
    }
	
public:
    // Allocate control fields and data buffer in one go
    template<typename T>
    static queue_segment * create( long logical, size_t seg_size,
				   size_t peekoff, size_t seqno ) {
	typeinfo_array tinfo = typeinfo_array::create<T>();
	size_t buffer_size = fixed_size_queue::get_buffer_space<T>( seg_size );
	char * memory = new char [sizeof(queue_segment) + buffer_size];
	char * buffer = &memory[sizeof(queue_segment)];
	size_t step = fixed_size_queue::get_element_size<T>();
	tinfo.construct<T>( buffer, &buffer[buffer_size], step );
	return new (memory) queue_segment( tinfo, logical, buffer, step,
					   seg_size, peekoff, seqno );
	return (queue_segment*)memory;
    }

    // Accessor functions for control (not exposed to user API)
    bool is_full()  const volatile { return q.full(); }
    // bool is_empty() const volatile { return q.empty(); }
    bool is_producing()  const volatile { return !copied_peek || ( producing && !next ); } // !!!
    void set_producing( bool p = true ) volatile { producing = p; }
    void clr_producing() volatile { producing = false; }

    size_t get_peek_dist() const { return q.get_peek_dist(); }

    long get_logical_head() const {
	return logical_pos < 0 ? -1 : logical_pos + volume_pop;
    }
    // void set_logical_head( int logical_ ) { logical_head = logical_; }
    long get_logical_tail_wpeek() const {
	return logical_pos < 0 ? -1 : logical_pos + volume_push;
    }
    long get_logical_tail() const {
	// TODO: might optimize peekoff > volume_push comparison away
	// by initializing both volume_push and volume_pop to peekoff, in case
	// peekoff > 0.
	return logical_pos < 0 ? -1 :
	    q.get_peek_dist() > volume_push ? logical_pos :
	    logical_pos + volume_push - q.get_peek_dist();
    }
    long get_logical_pos() const { return logical_pos; }
/* unused
    void set_logical_pos( int logical_ ) {
	// errs() << "Update logical position of " << this
	// << " from " << logical_pos << " to " << logical_ << "\n";
	logical_pos = logical_;
    }
*/

/*
    bool contains( size_t logical ) const {
	if( get_logical_head() <= (long)logical
	    && get_logical_tail() > (long)logical )
	    return true;
	else if( get_logical_head() == (long)logical
		 && get_logical_tail() == (long)logical )
	    return true;
	else if( get_peek_dist() > 0
		 && get_logical_pos() <= (long)logical
		 && get_logical_tail() > (long)logical )
	    return true;
	else
	    return false;
    }
*/

    // Beware of race condition between propagating logical position
    // versus pushing new segment and updating logical position.
    // Should be ok if link before update position.
    void propagate_logical_pos( long logical, queue_index & idx ) {
	// Done if known.
	lock();
	if( logical_pos >= 0 ) {
	    assert( logical_pos == logical );
	    unlock();
	    return;
	}
	assert( logical_pos < 0
		&& "logical position must be unknown when updating" );
	// Lock segment such that we cannot propagate the logical position into
	// a segment that is currently being pushed at unknown index. Such a race
	// may leave the newly pushed segment at unknown position.
	logical_pos = logical;
	idx.insert( this );
	unlock();
	if( next )
	    next->propagate_logical_pos( get_logical_tail(), idx );
    }

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
	assert( next_->logical_pos == get_logical_tail()
		|| logical_pos < 0 );
	assert( volume_push > q.get_peek_dist() );

	next = next_;
	// errs() << "Link " << this << " ltail=" << get_logical_tail()
	       // << " to " << next << " pos=" << next->logical_pos << std::endl;
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

    bool is_empty( size_t logical ) const {
	return ( ( logical - logical_pos < q.get_peek_dist() )
		 ? !copied_peek : false )
	    || (long)logical >= get_logical_tail()
	    || !q.is_produced( logical - logical_pos );
    }

    bool is_empty_wpeek( size_t logical ) const {
	return ( ( logical - logical_pos < q.get_peek_dist() )
		 ? !copied_peek 
		 : (long)logical >= get_logical_tail_wpeek() )
	    || !q.is_produced( logical - logical_pos );
    }

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
    T & pop( size_t logical ) {
#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	while( q.empty() || ( logical - size_t(logical_pos) < q.get_peek_dist() && !copied_peek ) )
	    sched_yield();
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().qs_pop );
#endif // PROFILE_QUEUE
	// Translate global position we're popping from to local queue position
	// Two behaviors of the fixed_size_queue:
	// * As a real queue, round-robin when used by one popper
	// * As an array, when concurrent pops occur
	// errs() << "queue_segment: pop @" << logical << " seg=" << *this << "\n";
	T & r = q.pop<T>( logical - logical_pos );
	__sync_fetch_and_add( &volume_pop, 1 );
	return r;
    }

    template<typename T>
    T & peek( size_t logical ) {
#if PROFILE_QUEUE
	pp_time_start( &get_profile_queue().qs_peek );
#endif // PROFILE_QUEUE
	while( q.empty() || ( logical - size_t(logical_pos) < q.get_peek_dist() && !copied_peek ) )
	    sched_yield();
#if PROFILE_QUEUE
	pp_time_end( &get_profile_queue().qs_peek );
#endif // PROFILE_QUEUE

	return q.peek<T>( logical - logical_pos );
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
    read_slice<MetaData,T> get_slice( size_t logical, size_t npop ) {
	return q.get_slice<MetaData,T>( logical - logical_pos, npop );
    }

private:
};

inline std::ostream &
operator << ( std::ostream & os, const queue_segment & seg ) {
    return os << "Segment: @" << &seg
	      << " @" << seg.logical_pos
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

template<>
struct avl_traits<obj::queue_segment, obj::queue_key_t> {
    typedef obj::queue_segment queue_segment;

    static queue_segment * & left( queue_segment * n ) {
	return n->child[(int)dir_left];
    }
    static queue_segment * & right( queue_segment * n ) {
	return n->child[(int)dir_right];
    }
    static queue_segment * & child( queue_segment * n, avl_dir_t dir ) {
	return n->child[(int)dir];
    }
    static avl_cmp_t compare( queue_segment * l, queue_segment * r ) {
	// errs() << "compare: l=" << *l << std::endl;
	// errs() << "         r=" << *r << std::endl;
	if( l->logical_pos < r->logical_pos )
	    return cmp_lt;
	else if( l->logical_pos > r->logical_pos )
	    return cmp_gt;
	else
	    return cmp_eq;
    }
    static avl_cmp_t compare( queue_segment * seg, obj::queue_key_t & key ) {
	if( size_t(seg->logical_pos) > key.logical )
	    return cmp_gt;
/* if commented back in, then make sure we distinguish primary and secondary match
	else if( seg->logical_pos + seg->volume_push == key.logical
		 && !seg->is_producing() && seg->seqno == key.seqno )
	    return cmp_eq;
*/
	else if( seg->logical_pos + seg->volume_push <= key.logical )
	    return cmp_lt;
	else
	    // Older segments are visible to newer pops
	    return seg->seqno <= key.seqno ? cmp_eq : cmp_gt;
    }
    static bool is_secondary( queue_segment * seg, obj::queue_key_t & key ) {
	return seg->logical_pos + seg->volume_push == key.logical
	    && !seg->is_producing() && seg->seqno == key.seqno;
    }
    static short & balance( queue_segment * n ) {
	return n->balance;
    }
};

#endif // QUEUE_QUEUE_SEGMENT_H

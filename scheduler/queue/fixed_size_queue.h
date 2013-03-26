// -*- c++ -*-

#ifndef QUEUE_FIXED_SIZE_QUEUE_H
#define QUEUE_FIXED_SIZE_QUEUE_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "swan/alc_allocator.h"
#include "swan/alc_mmappol.h"
#include "swan/alc_flpol.h"
#include "swan/padding.h"

namespace obj {

class fixed_size_queue
{
    // Cache block 0: owned by producer
    volatile size_t head;
    pad_multiple<CACHE_ALIGNMENT, sizeof(size_t)> pad0;

    // Cache block 1: owned by consumer
    volatile size_t tail;
    pad_multiple<CACHE_ALIGNMENT, sizeof(size_t)> pad1;

    // Cache block 2: unmodified during execution
    const size_t elm_size;
    const size_t size;
    const size_t mask;
    char * const buffer;
    pad_multiple<CACHE_ALIGNMENT, 3*sizeof(size_t) + sizeof(char *)> pad2;
	
private:
    static size_t log2_up( size_t uu ) {
	volatile size_t u = uu;
	if( u == 0 )
	    return 1;
	else {
	    volatile size_t l = 0;
	    while( u > 0 ) {
		u >>= 1;
		l++;
	    }
	    if( (uu & (uu-1)) == 0 )
		return l-1;
	    else
		return l;
	}
    }
	
private:
    // Credit:
    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    static size_t roundup_pow2( size_t v ) {
	v--;
	v |= v>>1;
	v |= v>>2;
	v |= v>>4;
	v |= v>>8;
	v |= v>>16;
	v |= v>>32;
	v++;
	return v;
    }

    // Compute buffer size: max_size elements of size rounded up to 8 bytes
    template<typename T>
    static size_t get_element_size() {
	return roundup_pow2(((sizeof(T)+7)&~(size_t)7));
    }

public:
    template<typename T>
    static size_t get_buffer_space( size_t max_size ) {
	size_t size = max_size * get_element_size<T>();
	size_t log_size = log2_up( size );
	assert( log_size > 0 && (size_t(1)<<(log_size-1)) < size
		&& size <= (size_t(1)<<log_size) );
	return size_t(1) << log_size;
    }

private:
    friend class queue_segment;

    fixed_size_queue( char * buffer_, size_t elm_size_, size_t max_size )
	: head( 0 ), tail( 0 ),
	  elm_size( elm_size_ ),
	  size( roundup_pow2( max_size * elm_size ) ),
	  mask( size-1 ), buffer( buffer_ ) { 
	static_assert( sizeof(fixed_size_queue) % CACHE_ALIGNMENT == 0,
		       "padding failed" );
    }

public:
    ~fixed_size_queue() {
// TODO: destruct
    }
	
    bool empty() const volatile { return head == tail; }
    bool full() const volatile {
	return ((tail+elm_size) & mask) == head;
    }
	
    // peek first element
    char* front() const { return &buffer[head]; }

    bool is_produced( size_t pos ) const {
	pos = (pos * elm_size) & mask;
	if( head <= tail )
	    return head <= pos && pos < tail;
	else
	    return head <= pos || pos < tail;
    }

    // returns NULL if pop fails
    template<typename T>
    bool pop( T & t, size_t pos ) {
	if( empty() ) {
	    // errs() << "Q " << this << " empty in pop\n";
	    return false;
	} else {
	    char* value = &buffer[(pos*elm_size) & mask];
	    t = *reinterpret_cast<T *>( value );
	    if( ((pos*elm_size) & mask) == head ) // Queue behavior (no concurrent pops)
		head = (head+elm_size) & mask;
	    return true;
	}
    }
	
    // returns true on success false on failure
    template<typename T>
    bool push( T * value ) {
	if( full() ) {
	    return false;
	} else {
	    *reinterpret_cast<T *>( &buffer[tail] ) = *value;
	    tail = (tail+elm_size) & mask;
	    return true;
	}
    }
    friend std::ostream & operator << ( std::ostream & os, fixed_size_queue & q );
};

inline std::ostream & operator << ( std::ostream & os, fixed_size_queue & q ) {
    return os << " QUEUE: head=" << q.head << " tail=" << q.tail
	      << " size=" << q.size << " mask=" << std::hex << q.mask
	      << std::dec;
}

} //namespace obj

#endif // QUEUE_FIXED_SIZE_QUEUE_H

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

template<typename MetaData, typename T>
class read_slice {
    const char * buffer;
    size_t npop;
    queue_version<MetaData> * version;

public:
    read_slice( const char * buffer_, size_t npop_ )
	: buffer( buffer_ ), npop( npop_ ), version( 0 ) { }
    void set_version( queue_version<MetaData> * version_ ) {
	version = version_;
    }

    size_t get_npops() const { return npop; }

    void commit() {
	version->pop_bookkeeping( npop );
    }

    const T & pop() {
	const char * e = buffer;
	buffer += sizeof(T);
	return *reinterpret_cast<const T *>( e );
    }

    const T & peek( size_t off ) {
	return *reinterpret_cast<const T *>( &buffer[off * sizeof(T)] );
    }
};

class fixed_size_queue
{
    // Cache block 0: owned by producer
    volatile size_t head;
    pad_multiple<CACHE_ALIGNMENT, sizeof(size_t)> pad0;

    // Cache block 1: owned by consumer
    volatile size_t tail;
    pad_multiple<CACHE_ALIGNMENT, sizeof(size_t)> pad1;

    // Cache block 2: unmodified during execution
    const typeinfo_array tinfo;
    const size_t elm_size;
    const size_t size;
    const size_t mask;
    char * const buffer;
    size_t peekoff;
    pad_multiple<CACHE_ALIGNMENT, sizeof(typeinfo_array) + 4*sizeof(size_t) + sizeof(char *)> pad2;
	
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
	// return roundup_pow2(((sizeof(T)+7)&~(size_t)7));
	return sizeof(T);
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

    fixed_size_queue( typeinfo_array tinfo_, char * buffer_,
		      size_t elm_size_, size_t max_size, size_t peekoff_ )
	: head( 0 ), tail( peekoff_*elm_size_ ), tinfo( tinfo_ ),
	  elm_size( elm_size_ ),
	  size( roundup_pow2( max_size * elm_size ) ),
	  mask( size-1 ), buffer( buffer_ ), peekoff( peekoff_ ) {
	static_assert( sizeof(fixed_size_queue) % CACHE_ALIGNMENT == 0,
		       "padding failed" );
    }

public:
    ~fixed_size_queue() {
	tinfo.destruct( buffer, &buffer[size], elm_size );
    }

    size_t get_peek_dist() const { return peekoff; }
    void rewind() { tail = 0; } // very first segment has no copied-in peek area
	
    bool empty() const volatile { return head == tail; }
    bool full() const volatile {
	// Freeze tail at end of buffer to avoid wrap-around in case of peeking
	size_t full_marker = peekoff > 0 ? 0 : head;
	return ((tail+elm_size) & mask) == full_marker;
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

    template<typename T>
    T & peek( size_t pos ) {
	char* value = &buffer[(pos*elm_size) & mask];
	return *reinterpret_cast<T *>( value );
    }

    // returns NULL if pop fails
    template<typename T>
    T & pop( size_t pos ) {
	assert( elm_size == sizeof(T) );
	char* value = &buffer[(pos*elm_size) & mask];
	T & r = *reinterpret_cast<T *>( value );
	// Queue behavior (no concurrent pops)
	if( ((pos*elm_size) & mask) == head )
	    head = (head+elm_size) & mask;
	return r;
    }
	
    // returns true on success false on failure
    template<typename T>
    bool push( const T * value ) {
	assert( elm_size == sizeof(T) );
	if( full() ) {
	    return false;
	} else {
	    // std::copy<T>( *reinterpret_cast<T *>( &buffer[tail] ), *value );
	    memcpy( reinterpret_cast<T *>( &buffer[tail] ), value, sizeof(T) );
	    tail = (tail+elm_size) & mask;
	    return true;
	}
    }

    template<typename MetaData, typename T>
    read_slice<MetaData,T> get_slice( size_t pos, size_t npop ) {
	return read_slice<MetaData,T>( &buffer[pos * sizeof(T)], npop );
    }

    void copy_peeked( const char * buff ) {
	if( peekoff > 0 )
	    memcpy( &buffer[head], buff, peekoff * elm_size );
    }

    const char * get_peek_suffix() const {
	return &buffer[tail - elm_size * peekoff];
    }

    friend std::ostream & operator << ( std::ostream & os, const fixed_size_queue & q );
};

inline std::ostream & operator << ( std::ostream & os, const fixed_size_queue & q ) {
    return os << " QUEUE: head=" << q.head << " tail=" << q.tail
	      << " size=" << q.size << " mask=" << std::hex << q.mask
	      << std::dec;
}

} //namespace obj

#endif // QUEUE_FIXED_SIZE_QUEUE_H

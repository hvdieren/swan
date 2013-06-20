/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.gr)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.gr)
 * 
 * This file is part of Swan.
 * 
 * Swan is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Swan is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Swan.  If not, see <http://www.gnu.org/licenses/>.
 */

// -*- c++ -*-
#ifndef ALC_BFLPOL_H
#define ALC_BFLPOL_H

#include "swan_config.h"

#include <limits>
#include <vector>

namespace alc {

template<typename Allocator, size_t ChunkSize>
class byte_freelist_alloc_policy {
public : 
    // typedefs
    typedef char value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

public : 
    // convert a byte_freelist_alloc_policy<T> to byte_freelist_alloc_policy<U>
    template<typename U>
    struct rebind {
    private:
	typedef typename Allocator::template rebind<U>::other other_alloc;
    public:
        typedef byte_freelist_alloc_policy<other_alloc, ChunkSize> other;
    };

public : 
    inline explicit byte_freelist_alloc_policy() : free_list( 0 ) { }
    inline ~byte_freelist_alloc_policy() {
/* Can't do this when passing allocators to STL classes, because they
   create/destruct allocators on the fly
	for( std::vector<void *>::const_iterator I=chunks.begin(),
		 E=chunks.end(); I != E; ++I ) {
	    allocator.deallocate( reinterpret_cast<pointer>(*I), ChunkSize );
	}
*/
    }
    inline explicit byte_freelist_alloc_policy(byte_freelist_alloc_policy const& fl)
	: free_list( 0 ) { }
    template <typename A, size_t C>
    inline explicit byte_freelist_alloc_policy(byte_freelist_alloc_policy<A, C> const&)
	: free_list( 0 ) {
    }
    
    // memory allocation
    inline pointer allocate(size_type cnt, 
			    typename std::allocator<void>::const_pointer = 0) { 
	if( !free_list )
	    refill(cnt);
	assert( cnt <= object_size );
	item_t * ret = free_list;
	free_list = free_list->next;
        return reinterpret_cast<pointer>( ret ); 
    }
    inline void deallocate(pointer p, size_type) {
        item_t * ret = reinterpret_cast<item_t *>( p ); 
	ret->next = free_list;
	free_list = ret;
    }

    // size
    inline size_type max_size() const { 
        return std::numeric_limits<size_type>::max(); 
    }

private:
    struct item_t {
	item_t * next;
    };
    item_t * free_list;
    std::vector<void *> chunks;
    Allocator allocator;
    size_t object_size;

    void refill( size_type cnt ) {
	pointer start = (pointer) allocator.allocate( cnt * ChunkSize );
	assert( cnt >= sizeof(item_t) );
	assert( !object_size || cnt == object_size );
	chunks.push_back( start );
	for( pointer p=start; p<start+cnt*ChunkSize; p += cnt ) {
	    item_t * ret = reinterpret_cast<item_t*>( p );
	    ret->next = free_list;
	    free_list = ret;
	    	// printf( "push %p\n", ret );
	}
	object_size = cnt;
    }
};    //    end of class byte_freelist_alloc_policy


// determines if memory from another
// allocator can be deallocated from this one
template<typename Allocator, size_t ChunkSize>
inline bool operator==(byte_freelist_alloc_policy<Allocator, ChunkSize> const&, 
		       byte_freelist_alloc_policy<Allocator, ChunkSize> const&) { 
    return true;
}
template<typename OtherAllocator, typename Allocator, size_t ChunkSize>
inline bool operator==(byte_freelist_alloc_policy<Allocator, ChunkSize> const&, 
		       OtherAllocator const&) { 
    return false; 
}

};

#endif // ALC_BFLPOL_H

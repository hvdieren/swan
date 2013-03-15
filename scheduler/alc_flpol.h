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
#ifndef ALC_FLPOL_H
#define ALC_FLPOL_H

#include "swan_config.h"

#include <limits>
#include <vector>

namespace alc {

template<typename T, typename Allocator, size_t ChunkSize>
class freelist_alloc_policy {
public : 
    // typedefs
    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

public : 
    // convert a freelist_alloc_policy<T> to freelist_alloc_policy<U>
    template<typename U>
    struct rebind {
    private:
	typedef typename Allocator::template rebind<U>::other other_alloc;
    public:
        typedef freelist_alloc_policy<U, other_alloc, ChunkSize> other;
    };

public : 
    inline explicit freelist_alloc_policy() : free_list( 0 ) { }
    inline ~freelist_alloc_policy() {
/* Can't do this when passing allocators to STL classes, because they
   create/destruct allocators on the fly
	for( std::vector<void *>::const_iterator I=chunks.begin(),
		 E=chunks.end(); I != E; ++I ) {
	    allocator.deallocate( reinterpret_cast<pointer>(*I), ChunkSize );
	}
*/
    }
    inline explicit freelist_alloc_policy(freelist_alloc_policy const& fl)
	: free_list( 0 ) { }
    template <typename U, typename A, size_t C>
    inline explicit freelist_alloc_policy(freelist_alloc_policy<U, A, C> const&)
	: free_list( 0 ) {
    }
    
    // memory allocation
    inline pointer allocate(size_type cnt, 
			    typename std::allocator<void>::const_pointer = 0) { 
	assert( cnt == 1
		&& "free_list_allocator can handle fixed-size blocks only" );
	if( unlikely( !free_list ) )
	    refill();
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

    void refill() __attribute__((noinline, cold));
};    //    end of class freelist_alloc_policy

template<typename T, typename Allocator, size_t ChunkSize>
void
freelist_alloc_policy<T,Allocator,ChunkSize>::refill() {
    pointer start = allocator.allocate( ChunkSize );
    chunks.push_back( start );
    for( pointer p=start; p<start+ChunkSize; ++p ) {
	item_t * ret = reinterpret_cast<item_t*>( p );
	ret->next = free_list;
	free_list = ret;
    }
}


// determines if memory from another
// allocator can be deallocated from this one
template<typename T, typename T2, typename Allocator, size_t ChunkSize>
inline bool operator==(freelist_alloc_policy<T, Allocator, ChunkSize> const&, 
		       freelist_alloc_policy<T2, Allocator, ChunkSize> const&) { 
    return true;
}
template<typename T, typename OtherAllocator,
	 typename Allocator, size_t ChunkSize>
inline bool operator==(freelist_alloc_policy<T, Allocator, ChunkSize> const&, 
		       OtherAllocator const&) { 
    return false; 
}

};

#endif // ALC_FLPOL_H

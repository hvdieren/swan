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
#ifndef ALC_MMAPPOL_H
#define ALC_MMAPPOL_H

#include "swan_config.h"

#include <sys/mman.h>

#include <cassert>
#include <limits>
#include <vector>

namespace alc {

template<typename T, size_t Align>
class mmap_alloc_policy {
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
    // convert a mmap_alloc_policy<T> to mmap_alloc_policy<U>
    template<typename U>
    struct rebind {
        typedef mmap_alloc_policy<U, Align> other;
    };

public : 
    inline explicit mmap_alloc_policy() {}
    inline ~mmap_alloc_policy() {}
    inline explicit mmap_alloc_policy(mmap_alloc_policy const&) {}
    template <typename U>
    inline explicit mmap_alloc_policy(mmap_alloc_policy<U, Align> const&) {}
    
    // memory allocation
    inline pointer allocate(size_type cnt, 
			    typename std::allocator<void>::const_pointer = 0) { 
	size_type alc_size = alloc_size( cnt );
	void * ptr = mmap( 0, alc_size, PROT_WRITE|PROT_READ,
			   MAP_ANON|MAP_PRIVATE, -1, 0 );
	assert( ptr != MAP_FAILED && "Cannot mmap() memory" );

	size_t offset = (Align - size_type(ptr)) & (Align-1);
	intptr_t start = intptr_t(ptr) + offset;
	*(void **)(start + restore_offset(cnt)) = ptr;
	return reinterpret_cast<pointer>( start );
    }
    inline void deallocate(pointer p, size_type cnt) {
	size_type alc_size = alloc_size( cnt );
	intptr_t start = reinterpret_cast<intptr_t>( p );
	void * ptr = *(void **)(start + restore_offset(cnt));
	munmap( ptr, alc_size );
    }

    // size
    inline size_type max_size() const { 
        return std::numeric_limits<size_type>::max(); 
    }

private:
    static const size_t align_size = (sizeof(T) + (Align-1)) & ~(Align-1);
    static size_t alloc_size( size_type cnt ) {
        return (cnt-1)*align_size + sizeof(T) + (Align-1) + sizeof(void *);
    }
    static size_type restore_offset( size_type cnt ) {
        return (cnt-1)*align_size + sizeof(T);
    }
};    //    end of class mmap_alloc_policy


// determines if memory from another
// allocator can be deallocated from this one
template<typename T, typename T2, size_t Align>
inline bool operator==(mmap_alloc_policy<T, Align> const&, 
		       mmap_alloc_policy<T2, Align> const&) { 
    return true;
}
template<typename T, size_t Align, typename OtherAllocator>
inline bool operator==(mmap_alloc_policy<T, Align> const&, 
		       OtherAllocator const&) { 
    return false; 
}

};

#endif // ALC_MMAPPOL_H

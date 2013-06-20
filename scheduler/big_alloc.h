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
#ifndef BIG_ALLOC_H
#define BIG_ALLOC_H

#include "swan_config.h"

#include <sys/mman.h>

#include <cstdint>
#include <cassert>
#include <vector>

template<typename T, size_t Chunk, size_t Align>
class big_mmap_allocator {
    struct node_t {
	node_t * next;
    };
    node_t * list;
    std::vector<void *> chunks;

    /*
      static_assert( sizeof(T) >= sizeof(node_t),
      "Size of elements in big_mmap_allocator must be at least "
      "that of node" );
    */

public:
    big_mmap_allocator() : list( 0 ) { }
    ~big_mmap_allocator() {
	for( std::vector<void *>::const_iterator
		 I=chunks.begin(), E=chunks.end(); I != E; ++I )
	    munmap( *I, sizeof(T) );
    }

    T * allocate() {
	if( !list )
	    refill();
	node_t * ptr = list;
	list = ptr->next;
	return reinterpret_cast<T*>( ptr );
    }

    void deallocate( T * t ) {
	node_t * ptr = reinterpret_cast<node_t *>( t );
	ptr->next = list;
	list = ptr;
    }

private:
    void refill( void ) {
	void * ptr = mmap( 0, sizeof(T)*Chunk, PROT_WRITE|PROT_READ,
			   MAP_ANON|MAP_PRIVATE, -1, 0 );
	assert( (intptr_t(ptr) & (Align-1)) == 0 || Chunk > 1 );

	chunks.push_back( ptr );

	size_t offset = (Align - (size_t)ptr) & (Align-1);
	intptr_t start = intptr_t(ptr) + offset;
	uintptr_t end = intptr_t(ptr) + sizeof(T)*Chunk + 1;
	size_t n = 0;
	for( intptr_t addr=start; addr+sizeof(T) <= end; addr+=Align, ++n ) {
	    node_t * node = reinterpret_cast<node_t *>( addr );
	    node->next = list;
	    list = node;
	}
	assert( n == Chunk || n == Chunk-1 );
    }
};


#endif // BIG_ALLOC_H

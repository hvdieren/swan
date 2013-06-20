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
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"
#include "logger.h"
#include "debug.h"

using namespace obj;

#define SEGMENT 10

struct A {
    int a;

    A() : a( -2 ) { }
    A( int a_ ) : a( a_ ) { }
    A( const A & a_ ) : a( a_.a ) {
	errs() << "warning: A copy constructor\n";
    }
    A( A && a_ ) : a( std::move( a_.a ) ) {
	// errs() << "A move constructor\n";
    }
    const A & operator = ( const A & a_ ) {
	errs() << "warning: A copy assignment\n";
	a = a_.a;
	// const_cast<A *>( &a_ )->a = -1;
	return *this;
    }
    const A & operator = ( A && a_ ) {
	// errs() << "A move assignment\n";
	a = a_.a;
	a_.a = -1; // move destroys old value
	return *this;
    }
};

void produce( pushdep<A> queue, int n ) {
    int g = 0;
    while( n > 0 ) {
	int s = std::min( n, SEGMENT );
	obj::write_slice<obj::queue_metadata, A> wslice
	    = queue.get_write_slice( s );
	for( int i=0; i < s; ++i ) {
	    wslice.push( A(g++) );
	}
	wslice.commit();
	n -= s;
    }
}

void consume( popdep<A> queue, int n, int p ) {
    int g = 0;
    int nn = n;
    while( n > 0 ) {
	int s = std::min( n, SEGMENT );
	obj::read_slice<obj::queue_metadata, A> rslice
	    = queue.get_read_slice_upto( s+p, p );
	s = rslice.get_length() - p;
	for( int i=0; i < s; ++i ) {
	    for( int j=0; j < p; ++j ) {
		const A & a = rslice.peek( j );
		assert( a.a == g+j );
	    }
	    A a = rslice.pop();
	    assert( a.a == g );
	    g++;
	}
	rslice.commit();
	n -= s;
    }
    assert( g == nn );
}

void test( int n, int p ) {
    hyperqueue<A> queue( SEGMENT, p );
    spawn( produce, (pushdep<A>)queue, n+p );
    spawn( consume, (popdep<A>)queue, n, p );
    ssync();
}

int main( int argc, char * argv[] ) {
    if( argc < 3 ) {
	std::cerr << "Usage: " << argv[0] << ": <num_elements> <num_peek>\n";
	return 1;
    }

    int n = atoi( argv[1] );
    int p = atoi( argv[2] );

    run( test, n, p );

    return 0;
}

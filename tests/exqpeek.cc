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
	errs() << "warning: A copy constructor (" << a << ")\n";
    }
    A( A && a_ ) : a( std::move( a_.a ) ) {
	// errs() << "A move constructor\n";
    }
    const A & operator = ( const A & a_ ) {
	a = a_.a;
	errs() << "warning: A copy assignment (" << a << ")\n";
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
    for( int i=0; i < n; ++i ) {
	std::cerr << "produce " << i << "\n";
	queue.push( A(i) );
    }
}

void consume( popdep<A> queue, int n, int p ) {
    for( int i=0; i < n; ++i ) {
	for( int j=0; j < p; ++j ) {
	    const A & a = queue.peek( j );
	    assert( a.a == i+j );
	    std::cerr << "peek " << (i+j) << "\n";
	}
	A a = queue.pop();
	assert( a.a == i );
	std::cerr << "pop " << i << "\n";
    }
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

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

void test_int( void ) {
    hyperqueue<int> queue;
    queue.push( 0 );
    int i = 1;
    queue.push( i );

    int p0 = queue.pop();
    const int & p1 = queue.pop();

    errs() << "int p0=" << p0 << "\n";
    errs() << "int p1=" << p1 << "\n";
}

struct A {
    int a;

    A() : a( -2 ) { }
    A( int a_ ) : a( a_ ) { }
    A( const A & a_ ) : a( a_.a ) {
	errs() << "A copy constructor\n";
    }
    A( A && a_ ) : a( std::move( a_.a ) ) {
	errs() << "A move constructor\n";
    }
    const A & operator = ( const A & a_ ) {
	errs() << "A copy assignment\n";
	a = a_.a;
	const_cast<A *>( &a_ )->a = -1;
	return *this;
    }
    const A & operator = ( A && a_ ) {
	errs() << "A move assignment\n";
	a = a_.a;
	a_.a = -1;
	return *this;
    }
};

void test_A( void ) {
    hyperqueue<A> queue;
    errs() << "pushing A(0)...\n";
    queue.push( A(0) );
    errs() << "creating A(1)...\n";
    A i = 1;
    errs() << "pushing A(1)...\n";
    queue.push( i );

    errs() << "now popping 0...\n";
    A p0 = queue.pop();
    errs() << "now popping 1...\n";
    const A & p1 = queue.pop(); // this should not be allowed!
    errs() << "popping done.\n";

    errs() << "A p0=" << p0.a << "\n";
    errs() << "A p1=" << p1.a << "\n";
}

void test( void ) {
    test_int();
    test_A();
}

int main( int argc, char * argv[] ) {
    run( test );

    return 0;
}

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

#if DEBUG_CERR
void iolock() { }
void iounlock() { }
#else
pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }
#endif

struct S {
    int a;

    S() : a( 0 ) {
	iolock();
	errs() << "S default-constructor on " << this << "\n";
	iounlock();
    }

    S( int a_ ) : a( a_ ) {
	iolock();
	errs() << "S int-constructor on " << this << "\n";
	iounlock();
    }

    S( const S & s ) : a( s.a ) {
	iolock();
	errs() << "S copy-constructor on " << this << "\n";
	iounlock();
    }

    ~S() {
	iolock();
	errs() << "S destructor on " << this << "\n";
	iounlock();
    }
};

int my_main( int argc, char * argv[] );

void stage_a( int i, outdep<S> s ) {
    iolock();
    errs() << "stage A: s=" << *s.get_version() << '\n';
    iounlock();
}

void stage_b( indep<S> s ) {
    iolock();
    errs() << "stage B: s=" << *s.get_version() << "\n";
    iounlock();
}

void apipe( int n ) {
    object_t<S> obj;
    // unversioned<S> obj;

    for( int i=0; i < n; ++i ) {
	spawn( stage_a, i, (outdep<S>)obj );
	spawn( stage_b, (indep<S>)obj );
    }
    ssync();
}

int main( int argc, char * argv[] ) {
#if !DEBUG_CERR
    pthread_mutex_init( &io_lock_var, NULL );
#endif

    if( argc <= 1 ) {
	std::cerr << "Usage: " << argv[0] << " <n>\n";
	return 1;
    }

    int n = atoi( argv[1] );
    run( apipe, n );

    return 0;
}

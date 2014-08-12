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

using namespace obj;

#if DEBUG_CERR
void iolock() { }
void iounlock() { }
#else
pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }
#endif

int my_main( int argc, char * argv[] );

void stage_a( int i, float * ab ) {
    *ab = float(i);
    iolock();
    errs() << "stage A: f=" << (float)*ab << '\n';
    iounlock();
}

void stage_b( float ab, int * bc ) {
    *bc = int((float)ab);
    iolock();
    errs() << "stage B: f=" << (float)ab << "\n";
    errs() << "         i=" << (int)*bc << "\n";
    iounlock();
}

void stage_c( int bc ) {
    static int seen[100] = { 0 };
    int i = (int)bc;
    iolock();
    errs() << "stage C: i=" << (int)bc << '\n';
    iounlock();
    assert( seen[i] == 0 );
    seen[i] = 1;
}

void apipe( int n ) {
    float ab;
    int bc;

    ab = -1.0;
    bc = -1;

    for( int i=0; i < n; ++i ) {
	stage_a( i, &ab );
	stage_b( ab, &bc );
	stage_c( bc );
    }
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

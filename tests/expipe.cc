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

int my_main( int argc, char * argv[] );

int stage_a( int i, outdep<float> ab ) {
    ab = float(i);
    iolock();
    errs() << "stage A: f=" << *ab.get_version() << ", val=" << (float)ab << '\n';
    iounlock();
    usleep( 1 );
    return 0;
}

int stage_b( indep<float> ab, outdep<int> bc ) {
    static int seen[100] = { 0 };
    int i = int((float)ab);
    bc = i;
    iolock();
    errs() << "stage B: f=" << *ab.get_version() << ", val=" << (float)ab << "\n";
    errs() << "         i=" << *bc.get_version() << ", val=" << (int)bc << "\n";
    iounlock();
    if( seen[i] != 0 ) {
	fprintf( stderr, "ERROR: B: value %d sent multiple times!\n", i );
	fflush( stderr );
	abort();
    }
    seen[i] = 1;
    return 0;
}

int stage_c( inoutdep<int> bc ) {
    static int seen[100] = { 0 };
    int i = (int)bc;
    iolock();
    errs() << "stage C: i=" << *bc.get_version() << ", val=" << (int)bc << '\n';
    iounlock();
    if( seen[i] != 0 ) {
	fprintf( stderr, "ERROR: C: value %d sent multiple times!\n", i );
	fflush( stderr );
	abort();
    }
    seen[i] = 1;
    return 0;
}

void apipe( int n ) {
    object_t<float> obj_ab;
    object_t<int> obj_bc;
    chandle<int> ka[n], kb[n], kc[n];

    obj_ab = -1.0;
    obj_bc = -1;

    for( int i=0; i < n; ++i ) {
	spawn( stage_a, ka[i], i, (outdep<float>)obj_ab );
	// iolock(); errs() << "Between A and B\n"; iounlock();
	spawn( stage_b, kb[i], (indep<float>)obj_ab, (outdep<int>)obj_bc );
	// iolock(); errs() << "Between B and C\n"; iounlock();
	spawn( stage_c, kc[i], (inoutdep<int>)obj_bc );
	// iolock(); errs() << "Between C and A\n"; iounlock();
    }
    ssync();
    errs() << "apipe after sync" << std::endl;
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

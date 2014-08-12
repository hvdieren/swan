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

#include <swan/wf_interface.h>

using namespace obj;

#if DEBUG_CERR
void iolock() { }
void iounlock() { }
#else
pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }
#endif

void one_producer( int from, int to, int delay, pushdep<int> q ) {
    iolock();
    errs() << "Producer from " << from << " to " << to << "\n";
    iounlock();
    for( int i=from; i < to; ++i ) {
	iolock();
	errs() << "produce: " << i << '\n';
	iounlock();
	q.push( i );
	usleep( delay );
    }
}

void producer( int n, int producers, int delay, pushdep<int> q ) {
    // Spawn this many producer tasks
    int np = (n + (producers-1)) / producers;
    for( int i=0; i < producers; ++i )
	spawn( one_producer, np*i, np*i+std::min(n-i*np, np), delay, q );
    ssync();
}


void one_consumer( int from, int to, int delay, popdep<int> q ) {
    iolock();
    errs() << "Consumer from " << from << " to " << to << "\n";
    iounlock();
    for( int i=from; i < to; ++i ) {
	int j = q.pop();
	iolock();
	errs() << "consume: " << j << '\n';
	iounlock();
	if( i != j ) {
	    errs() << "ERROR: expected " << i << " got " << j << "\n";
	    abort();
	}
	usleep( delay );
    }
}

void consumer( int n, int consumers, int delay, popdep<int> q ) {
    // Spawn this many consumer tasks
    int nc = (n + (consumers-1)) / consumers;
    for( int i=0; i < consumers; ++i ) {
	spawn( one_consumer, nc*i, nc*i+std::min(n-i*nc, nc), delay, q );
    }
    ssync();
    if( !q.empty() ) {
	errs() << "ERROR: expected empty queue!\n";
	abort();
    }
}

void apipe( int n, int producers, int consumers, int delay ) {
    hyperqueue<int> queue;

    spawn( producer, n, producers, delay, (pushdep<int>)queue );
    spawn( consumer, n, consumers, delay, (popdep<int>)queue );

    ssync();
}

int main( int argc, char * argv[] ) {
#if !DEBUG_CERR
    pthread_mutex_init( &io_lock_var, NULL );
#endif

    if( argc <= 1 ) {
	std::cerr << "Usage: " << argv[0]
		  << " <n> [<producers>] [<consumers>] [<delay>]\n";
	return 1;
    }

    int n = atoi( argv[1] );
    int delay = 0, producers = 1, consumers = 1;
    if( argc > 2 )
	producers = atoi( argv[2] );
    if( argc > 3 )
	consumers = atoi( argv[3] );
    if( argc > 4 )
	delay = atoi( argv[4] );

    run( apipe, n, producers, consumers, delay );

    return 0;
}

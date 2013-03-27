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

void producer( int s, int n, int delay, pushdep<int> q ) {
    for( int i=0; i < n; ++i ) {
	q.push( s+i );
	iolock();
	errs() << "produce: " << (s+i) << '\n';
	iounlock();
	usleep( delay );
    }
}

void producer_rec( int s, int n, int step, int delay, pushdep<int> q ) {
    int nsteps = (n-s+step-1)/step;
    errs() << "Recursive: " << s << " by " << step << " to " << n
	   << " in " << nsteps << " steps\n";
    if( nsteps > 1 ) {
	spawn( producer_rec, s, s+(nsteps/2)*step, step, delay, q );
	spawn( producer_rec, s+(nsteps/2)*step, n, step, delay, q );
    } else
	spawn( producer, s, std::min(step,n-s), delay, q );
    ssync();
}

void consumer( int s, int n, int delay, prefixdep<int> q ) {
    errs() << "Consumer with n=" << n << " q.count=" << q.count << "\n";
    for( int i=0; i < n; ++i ) {
	int j = q.pop();
	int idx = q.get_index();
	iolock();
	errs() << "consume: " << j << '\n';
	iounlock();
	if( s+i != j ) {
	    errs() << "ERROR: expected " << s+i << " got " << j << "\n";
	    abort();
	}
	if( j+1 != idx ) {
	    errs() << "ERROR: index " << idx << " (+1) got " << j << "\n";
	    abort();
	}
	usleep( delay );
    }
    if( !q.empty() ) {
	errs() << "ERROR: expected empty queue!\n";
	abort();
    }
}

void zpipe( int dummy ) {
    hyperqueue<int> queue;
    // TODO: Add a check for emptiness in consumer!
    spawn( consumer, 0, 0, 0, queue.prefix( 0 ) );
    ssync();
}

// Add variable delays to producers, delaying first producer significantly
// compared to second, and so on.
void apipe( int n, int producers_, int consumers_, int delay ) {
    hyperqueue<int> queue;
    int np, nc;
    int producers = producers_ < 0 ? -producers_ : producers_;
    int produce_rec = producers_ < 0;
    int consumers = consumers_ < 0 ? -consumers_ : consumers_;
    int consume_open = consumers_ < 0;

    np = (n + (producers-1)) / producers;
    if( produce_rec )
	spawn( producer_rec, 0, n, np, delay, (pushdep<int>)queue );
    else {
	for( int i=0; i < producers; ++i )
	    spawn( producer, np*i, std::min(n-i*np, np),
		   delay, (pushdep<int>)queue );
    }

    nc = (n + (consumers-1)) / consumers;
    for( int i=0; i < consumers; ++i ) {
	spawn( consumer, nc*i, std::min(n-i*nc, nc),
	       delay, queue.prefix( std::min(n-i*nc, nc) ) );
    }

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

    if( producers == 0 )
	run( zpipe, 0 );
    else
	run( apipe, n, producers, consumers, delay );

    return 0;
}

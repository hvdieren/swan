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

#include <unistd.h>

#include "wf_interface.h"

using namespace obj;

void task_in( indep<int> in, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_in n=" << int(n) << '\n';
}

void task_out( outdep<int> out, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_out n=" << int(n) << '\n';
}

void task_inout( inoutdep<int> inout, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_inout n=" << int(n) << '\n';
}

#if OBJECT_COMMUTATIVITY
void task_cinout( cinoutdep<int> inout, int n ) {
    usleep( int(n)*1000 );
    errs() << "task_cinout n=" << int(n) << '\n';
}
#endif

void test0( int n ) {
    object_t<int> obj;

    spawn( task_inout, (inoutdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

void test1( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_out, (outdep<int>)obj, 0 );

    ssync();
}

void test2( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i ) {
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    }
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

void test3( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );

    ssync();
}

#if OBJECT_COMMUTATIVITY
void test4( int n ) {
    object_t<int> obj;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	spawn( task_cinout, (cinoutdep<int>)obj, (n-i)*10 );
    spawn( task_inout, (inoutdep<int>)obj, 0 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );

    ssync();
}

void test5( int n ) {
    object_t<int> obj, x;

    spawn( task_out, (outdep<int>)obj, 1000 );
    for( int i=0; i < n; ++i )
	if( i&1 )
	    spawn( task_cinout, (cinoutdep<int>)obj, (n-i)*10 );
	else
	    spawn( task_cinout, (cinoutdep<int>)x, (n-i)*10 );
    for( int i=0; i < n; ++i )
	spawn( task_in, (indep<int>)obj, (n-i)*10 );
    spawn( task_cinout, (cinoutdep<int>)obj, 0 );

    ssync();
}
#endif

int my_main( int argc, char * argv[] ) {
    if( argc <= 2 ) {
	std::cerr << "Usage: " << argv[0] << " <test> <n>\n";
	return 1;
    }

    int t = atoi( argv[1] );
    int n = atoi( argv[2] );
    switch( t ) {
    case 0:
	call( test0, n );
	break;
    case 1:
	call( test1, n );
	break;
    case 2:
	call( test2, n );
	break;
    case 3:
	call( test3, n );
	break;
    case 4:
#if OBJECT_COMMUTATIVITY
	call( test4, n );
#else
	std::cerr << argv[0] << ": commutativity not enabled\n";
#endif
	break;
    case 5:
#if OBJECT_COMMUTATIVITY
	call( test5, n );
#else
	std::cerr << argv[0] << ": commutativity not enabled\n";
#endif
	break;
    }

    return 0;
}

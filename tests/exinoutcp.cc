/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.org)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.org)
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

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

void task0( outdep<int> out ) {
    printf( "Task 0 start %p\n", out.get_version() );
    sleep(2); // secs
    out = 3;
    printf( "Task 0 done %p\n", out.get_version() );
}

void task1( indep<int> in ) {
    printf( "Task 1 start %p\n", in.get_version() );
    sleep(1);
    printf( "Task 1 done %p\n", in.get_version() );
}

void task2( inoutdep<int> inout ) {
    inout++;
    printf( "Task 2 increments to %d %p\n", (int)inout, inout.get_version() );
}

void task3( indep<int> in ) {
    printf( "Task 3 reads %d from %p\n", (int)in, in.get_version() );
}

int my_main( int argc, char * argv[] ) {
    object_t<int> v;
    spawn( task0, (outdep<int>)v );
    spawn( task1, (indep<int>)v );
    spawn( task2, (inoutdep<int>)v );
    spawn( task3, (indep<int>)v );

    ssync();

    return 0;
}

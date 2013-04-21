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

int * work0() __attribute__((noinline));
void * work1() __attribute__((noinline));

int * work0() {
    obj::typeinfo_array tinfo = obj::typeinfo_array::create<int *>();

    int * ptr = new int [1024];

    obj::typeinfo_array::construct<int *>( &ptr[0], &ptr[1024],
					   sizeof(int) );

    return ptr;
}

void * work1() { 
    obj::queue_segment * seg = obj::queue_segment::create<int *>(
	0, 1023, 0 );
    return (void *)seg;
}

int my_main( int argc, char * argv[] ) {
    work0();
    work1();
    return 0;
}

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

struct s5 {
    int x, y;
    char const * a;
    char const * b;
    char const * c;
};

namespace platform_x86_64 {

template<size_t ireg, size_t freg, size_t loff>
struct arg_passing<ireg, freg, loff, s5>
    : arg_passing_struct5<ireg, freg, loff, s5, int, int, char const *, char const *, char const *> {
};

}

void func( s5 s ) {
    errs() << "receive: x=" << s.x << " y=" << s.y
	   << " a=" << (void*)s.a << " b=" << (void*)s.b
	   << " c=" << (void*)s.c << "\n";
}

int my_main( int argc, char * argv[] ) {
    s5 s = { 1, 2, "astring", "bstring", "cstring" };

    errs() << "abi_arg_size: " << abi_arg_size<s5>() << "\n";
    errs() << "count_mem_words: "
	   << platform_x86_64::count_mem_words<s5>() << "\n";

    typedef platform_x86_64::arg_passing<0, 0, 0, s5> arg_pass;
    errs() << "in_reg? " << arg_pass::in_reg
	   << " ibump=" << arg_pass::ibump
	   << " fbump=" << arg_pass::fbump
	   << " lbump=" << arg_pass::lbump
	   << " inext=" << arg_pass::inext
	   << " fnext=" << arg_pass::fnext
	   << " lnext=" << arg_pass::lnext
	   << "\n";

    errs() << "send: x=" << s.x << " y=" << s.y
	   << " a=" << (void*)s.a << " b=" << (void*)s.b
	   << " c=" << (void*)s.c << "\n";

    errs() << "spawn:\n";
    spawn( func, s );
    ssync();

    errs() << "call:\n";
    call( func, s );

    errs() << "leaf_call:\n";
    leaf_call( func, s );

    return 0;
}

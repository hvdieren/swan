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

#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"
#include "logger.h"

using namespace std;

void func_int( int i ) {
    cout << "iteration " << i << endl;
}

void func_str( char * arg ) {
    cout << "argument " << arg << endl;
}

void func_str_ptr( char ** arg ) {
    cout << "argument pointer to " << *arg << endl;
}

// Need my_main() instead of main() to "enter parallel mode"
int my_main( int argc, char * argv[] ) {

    // integer range
    foreachi(0, 10, &func_int);

    // pointer to character string range
    foreach(&argv[0], &argv[argc], &func_str);

    // character string range
    foreachi(&argv[0], &argv[argc], &func_str_ptr);

    return 0;
}

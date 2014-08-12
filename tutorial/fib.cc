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

int my_main( int argc, char * argv[] );

int fib( int n ) {
  LOG( id_app, n );
  if( n < 2 )
    return n;
  else {
    chandle<int> x;
    spawn( fib, x, n-1 );
    int y = call( fib, n-2 );
    ssync();
    return y + (int)x;
  }
}

#if 0
int my_main( int argc, char * argv[] ) {
  if( argc <= 1 ) {
    std::cerr << "Usage: " << argv[0] << " <n>\n";
    return 1;
  }
  int n = atoi( argv[1] );
  chandle<int> k;
  spawn( fib, k, n );
  // std::cout << "my_main() after spawn before sync\n";
  ssync();
  // should be leaf-call(s)
  std::cout << "fib(" << n << ") = " << k << "\n";
  return 0;
}
#else
int main( int argc, char * argv[] ) {
  if( argc <= 1 ) {
    std::cerr << "Usage: " << argv[0] << " <n>\n";
    return 1;
  }
  int n = atoi( argv[1] );
  int k = run( fib, n );
  std::cout << "fib(" << n << ") = " << k << "\n";
  return 0;
}
#endif

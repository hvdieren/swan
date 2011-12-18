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

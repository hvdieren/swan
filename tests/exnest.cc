// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"
#include "logger.h"

void g( obj::indep<int>data ) {
    std::cout << "g: data=" << data.get_version()
	      << ' ' << *data.get_version()
	      << '\n';
    std::cout << "g: data=" << int(data) << '\n';
}

void fa( obj::outdep<int>data ) {
    data = 10;
    std::cout << "fa: data=" << data.get_version()
	      << ' ' << *data.get_version()
	      << '\n';
    std::cout << "fa: data=" << int(data) << '\n';
    sleep(1);
}

void fb( obj::indep<int>data ) {
    std::cout << "fb: data=" << data.get_version()
	      << ' ' << *data.get_version()
	      << '\n';
    std::cout << "fb: data=" << int(data) << '\n';
}

void fc( obj::outdep<int>data ) {
    data = 20;
    std::cout << "fc: data=" << data.get_version()
	      << ' ' << *data.get_version()
	      << '\n';
    std::cout << "fc: data=" << int(data) << '\n';
}

// Delegation: f( obj::inoutdep<int, obj::delegate> data )
// And simply call with obj::inoutdep<int> type.
// Now pas on delegated in other functions, but disallow any cast
// Only cast to obj::inoutdep<int> possible
void f( obj::outdep<int> data ) {
    // Enable this particular (set of) constructor(s) and do the copy-back
    // automatically with this flag in the object type
    // obj::object_t<int, obj::recast> child_data = data;
    obj::object_t<int, obj::obj_recast> child_data = data;
    std::cout << "f: data=" << data.get_version()
	      << ' ' << *data.get_version()
	      << " instance " << data.get_version()->get_instance()
	      << '\n'
	      << "   child_data=" << child_data.get_version()
	      << ' ' << *child_data.get_version()
	      << " instance " << child_data.get_version()->get_instance()
	      << '\n';
    spawn( fa, (obj::outdep<int>)child_data );
    spawn( fb, (obj::indep<int>)child_data );
    spawn( fc, (obj::outdep<int>)child_data );
    sleep(1);
    ssync();
    // data = (int)child_data; // uuurgh! - removed with obj::recast
}

void top( int n ) {
    obj::object_t<int> data;
    spawn( f, (obj::outdep<int>)data );
    spawn( g, (obj::indep<int>)data );
    ssync();
}


int main( int argc, char * argv[] ) {
  if( argc <= 1 ) {
    std::cerr << "Usage: " << argv[0] << " <n>\n";
    return 1;
  }
  int n = atoi( argv[1] );
  run( top, n );
  return 0;
}

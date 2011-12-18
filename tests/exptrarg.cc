// -*- c++ -*-
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <iostream>

#include "wf_interface.h"

using namespace obj;

void func( int * a ) {
    (*a)++;
}

int my_main( int argc, char * argv[] ) {
    int n = 0;

    spawn( func, &n );
    ssync();
    std::cout << "after spawn(), n=" << n << "\n";

    call( func, &n );
    std::cout << "after call(), n=" << n << "\n";

    leaf_call( func, &n );
    std::cout << "after leaf_call(), n=" << n << "\n";

    return 0;
}

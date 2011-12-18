// -*- c++ -*-
#include "wf_interface.h"

int main( int argc, char * argv[] ) {
    extern int my_main( int, char ** );
    return run( my_main, argc, argv );
}

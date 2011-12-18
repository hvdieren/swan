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

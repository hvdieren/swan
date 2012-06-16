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
#include "debug.h"

using namespace obj;

#if DEBUG_CERR
void iolock() { }
void iounlock() { }
#else
pthread_mutex_t io_lock_var;
void iolock() { pthread_mutex_lock( &io_lock_var ); }
void iounlock() { pthread_mutex_unlock( &io_lock_var ); }
#endif

struct quad_type {
    int a;
    int b;
    int c;
    int d;
};

namespace platform_x86_64 {
template<size_t ireg, size_t freg, size_t loff>
struct arg_passing<ireg, freg, loff, ::quad_type>
    : arg_passing_struct4<ireg, freg, loff, ::quad_type, int, int ,int, int> {
};
} // end namespace platform_x86_64

struct dual_type {
    int a;
    float b;
};

namespace platform_x86_64 {
template<size_t ireg, size_t freg, size_t loff>
struct arg_passing<ireg, freg, loff, ::dual_type>
    : arg_passing_struct2<ireg, freg, loff, ::dual_type, int, float> {
};
} // end namespace platform_x86_64

void many_args( int i,
		int a, int b, int c, int d, int f, int g, int h,
		int j, int k, int l, int m, int n ) {
    printf( "we're here! %d %d %d %d %d %d %d %d %d %d %d %d %d\n",
	    i, a, b, c, d, f, g, h, j, k, l, m, n );
}

void many_args2( int i,
		 int a, int b, int c, int d, int f, int g, int h,
		 int j, int k, int l, quad_type m, int n ) {
    printf( "we're here! %d %d %d %d %d %d %d %d %d %d %d %d/%d/%d/%d %d\n",
	    i, a, b, c, d, f, g, h, j, k, l, m.a, m.b, m.c, m.d, n );
}

void many_args3( void * ptr, int a, int b, quad_type c, quad_type d ) {
    printf( "we're here! %p %d %d %d/%d/%d/%d %d/%d/%d/%d\n",
	    ptr, a, b, c.a, c.b, c.c, c.d, d.a, d.b, d.c, d.d );
}

void many_args4( void * ptr, int a, int b, quad_type c, quad_type d,
		 int e ) {
    printf( "we're here! %p %d %d %d/%d/%d/%d %d/%d/%d/%d %d\n",
	    ptr, a, b, c.a, c.b, c.c, c.d, d.a, d.b, d.c, d.d, e );
}

void many_args5( quad_type c ) {
    printf( "we're here! %d/%d/%d/%d\n", c.a, c.b, c.c, c.d );
}

void two_args1( long i, float f ) {
    printf( "two_args1: %ld %f\n", i, f );
}

void two_args2( int i, float f ) {
    printf( "two_args2: %d %f\n", i, f );
}

void two_argsS( dual_type d ) {
    printf( "two_argsS: %d %f\n", d.a, d.b );
}

template<typename T>
const char * as_string() {
    return "unknown";
}

template<>
const char * as_string<platform_x86_64::ap_none_ty>() {
    return "ap_none_ty";
}

template<>
const char * as_string<platform_x86_64::ap_mem_ty>() {
    return "ap_mem_ty";
}

template<>
const char * as_string<platform_x86_64::ap_int_ty>() {
    return "ap_int_ty";
}

template<>
const char * as_string<platform_x86_64::ap_sse_ty>() {
    return "ap_sse_ty";
}

template<typename T0>
void describe_types() {
    std::cout << "Type: " << as_string<T0>() << "\n";
}

template<typename T0, typename T1, typename... T>
void describe_types() {
    std::cout << "Type: " << as_string<T0>() << "\n";
    describe_types<T1,T...>();
}

template<typename... T>
void describe( std::tuple<T...> ) {
    describe_types<T...>();
}

int main( int argc, char * argv[] ) {
#if !DEBUG_CERR
    pthread_mutex_init( &io_lock_var, NULL );
#endif

    if( argc <= 1 ) {
	std::cerr << "Usage: " << argv[0] << " <n>\n";
	return 1;
    }

    int n = atoi( argv[1] );
    quad_type p = { 100, 101, 102, 103 };
    quad_type q = { 200, 201, 202, 203 };

    std::cout << "mem_words many_args3: "
	      << platform_x86_64::count_mem_words<void *, int, int, quad_type,
	quad_type>() << "\n";
    std::cout << "mem_words many_args4: "
	      << platform_x86_64::count_mem_words<void *, int, int, quad_type,
	quad_type, int>() << "\n";
    std::cout << "mem_words many_args5: "
	      << platform_x86_64::count_mem_words<quad_type>() << "\n";


    run( many_args, n, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 );
    run( many_args2, n, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, q, 11 );
    run( many_args3, (void *)&n, 0, 1, p, q );
    run( many_args4, (void *)&n, 0, 1, p, q, 11 );
    run( many_args5, p );

    // dual_type d = { 1, 2 };

    // run( two_args1, (long)1, (float)2 );
    // run( two_args2, (int)1, (float)2 );
    // run( two_argsS, d );

    return 0;
}

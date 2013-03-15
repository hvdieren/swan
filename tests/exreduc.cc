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

using namespace obj;

// Output
#if DEBUG_CERR
void iolock() { }
void iounlock() { }
std::ostream & out() { return errs(); }
#else
cas_mutex io_lock_var;
void iolock() { io_lock_var.lock(); }
void iounlock() { io_lock_var.unlock(); }
std::ostream & out() { return std::cout; }
#endif

template<typename T>
struct or_monad {
    typedef T value_type;
    typedef cheap_reduction_tag reduction_tag;
    static void identity( T * p ) { new (p) T(0); }
    static void reduce( T * left, T * right ) { *left |= *right; }

    // Only for this test code
    static void action( T * p, int n ) { *p |= 1U << n; }
    static T peek( T * p ) { return *p; }
    static void action( T * p, T pk, int n ) { *p = pk | (1U << n); }
    static void write( char const * msg, T * p, bool = true, int n = 0 ) {
	out() << msg << ": " << std::hex << *p << std::dec << '\n';
	if( n > 0 && *p != (T(1)<<n)-T(1) ) {
	    std::cerr << "ERROR: " << msg << ": wrong value: val="
		      << std::hex << *p << std::dec << " correct="
		      << ((T(1)<<n)-T(1)) << "\n";
	    abort();
	}
    }
};

template<typename T>
struct add_monad {
    typedef T value_type;
    typedef cheap_reduction_tag reduction_tag;
    static void identity( T * p ) { new (p) T(0); }
    static void reduce( T * left, T * right ) { *left += *right; }

    // Only for this test code
    static void action( T * p, int n ) { *p += T(1U << n); }
    static T peek( T * p ) { return *p; }
    static void action( T * p, T pk, int n ) { *p = pk + T(1U << n); }
    static void write( char const * msg, T * p, bool = true, int n = 0 ) {
	out() << msg << ": " << *p << '\n';
	if( n > 0 && *p != T((1<<n)-1) ) {
	    std::cerr << "ERROR: " << msg << ": wrong value\n";
	    abort();
	}
    }
};

static size_t num_madd_actions = 0;

template<typename T, int D>
struct madd_monad {
    typedef T value_type[D*D];
    typedef expensive_reduction_tag reduction_tag;
    static void identity( value_type * p ) {
	out() << "identity on " << p << '\n';
	for( int i=0; i < D*D; ++i )
	    (*p)[i] = 0;
    }
    static void reduce( value_type * left, value_type * right ) {
	for( int i=0; i < D; ++i )
	    for( int j=0; j < D; ++j )
		(*left)[i*D+j] += (*right)[i*D+j];
    }

    // Only for this test code
    static void action( value_type * p, int n ) {
	for( int i=0; i < D; ++i )
	    (*p)[i*D+i] += 1.0;
	__sync_fetch_and_add( &num_madd_actions, 1 );
    }
    static T peek( value_type * p ) { return (*p)[0]; }
    static void action( value_type * p, T pk, int n ) {
	(*p)[0] = pk + 1.0;
	for( int i=1; i < D; ++i )
	    (*p)[i*D+i] += 1.0;
	__sync_fetch_and_add( &num_madd_actions, 1 );
    }
    static void write( char const * msg, value_type * p, bool check = true, int = 0 ) {
#if 0
	out() << msg << ":\n";
	for( int i=0; i < D; ++i ) {
	    for( int j=0; j < D; ++j )
		out() << (*p)[i*D+j] << ' ';
	    out() << '\n';
	}
#else
	if( !check ) {
	    out() << msg << ": do not check matrix (wasted)\n";
	    return;
	}

	bool error = false;
	for( int i=0; i < D; ++i )
	    if( (*p)[i*D+i] != T(num_madd_actions) ) {
		out() << "ERROR: " << msg << ": Element " << i << ", " << i
		      << " = " << (*p)[i*D+i]
		      << " differs from actions " << num_madd_actions
		      << "\n";
		error = true;
	    }
	if( !error )
	    out() << msg << ": matrix check ok\n";
	else
	    exit( 1 );
#endif
    }
};

template<typename M>
struct monad_name {
    static const char * get_name() { return "unknown"; }
};

template<>
struct monad_name< or_monad<unsigned int> > {
    static const char * get_name() { return "unsigned int, |"; }
};

template<>
struct monad_name< add_monad<unsigned int> > {
    static const char * get_name() { return "unsigned int, +"; }
};

template<>
struct monad_name< add_monad<double> > {
    static const char * get_name() { return "double, +"; }
};

template<size_t D>
struct monad_name< madd_monad<double, D> > {
    static const char * get_name() {
	static char str[128];
	snprintf( str, sizeof(str)-1, "double[%d][%d], +", D, D );
	return str;
    }
};

void delay( outdep<int> ) {
    usleep( 10000 );
    iolock();
    out() << "delay task done\n";
    iounlock();
}

void long_delay( outdep<int> ) {
    sleep( 1 );
    iolock();
    out() << "long delay task done\n";
    iounlock();
}

template<typename Monad>
void task( int n, indep<int>, reduction<Monad> r ) {
    usleep( 1000 );
    iolock();
    out() << "task " << n << " working on "
	  << r.get_version() << "\n";
    iounlock();
    Monad::action( (typename Monad::value_type*)r, n );
}

template<typename Monad>
void ptask( int n, indep<int>, reduction<Monad> r ) {
    extern __thread size_t threadid;

    iolock();
    out() << "ptask " << n << " working on "
	  << r.get_version() << " before spawn in thread "
	  << threadid << "\n";
    iounlock();
    
    auto peek = Monad::peek( (typename Monad::value_type*)r );

    object_t<int> d;
    // if( n%2 )
	// spawn( long_delay, (outdep<int>)d );
    // else
	spawn( delay, (outdep<int>)d );

    usleep( 100000 );
    Monad::action( (typename Monad::value_type*)r, peek, n );

    iolock();
    out() << "ptask " << n << " working on "
	  << r.get_version() << " after spawn in thread "
	  << threadid << "\n";
    iounlock();

    ssync();
}

template<typename Monad>
void waste( outdep<typename Monad::value_type> r ) {
    iolock();
    Monad::write( "waste reduction output", (typename Monad::value_type*)r, false );
    iounlock();
}

template<typename Monad>
void output( indep<typename Monad::value_type> r, bool b, int i ) {
    iolock();
    Monad::write( "reduction from task", (typename Monad::value_type*)r, b, i );
    iounlock();
}

template<typename Monad>
void between( inoutdep<typename Monad::value_type> r ) {
    iolock();
    Monad::write( "between: reduction from task", (typename Monad::value_type*)r );
    iounlock();
}

// Scenario's:
// cheap vs expensive
// reduction followed by in/inout/cinout
// reduction followed by out
// reduction followed by sync
template<typename Monad>
void test( int nr, int ni, int dd ) {
    object_t<typename Monad::value_type> redu;
    object_t<int> d;

    Monad::identity( (typename Monad::value_type*)redu );

    out() << "Reduction test harness: reduction-tasks=" << (nr>0?nr:-nr)
	  << " parallel=" << (nr<0?"yes":"no")
	  << " indep-tasks=" << (ni>0?ni:0)
	  << " outdep-tasks=" << (ni<0?-ni:0)
	  << " with delay task=" << dd
	  << " using monad " << monad_name<Monad>::get_name()
	  << '\n';

    if( dd )
	spawn( delay, (outdep<int>)d );
    if( nr > 0 )
	for( int i=0; i < nr; ++i )
	    spawn( task<Monad>, i, (indep<int>)d, (reduction<Monad>)redu );
    else if( nr < 0 )
	for( int i=0; i < -nr; ++i )
	    spawn( ptask<Monad>, i, (indep<int>)d, (reduction<Monad>)redu );
    if( ni > 0 )
	for( int i=0; i < ni; ++i )
	    spawn( output<Monad>, (indep<typename Monad::value_type>)redu,
		   true, nr>0?nr:-nr );
    else if( ni < 0 )
	for( int i=0; i < -ni; ++i )
	    spawn( waste<Monad>, (outdep<typename Monad::value_type>)redu );
    ssync();

    Monad::write( "reduction after sync",
		  (typename Monad::value_type*)redu, ni >= 0, nr>0?nr:-nr );
}

template<typename Monad>
void test2( int nr, int ni, int dd ) {
    object_t<typename Monad::value_type> redu;
    object_t<int> d;

    Monad::identity( (typename Monad::value_type*)redu );

    out() << "Reduction test harness 2: reduction-tasks=" << (nr>0?nr:-nr)
	  << " parallel=" << (nr<0?"yes":"no")
	  << " indep-tasks=" << (ni>0?ni:0)
	  << " outdep-tasks=" << (ni<0?-ni:0)
	  << " with delay task=" << dd
	  << " using monad " << monad_name<Monad>::get_name()
	  << '\n';

    if( dd )
	spawn( delay, (outdep<int>)d );
    for( int i=0; i < nr; ++i )
	spawn( task<Monad>, i, (indep<int>)d, (reduction<Monad>)redu );
    spawn( between<Monad>, (inoutdep<typename Monad::value_type>)redu );
    for( int i=0; i < nr; ++i )
	spawn( task<Monad>, nr+i, (indep<int>)d, (reduction<Monad>)redu );
    if( ni > 0 )
	for( int i=0; i < ni; ++i )
	    spawn( output<Monad>, (indep<typename Monad::value_type>)redu,
		   true, 2*(nr>0?nr:-nr) );
    else if( ni < 0 )
	for( int i=0; i < -ni; ++i )
	    spawn( waste<Monad>, (outdep<typename Monad::value_type>)redu );
    ssync();

    Monad::write( "reduction after sync", (typename Monad::value_type*)redu, ni >= 0, 2*(nr>0?nr:-nr) );

    for( int i=0; i < nr; ++i )
	spawn( task<Monad>, 2*nr+i, (indep<int>)d, (reduction<Monad>)redu );
    ssync();
    Monad::write( "reduction after 2nd sync",
		  (typename Monad::value_type*)redu, ni >= 0, 3*(nr>0?nr:-nr) );
}


void wrap( void (*fn)(int, int,int), int nr, int ni, int dd ) {
    call( fn, nr, ni, dd );
}

template<typename M>
void testcase( int nr, int ni, int wr, int dd ) {
    if( wr )
	run<void, void(*)(int,int,int), int, int, int>(
	    &wrap, &test<M>, nr, ni, dd );
    else
	run( test<M>, nr, ni, dd );
}

template<typename M>
void testcase2( int nr, int ni, int wr, int dd ) {
    if( wr )
	run<void, void(*)(int,int,int), int, int, int>(
	    &wrap, &test2<M>, nr, ni, dd );
    else
	run( test2<M>, nr, ni, dd );
}

int main( int argc, char * argv[] ) {
    if( argc <= 5 ) {
	std::cerr << "Usage: " << argv[0]
		  << " <monad> <num-redu> <num-in> <wrap?> <delay?>\n";
	return 1;
    }

    int t = atoi( argv[1] );
    int nr = atoi( argv[2] );
    int ni = atoi( argv[3] );
    int wr = atoi( argv[4] );
    int dd = atoi( argv[5] );
    switch( t ) {
    case 0:
	testcase< or_monad<unsigned int> >( nr, ni, wr, dd );
	break;
    case 1:
	testcase< add_monad<unsigned int> >( nr, ni, wr, dd );
	break;
    case 2:
	testcase< add_monad<double> >( nr, ni, wr, dd );
	break;
    case 3:
	testcase< madd_monad<double,16> >( nr, ni, wr, dd );
	break;
    case 4:
	testcase2< or_monad<unsigned int> >( nr, ni, wr, dd );
	break;
    case 5:
	testcase2< add_monad<unsigned int> >( nr, ni, wr, dd );
	break;
    case 6:
	testcase2< add_monad<double> >( nr, ni, wr, dd );
	break;
    case 7:
	testcase2< madd_monad<double,16> >( nr, ni, wr, dd );
	break;
    }

    return 0;
}

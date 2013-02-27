// -*- c++ -*-

#include <stdio.h>
#include <stdlib.h>
#include "swan/alc_allocator.h"
#include "swan/alc_mmappol.h"
#include "swan/alc_flpol.h"

using namespace std;

namespace obj {

#define MAX_SIZE	128

class fixed_size_queue
{
    volatile unsigned head;
    pad_multiple<CACHE_ALIGNMENT, sizeof(unsigned)> pad0;
    volatile unsigned tail;
    pad_multiple<CACHE_ALIGNMENT, sizeof(unsigned)> pad1;
    char * buffer;
    pad_multiple<CACHE_ALIGNMENT, sizeof(char*)> pad3;
    q_typeinfo   tinfo;
    volatile unsigned size;
    volatile unsigned mask;
    volatile unsigned tinfo_size;
    volatile unsigned actual_size;
    pad_multiple<CACHE_ALIGNMENT, 4*sizeof(unsigned) + sizeof(q_typeinfo)> pad2;
	
private:
    static unsigned log2_up( unsigned uu ) {
	volatile unsigned u = uu;
	if( u == 0 )
	    return 1;
	else {
	    volatile unsigned l = 0;
	    while( u > 0 ) {
		u >>= 1;
		l++;
	    }
	    if( (uu & (uu-1)) == 0 )
		return l-1;
	    else
		return l;
	}
    }
	
public:
    fixed_size_queue(q_typeinfo tinfo_) : tinfo( tinfo_ ) { 
	static_assert( sizeof(fixed_size_queue) % 64 == 0, "padding failed" );
	tinfo_size = tinfo.get_size();
	actual_size = tinfo.get_actual_size();
	head = 0;
	tail = 0;
	size = MAX_SIZE*tinfo_size;
	volatile unsigned log_size = log2_up( size );
	assert( log_size > 0 && (1<<(log_size-1)) < size && size <= (1<<log_size) );
	if(!(log_size > 0 && (1<<(log_size-1)) < size && size <= (1<<log_size) )) {
	    cout<<"Assertion failed log_size > 0 && (1<<(log_size-1)) < size && size <= (1<<log_size) "<<endl;
	    exit(1);
	}
	size = 1 << log_size;
	mask = size - 1;
	buffer = new char[size];
    }
	
    ~fixed_size_queue() {
	static_assert( sizeof(fixed_size_queue) % 64 == 0, "padding failed" );
	delete[] buffer;
	buffer = NULL;
    }
    inline void * operator new ( size_t size );
    inline void operator delete( void * p );
	
    bool empty() const volatile {
	return (head == tail);
    }
    bool full() const volatile {
	return (((tail+tinfo_size) & mask) == head);
    }
	
    //returns NULL if pop fails
    char* pop() volatile {
	if( empty() ) {
	    return NULL;
	} else {
	    char* value = buffer+head;
	    head = (head+tinfo_size) & mask;
	    return value;
	}
    }
	
    //returns true on success false on failure
    bool push( char* value ) volatile {
	if( full() ) {
	    return false;
	} else {
	    tinfo.copy( buffer+tail, value );
	    //free value, we don't need this anymore
	    tinfo.destruct(value); // HV: this is the wrong place to do this
	    tail = (tail+tinfo_size) & mask;
	    return true;
	}
    }
    char* front() volatile {
	return (buffer+head);
    }
};

}//namespace obj

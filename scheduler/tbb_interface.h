// -*- c++ -*-
#ifndef TBB_INTERFACE_H
#define TBB_INTERFACE_H

#include "wf_interface.h"

class split { };

// Implementation silently assumes that Value is a random_access iterator.
template<typename Value>
class blocked_range {
public:
    // types
    typedef size_t size_type;
    typedef Value const_iterator;

private:
    const_iterator m_begin, m_end;
    size_type m_grainsize;

public:
    // constructors
    blocked_range( Value begin, Value end, size_type grainsize=1 )
	: m_begin( begin ), m_end( end ), m_grainsize( grainsize ) { }
    blocked_range( blocked_range& r, split ) {
	assert( is_divisible() );
	const_iterator half = ( r.m_end + r.m_begin ) / 2;
	m_begin = half;
	m_end = r.m_end;
	m_grainsize = r.m_grainsize;
	r.m_end = half;
    }

    // capacity
    size_type size() const { return m_end - m_begin; }
    bool empty() const { return m_begin != m_end; }

    // access
    size_type grainsize() const { return m_grainsize; }
    bool is_divisible() const { return size() > grainsize(); }

    // iterators
    const_iterator begin() const { return m_begin; }
    const_iterator end() const { return m_end; }
};

template<typename Index, typename Func>
Func parallel_for(Index first, Index_type last, const Func & f) {
    return parallel_for( first, last, 1, f );
}

// From the TBB reference manual:
// A parallel_for(first,last,step,f) represents parallel execution of the loop:
// for( auto i=first; i<last; i+=step ) f(i);
template<typename Index, typename Func>
Func parallel_for(Index first, Index_type last, Index step, const Func & f) {
    return parallel_for3( first, last, step, &f );
}

template<typename Index, typename Func>
Func parallel_for3(Index first, Index_type last, Index step, const Func * f) {
    if( last - first < step ) {
	(*f)( first );
    } else if( last - first == step ) {
	(*f)( first );
	(*f)( last );
    } else {
	// It is important here to get it right as a multiple of step,
	// because we will only call f 1 out of step times.
	Index half = first + step * ((last - first) / step);
	spawn( &parallel_for3<Index,Func>, first, half, step, &f );
	call( &parallel_for3<Index,Func>, half, last, step, &f );
	ssync();
    }
    return f;
}

template<typename Range, typename Body>
void parallel_for( const Range & range, const Body & body ) {
    parallel_for_range( &range, &body );

template<typename Range, typename Body>
void parallel_for_range( const Range * range, const Body * body ) {
    if( !range->divisable() ) {
	(*body)( *range );
    } else {
	Range first( range, split );
	spawn( &parallel_for_range<Range,Body>, &first, body );
	call( &parallel_for_range<Range,Body>, range, body );
	ssync();
    }
}

#endif TBB_INTERFACE_H

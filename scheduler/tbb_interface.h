// -*- c++ -*-
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
    blocked_range( const blocked_range & r )
	: m_begin( r.m_begin ), m_end( r.m_end ),
	  m_grainsize( r.m_grainsize ) { }
    blocked_range( Value begin, Value end, size_type grainsize=1 )
	: m_begin( begin ), m_end( end ), m_grainsize( grainsize ) { }
    blocked_range( blocked_range& r, split ) {
	assert( r.is_divisable() );
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
    bool is_divisable() const { return size() > grainsize(); }

    // iterators
    const_iterator begin() const { return m_begin; }
    const_iterator end() const { return m_end; }
};

// Implementation silently assumes that Value is a random_access iterator.
template<typename RowValue, typename ColValue=RowValue>
class blocked_range2d {
public:
    // types
    typedef blocked_range<RowValue> row_range_type;
    typedef blocked_range<ColValue> col_range_type;

private:
    row_range_type m_row;
    col_range_type m_col;

public:
    // constructors
    blocked_range2d( RowValue row_begin, RowValue row_end,
		     typename row_range_type::size_type row_grainsize,
		     ColValue col_begin, ColValue col_end,
		     typename col_range_type::size_type col_grainsize )
	: m_row( row_begin, row_end, row_grainsize ),
	  m_col( col_begin, col_end, col_grainsize ) { }
    blocked_range2d( RowValue row_begin, RowValue row_end,
		     ColValue col_begin, ColValue col_end )
	: m_row( row_begin, row_end ),
	  m_col( col_begin, col_end ) { }
    blocked_range2d( blocked_range2d& r, split s )
	: m_row( r.rows() ), m_col( r.cols() ) {
	assert( r.is_divisable() );
	if( r.rows().size() * r.cols().grainsize()
	    > r.cols().size() * r.rows().grainsize()
	    && r.rows().is_divisable() ) { // split rows
	    m_row = row_range_type( r.m_row, s );
	} else if( r.cols().is_divisable() ) { // split cols
	    m_col = col_range_type( r.m_col, s );
	} else
	    assert( 0 );
    }

    // capacity
    bool empty() const { return m_row.empty() || m_col.empty(); }

    // access
    bool is_divisable() const {
	return m_row.is_divisable() || m_col.is_divisable();
    } 
    const row_range_type & rows() const { return m_row; }
    const col_range_type & cols() const { return m_col; }
};

template<typename Index, typename Func>
Func parallel_for(Index first, Index last, const Func & f) {
    return parallel_for( first, last, 1, f );
}

// From the TBB reference manual:
// A parallel_for(first,last,step,f) represents parallel execution of the loop:
// for( auto i=first; i<last; i+=step ) f(i);
template<typename Index, typename Func>
Func parallel_for(Index first, Index last, Index step, const Func & f) {
    return parallel_for3( first, last, step, &f );
}

template<typename Index, typename Func>
Func parallel_for3(Index first, Index last, Index step, const Func * f) {
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
}

template<typename Range, typename Body>
void parallel_for_range( const Range * range, const Body * body ) {
    if( !range->is_divisable() ) {
	(*body)( *range );
    } else {
	Range second = *range;
	Range first( second, split() );
	// void (*f)(const Range *, const Body *) = &parallel_for_range<Range,Body>;
	spawn( &parallel_for_range<Range,Body>, (const Range *)&first, body );
	call( &parallel_for_range<Range,Body>, (const Range *)&second, body );
	ssync();
    }
}

#endif // TBB_INTERFACE_H

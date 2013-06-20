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

// -*- C++ -*-
#ifndef LOGGER_H
#define LOGGER_H

#include "swan_config.h"

#include <cassert>

class logger {
public:
    struct clue {
	char const * id;
	void * value;
	char const * file;
	size_t lineno;

	clue() { }
	template<typename T>
	clue( char const * id_, T value_,
	      char const * file_ = __FILE__, size_t lineno_ = __LINE__ )
	    : id( id_ ), value( (void *)(intptr_t)(value_) ), file( file_ ), lineno( lineno_ ) {
	}
    };

private:
    clue * clues;
    size_t nclues;
    size_t nalloc;
    static const size_t Chunk = 4096;

public:
    logger() : nclues( 0 ), nalloc( Chunk ) {
	clues = new clue[Chunk];
    }
    ~logger() {
	delete[] clues;
    }

    void log( const clue & c ) {
	// assert( nclues < nalloc && "Ran out of log space" );
	if( nclues >= nalloc ) // wrap-around
	    nclues = 0;
	clues[nclues++] = c;
    }

    static void glog( const clue & c ) {
	extern __thread logger * tls_thread_logger;
	tls_thread_logger->log( c );
    }
};

#if DBG_LOGGER
#define LOG(id,value) logger::glog( logger::clue( #id, (value), __FILE__, __LINE__ ) )
#else
#define LOG(id,value)
#endif

#endif // LOGGER_H

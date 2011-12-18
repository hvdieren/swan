// -*- C++ -*-
#ifndef LOGGER_H
#define LOGGER_H

#include "config.h"

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

/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.org)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.org)
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

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <signal.h>
#include "pp_time.h"

#ifndef USE_RDTSC
#define USE_RDTSC 1
#endif

#ifndef DBG
#define DBG 0           /* higher debug level = more output */
#endif

#define printv(lvl, fmt, ...) {if(lvl <= DBG) printf(fmt, ##__VA_ARGS__); }

#if USE_RDTSC
#include "rdtsc.h"
#endif

static pp_time_t * report = NULL;

#if USE_RDTSC
unsigned long long pp_time()
{
    unsigned long long cycles = rdtsc();
    // return (unsigned long long)( (float)cycles / 2126.455 );
    return cycles;
}
#else
unsigned long long pp_time()
{
    struct timeval tv;

    gettimeofday( &tv, 0 );

    return (unsigned long long)tv.tv_usec
	+ 1000000ULL*(unsigned long long)tv.tv_sec;
}
#endif

void pp_time_print( pp_time_t * t, char * region )
{
    float f = t->measurements == 0 ? 0 : t->total / t->measurements;
	fprintf( stderr, "Time spent in %s: %llu %s, %.2f avg, %lu calls %lu drops\n",
		 region ? region : "unnamed", t->total,
		 USE_RDTSC ? "cycles" : "usec",
		 f, t->measurements, t->drops );
	t = t->next;
}

static
void pp_time_dump( int sig )
{
    pp_time_t * t = report;
    float f;

    while( t ) {
        f = t->measurements == 0 ? 0 : t->total / t->measurements;
	fprintf( stderr, "Time spent in %s: %llu %s, %.2f avg, %lu calls %lu drops\n",
		 t->region ? t->region : "unnamed", t->total,
		 USE_RDTSC ? "cycles" : "usec",
		 f, t->measurements, t->drops );
	t = t->next;
    }
}

char const * pp_time_unit()
{
    return USE_RDTSC ? "cycles" : "usec";
}

double pp_time_read( pp_time_t * t )
{
    return t->measurements == 0
	? 0 : (double)t->total / (double)t->measurements;
}

void pp_time_report( pp_time_t * t, const char * region )
{
    int first_add = report == 0;

    t->total = 0;
    t->measurements = 0;
    t->drops = 0;
    t->region = region;
    t->next = report;
    report = t;

    if( first_add ) {
	atexit( (void (*)(void))&pp_time_dump );
	signal( SIGUSR2, pp_time_dump );
    }
}

void pp_time_start( pp_time_t * t )
{
    t->last = pp_time();
}

void pp_time_end( pp_time_t * t )
{
    unsigned long long d = pp_time() - t->last;
    if( (long long)d < 0LL ) {
	t->drops++;
    } else {
	t->total += d;
	t->measurements++;
	printv( 1, "%s:%lu: %llu %.2f\n", t->region, t->measurements, d,
		(float)t->total/(float)t->measurements );
    }
}

void pp_time_max( pp_time_t * m, const pp_time_t * v ) {
    if( v->total > m->total )
        *m = * v;
}

void pp_time_add( pp_time_t * m, const pp_time_t * v ) {
    m->total += v->total;
    m->measurements += v->measurements;
    m->drops += v->drops;
}

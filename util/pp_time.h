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

#ifndef PP_TIME_H
#define PP_TIME_H

#if defined(__cpluplus) || defined(__GNUG__)
extern "C" {
#endif

struct pp_time_t {
    unsigned long long total;
    unsigned long long last;
    unsigned long measurements;
    unsigned long drops;
    const char * region;
    struct pp_time_t * next;
};

typedef struct pp_time_t pp_time_t;

unsigned long long pp_time();

void pp_time_report( pp_time_t * t, const char * region );
void pp_time_start( pp_time_t * t );
void pp_time_end( pp_time_t * t );

char const * pp_time_unit();
double pp_time_read( pp_time_t * t );

void pp_time_print( pp_time_t * t, char * region );

void pp_time_max( pp_time_t * m, const pp_time_t * v );
void pp_time_add( pp_time_t * m, const pp_time_t * v );

#if defined(__cpluplus) || defined(__GNUG__)
}
#endif

#endif /* PP_TIME_H */

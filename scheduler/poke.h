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

// -*- c++ -*-
#ifndef POKE_H
#define POKE_H

inline void poke(void)  __attribute__((always_inline));

inline void poke(void) {
    void * g_esp, * g_ebp, * g_ebx;
    GET_BP( g_ebp );
    GET_SP( g_esp );
    GET_PR( g_ebx );

    std::cerr << "poke esp: " << g_esp << "\n"
	      << "poke ebp: " << g_ebp << "\n"
	      << "poke ebx: " << g_ebx << "\n";
}

#endif // POKE_H

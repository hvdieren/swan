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
#ifndef PADDING_H
#define PADDING_H

#include "swan_config.h"

#include <cassert>

template<size_t nbytes>
struct padding {
private:
    char padding[nbytes];
};

template<>
struct padding<0> {
};

template<size_t multi, size_t nbytes>
struct pad_multiple {
private:
    char padding[(multi-(nbytes%multi))%multi];
};

// ---------------------------------------------------------------------
// Aligning objects of a class.
// ---------------------------------------------------------------------
template<typename Base, size_t Alignment>
class aligned_class : public Base {
    // Padding
    static const size_t data_size = sizeof(Base);
    pad_multiple<Alignment,data_size> padding;

    void pad_check() const {
	static_assert( (sizeof(*this) % Alignment) == 0,
		       "template instantiation of aligned_mutex: "
		       "alignment failed" );
    }
};


#endif // PADDING_H

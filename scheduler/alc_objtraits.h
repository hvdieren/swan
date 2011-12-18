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
#ifndef ALC_OBJTRAITS_H
#define ALC_OBJTRAITS_H

// Based on http://www.codeproject.com/KB/cpp/allocator.aspx

namespace alc {

template<typename T>
class object_traits_base {
public : 
    // convert an object_traits<T> to object_traits<U>
    template<typename U>
    struct rebind {
        typedef object_traits_base<U> other;
    };

public : 
    inline explicit object_traits_base() {}
    inline ~object_traits_base() {}
    template <typename U>
    inline explicit object_traits_base(object_traits_base<U> const&) {}

    // address
    inline T* address(T& r) { return &r; }
    inline T const* address(T const& r) { return &r; }

    inline void construct(T* p, const T& t) { new(p) T(t); }
    inline void destroy(T* p) { p->~T(); }
};

template<typename T>
class object_traits : public object_traits_base<T> {
public : 
    inline explicit object_traits() : object_traits_base<T>() {}
    inline ~object_traits() {}
    template <typename U>
    inline explicit object_traits(object_traits<U> const&ref)
	: object_traits_base<T>(ref) {}

};

};

#endif // ALC_OBJTRAITS_H

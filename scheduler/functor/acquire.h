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

/* functor/acquire.h
 * This file implements a common 'acquire' functor, and function.
 * It is parameterized on task graph 'MetaData' and assumes that such a
 * MetaData class implements:
 *    bool commutative_try_acquire();
 *    void commutative_release();
 */
#ifndef FUNCTOR_ACQUIRE_H
#define FUNCTOR_ACQUIRE_H

namespace obj {

// ----------------------------------------------------------------------
// Functor for acquiring locks (commutativity) and privatization (reductions)
// when selecting an otherwise ready task.
// ----------------------------------------------------------------------
template<typename MetaData>
struct acquire_functor {
    // Default case is do nothing
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) {
	return true;
    }
    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) { }

    // Commutativity
#if OBJECT_COMMUTATIVITY
    template<typename T>
    bool operator () ( cinoutdep<T> & obj,
		       typename cinoutdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	return md->commutative_try_acquire();
    }
    template<typename T>
    void undo( cinoutdep<T> & obj, typename cinoutdep<T>::dep_tags & sa ) {
	obj.get_version()->get_metadata()->commutative_release();
    }
#endif
};

// An acquire and privatize function
#if STORED_ANNOTATIONS
template<typename MetaData>
static inline bool arg_acquire_fn( task_data_t & td ) {
    acquire_functor<MetaData> fn;
    char * args = td.get_args_ptr();
    char * tags = td.get_tags_ptr();
    size_t nargs = td.get_num_args();
    if( arg_apply_stored_fn( fn, nargs, args, tags ) ) {
	finalize_functor<MetaData> ffn( td );
	arg_apply_stored_ufn( ffn, nargs, args, tags );
	privatize_functor<MetaData> pfn;
	arg_apply_stored_ufn( pfn, nargs, args, tags );
	return true;
    }
    return false;
}
#else
template<typename MetaData, typename... Tn>
static inline bool arg_acquire_fn( task_data_t & td ) {
    acquire_functor<MetaData> fn;
    char * args = td.get_args_ptr();
    char * tags = td.get_tags_ptr();
    if( arg_apply_fn<acquire_functor<MetaData>,Tn...>( fn, args, tags ) ) {
	finalize_functor<MetaData> ffn( td );
	arg_apply_ufn<finalize_functor<MetaData>,Tn...>( ffn, args, tags );
	privatize_functor<MetaData> pfn;
	arg_apply_ufn<privatize_functor<MetaData>,Tn...>( pfn, args, tags );
	return true;
    }
    return false;
}
#endif

} // end of namespace obj

#endif // FUNCTOR_ACQUIRE_H

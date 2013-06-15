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

/* functor/tkt_ready.h
 * This file implements a functor for checking readiness, and finalizing
 * and privatizing reductions in a ticket task graph scheme.
 */
#ifndef FUNCTOR_TKT_READY_H
#define FUNCTOR_TKT_READY_H

namespace obj {

// ----------------------------------------------------------------------
// Checking readiness of pending_metadata
// ----------------------------------------------------------------------
// Ready? functor
template<typename MetaData_, typename Task>
struct ready_functor {
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	return dep_traits<MetaData, Task, DepTy>::arg_ready( obj, sa );
    }
    template<typename T>
    bool operator () ( truedep<T> & obj, typename truedep<T>::dep_tags & sa ) {
	return true;
    }

    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) { }
#if OBJECT_COMMUTATIVITY
    template<typename T>
    void undo( cinoutdep<T> & obj, typename cinoutdep<T>::dep_tags & sa ) {
	obj.get_version()->get_metadata()->commutative_release();
    }
#endif
};


// A "ready function" to check readiness with the dep_traits.
#if STORED_ANNOTATIONS
template<typename MetaData, typename Task>
static inline bool arg_ready_fn( const task_data_t & task_data ) {
    ready_functor<MetaData, Task> fn;
    char * args = task_data.get_args_ptr();
    char * tags = task_data.get_tags_ptr();
    size_t nargs = task_data.get_num_args();
    if( arg_apply_stored_ufn( fn, nargs, args, tags ) ) {
	finalize_functor<MetaData> ffn;
	arg_apply_stored_ufn( ffn, nargs, args, tags );
	privatize_functor<MetaData> pfn;
	arg_apply_stored_ufn( pfn, nargs, args, tags );
	return true;
    }
    return false;
}
#else
template<typename MetaData, typename Task, typename... Tn>
static inline bool arg_ready_fn( const task_data_t & task_data ) {
    ready_functor<MetaData, Task> fn;
    char * args = task_data.get_args_ptr();
    char * tags = task_data.get_tags_ptr();
    if( arg_apply_ufn<ready_functor<MetaData, Task>,Tn...>( fn, args, tags ) ) {
	// The finalization is not performed if task_data indicates that none
	// of the arguments are the result of a non-finalized reduction.
	finalize_functor<MetaData> ffn( task_data );
	arg_apply_ufn<finalize_functor<MetaData>,Tn...>( ffn, args, tags );
	// The privatization code optimizes to a no-op if it is not required
	privatize_functor<MetaData> pfn;
	arg_apply_ufn<privatize_functor<MetaData>,Tn...>( pfn, args, tags );
	return true;
    }
    return false;
}
#endif

} // end of namespace obj

#endif // FUNCTOR_TKT_READY_H

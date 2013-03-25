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

/*
 * argwalk.h
 *
 * This file provides basic mechanisms to walk the argument list of a task.
 * There are two implementations: a high-performance type-based implementation
 * and a slower (baseline) implementation which stores types as data in memory.
 */
#ifndef ARGWALK_H
#define ARGWALK_H

#include "platform.h"
#include "wf_task.h"

namespace obj {

// ------------------------------------------------------------------------
// Current limitations
// ------------------------------------------------------------------------
#if STORED_ANNOTATIONS && OBJECT_REDUCTION
#error STORED_ANNOTATIONS + OBJECT_REDUCTION combination currently not allowed
#endif

// ------------------------------------------------------------------------
// Auxiliary to step through a list {in,out,inout}dep_traits structures
// that corresponds to an argument list typename... Tn
// ------------------------------------------------------------------------
template<typename T, class IsObject = void>
struct arg_stored_size_single {
#if STORED_ANNOTATIONS
    static const size_t value = sizeof(annotation_tags);
#else
    static const size_t value = 0;
#endif
};

template<typename T>
struct arg_stored_size_single<T, typename std::enable_if< is_object<T>::value >::type> {
    static const size_t value = sizeof(typename T::dep_tags);
};

template<typename... Tn>
struct arg_stored_size_helper;

template<>
struct arg_stored_size_helper<> {
    static const size_t value = 0;
};

template<typename T, typename... Tn>
struct arg_stored_size_helper<T, Tn...> : public arg_stored_size_helper<Tn...> {
    static const size_t value =
    	arg_stored_size_single<T>::value
	+ arg_stored_size_helper<Tn...>::value;
};

template<typename... Tn>
inline size_t
arg_stored_size() {
    return arg_stored_size_helper<Tn...>::value; // + fn_stored_size<Tfdep>();
}

template<typename Tfdep>
inline size_t
fn_stored_size() {
    return std::is_empty<Tfdep>::value ? 0 : sizeof( Tfdep );
}

template<typename Tfdep>
inline Tfdep * get_fn_tags( char * tags ) {
    return reinterpret_cast<Tfdep *>(
	std::is_empty<Tfdep>::value ? (char *)0 : (tags - sizeof(Tfdep)) );
}

// ------------------------------------------------------------------------
// Applicator base case
// ------------------------------------------------------------------------
// obj_int and tag
template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value, bool>::type
apply_functor( Fn &, char * __restrict__, char * __restrict__ ) { return true; }

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value
			       && !is_outdep<T>::value, bool>::type
apply_functor( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp ) {
    return fn( *reinterpret_cast<T*>( argp ),
	       *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

template<typename Fn, typename T>
inline typename std::enable_if<is_outdep<T>::value, bool>::type
apply_functor( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp ) {
    T * obj = reinterpret_cast<T*>( argp );
    if( likely( obj->get_version()->is_versionable() ) )
	return fn( *reinterpret_cast<T*>( argp ),
		   *reinterpret_cast<typename T::dep_tags *>( tagp ) );
    else {
	typedef inoutdep<typename T::elem_type> U;
	return apply_functor<Fn,U>( fn, argp, tagp );
    }
}

template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value>::type
apply_undo( Fn &, char * __restrict__, char * __restrict__ ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value
			       && !is_cinoutdep<T>::value>::type
apply_undo( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_cinoutdep<T>::value>::type
apply_undo( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp ) {
    fn.undo( *reinterpret_cast<T*>( argp ),
	     *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

// obj_ext, obj_int and tag
template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value, bool>::type
apply3_functor( Fn &, char * __restrict__, char * __restrict__, T & ) {
    return true;
}

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value && !is_outdep<T>::value,
			       bool>::type
apply3_functor( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp,
		T & t ) {
    return fn( t, *reinterpret_cast<T*>( argp ),
	       *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

template<typename Fn, typename T>
inline typename std::enable_if<is_outdep<T>::value, bool>::type
apply3_functor( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp,
		T & t ) {
    if( likely( t.get_version()->is_versionable() ) )
	return fn( t, *reinterpret_cast<T*>( argp ),
		   *reinterpret_cast<typename T::dep_tags *>( tagp ) );
    else {
	typedef inoutdep<typename T::elem_type> U;
	U u = U::create( t.get_version() );
	return apply3_functor<Fn,U>( fn, argp, tagp, u );
    }
}

template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value>::type
apply3_undo( Fn &, char * __restrict__, char * __restrict__, T & ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value
                               && !is_cinoutdep<T>::value>::type
apply3_undo( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp,
	     T & t ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_cinoutdep<T>::value>::type
apply3_undo( Fn & fn, char * __restrict__ argp, char * __restrict__ tagp,
	     T & t ) {
    fn.undo( t, *reinterpret_cast<T*>( argp ),
	     *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

// obj_ext and tag
template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value, bool>::type
dapply_functor( Fn &, char * __restrict__, T & ) { return true; }

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value && !is_outdep<T>::value,
			       bool>::type
dapply_functor( Fn & fn, char * __restrict__ tagp, T & t ) {
    return fn( t, *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

template<typename Fn, typename T>
inline typename std::enable_if<is_outdep<T>::value, bool>::type
dapply_functor( Fn & fn, char * __restrict__ tagp, T & t ) {
    if( likely( t.get_version()->is_versionable() ) )
	return fn( t, *reinterpret_cast<typename T::dep_tags *>( tagp ) );
    else {
	typedef inoutdep<typename T::elem_type> U;
	U u = U::create( t.get_version() );
	return dapply_functor<Fn,U>( fn, tagp, u );
    }
}

template<typename Fn, typename T>
inline typename std::enable_if<!is_object<T>::value>::type
dapply_undo( Fn &, char * __restrict__, T & ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_object<T>::value
			       && !is_cinoutdep<T>::value>::type
dapply_undo( Fn & fn, char * __restrict__ tagp, T & t ) { }

template<typename Fn, typename T>
inline typename std::enable_if<is_cinoutdep<T>::value>::type
dapply_undo( Fn & fn, char * __restrict__ tagp, T & t ) {
    fn.undo( t, *reinterpret_cast<typename T::dep_tags *>( tagp ) );
}

#if STORED_ANNOTATIONS

// ------------------------------------------------------------------------
//        *** APPLICATOR IMPLEMENTATIONS FOR STORED ANNOTATIONS ***
//
// Applicators provided:
// arg_stored_init_fn(): store annotations to memory at spawn time
// arg_apply3_stored_fn<>(): for immediate issue with potential renaming
// arg_apply_stored_fn<>(): all others, operating on the stored annotations
// arg_apply_stored_ufn<>(): same as above, but call undo function on failure
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Allows building all kinds of functors to build callee-specific functions
// that access all three of copied arguments, tags and real arguments.
// ------------------------------------------------------------------------
template<typename Fn>
static inline bool arg_apply3_stored_fn( Fn & fn, size_t nargs, char * __restrict__ argp, char * __restrict__ tagp ) {
    return true;
}

template<typename Fn, typename T, typename... Tn>
static inline bool arg_apply3_stored_fn( Fn & fn, size_t nargs, char * __restrict__ argp, char * __restrict__ tagp, T & a0, Tn & ... an ) {
    annotation_tags * at = reinterpret_cast<annotation_tags*>( tagp );
    switch( at->annot ) {
    case a_none:
	argp += at->arg_step;
	tagp += sizeof(annotation_tags);
	break;
    case a_in:
	typedef T any_in;
	if( !apply3_functor<Fn,any_in>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_in>();
	break;
    case a_out:
	typedef T any_out;
	if( !apply3_functor<Fn,any_out>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_out>();
	break;
    case a_inout:
	typedef T any_inout;
	if( !apply3_functor<Fn,any_inout>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_inout>();
	break;
    case a_true:
	typedef T any_true;
	if( !apply3_functor<Fn,any_true>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_true>();
	break;
#if OBJECT_COMMUTATIVITY
    case a_cinout:
	typedef T any_cinout;
	if( !apply3_functor<Fn,any_cinout>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_cinout>();
	break;
#endif
#if OBJECT_REDUCTION
    case a_reduct_cheap:
    {
	typedef T any_reduct;
	if( !apply3_functor<Fn,any_reduct>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_reduct>();
	break;
    }
    case a_reduct_expensive:
    {
	typedef T any_reduct;
	if( !apply3_functor<Fn,any_reduct>( fn, argp, tagp, a0 ) )
	    return false;
	argp += at->arg_step;
	tagp += arg_stored_size<any_reduct>();
	break;
    }
#endif
    case a_NUM:
	abort();
    }
    return arg_apply3_stored_fn<Fn,Tn...>( fn, nargs-1, argp, tagp, an... );
}

// ------------------------------------------------------------------------
// Applicator to initialize the stored argument list.
// ------------------------------------------------------------------------
template<template <typename U> class DepTy>
struct annotation_of {
    static const annotation_t value = a_NUM;
};

template<>
struct annotation_of<indep> {
    static const annotation_t value = a_in;
};

template<>
struct annotation_of<outdep> {
    static const annotation_t value = a_out;
};

template<>
struct annotation_of<inoutdep> {
    static const annotation_t value = a_inout;
};

template<>
struct annotation_of<truedep> {
    static const annotation_t value = a_true;
};

#if OBJECT_COMMUTATIVITY
template<>
struct annotation_of<cinoutdep> {
    static const annotation_t value = a_cinout;
};
#endif

template<typename T, template <typename U> class DepTy>
void set_annotation_tags( DepTy<T> & dep, char * s ) {
    annotation_tags * tags = reinterpret_cast<annotation_tags *>( s );
    tags->annot = annotation_of<DepTy>::value;
}

#if OBJECT_REDUCTION
template<typename M>
void set_annotation_tags( reduction<M> & dep, char * s ) {
    annotation_tags * tags = reinterpret_cast<annotation_tags *>( s );
    tags->annot = std::is_same<typename M::reduction_tag,
	cheap_reduction_tag>::value ? a_reduct_cheap : a_reduct_expensive;
}
#endif

template<size_t argnum, typename T>
inline typename std::enable_if<!is_object<T>::value>::type
arg_stored_init_fn1( size_t (*off)(size_t), bool more, char * __restrict__ a, char * __restrict__ s ) {
    annotation_tags * tags = reinterpret_cast<annotation_tags *>( s );
    tags->arg_step = more ? (*off)(argnum+1) - (*off)(argnum) : 0;
    tags->annot = a_none;
    // errs() << "stored_init " << tags << " a_none step" << tags->arg_step << "\n";
}

template<size_t argnum, typename T>
inline typename std::enable_if<is_object<T>::value>::type
arg_stored_init_fn1( size_t (*off)(size_t), bool more, char * __restrict__ a, char * __restrict__ s ) {
    annotation_tags * tags = reinterpret_cast<annotation_tags *>( s );
    tags->arg_step = more ? (*off)(argnum+1) - (*off)(argnum) : 0;
    set_annotation_tags( *reinterpret_cast<T*>( a+(*off)(argnum) ), s );
    // errs() << "stored_init " << tags << (int)tags->annot << " step" << tags->arg_step << "\n";
}

template<size_t argnum, typename T>
static inline void arg_stored_init_fnr( size_t (*off)(size_t), char * __restrict__ a, char * __restrict__ s ) {
    arg_stored_init_fn1<argnum,T>( off, false, a, s );
}

template<size_t argnum, typename T, typename T1, typename... Tn>
static inline void arg_stored_init_fnr( size_t (*off)(size_t), char * __restrict__ a, char * __restrict__ s ) {
    arg_stored_init_fn1<argnum,T>( off, true, a, s );
    arg_stored_init_fnr<argnum+1,T1,Tn...>( off, a, s+arg_stored_size<T>() );
}

template<typename... Tn>
static inline void arg_stored_init_fn( task_data_t & td ) {
    size_t (*off)(size_t) = &offset_of<Tn...>;
    arg_stored_init_fnr<0,Tn...>( off, td.get_args_ptr(), td.get_tags_ptr() );
}


// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Allows building all kinds of functors to build callee-specific functions
// that check readiness, acquire and release objects, update object depths, etc.
// These functors step over a list of stack-stored arguments and a list
// of stack-stored dep_tags.
// ------------------------------------------------------------------------
template<typename Fn>
static inline bool arg_apply_stored_fn( Fn & fn, size_t nargs,
					char * __restrict__ argp,
					char * __restrict__ tagp ) {
    for( size_t i=0; i < nargs; ++i ) {
	annotation_tags * at = reinterpret_cast<annotation_tags*>( tagp );
	// errs() << "apply " << at << ' ' << (int)at->annot << "\n";
	switch( at->annot ) {
	case a_none:
	    argp += at->arg_step;
	    tagp += sizeof(annotation_tags);
	    break;
	case a_in:
	    typedef indep<int> any_in;
	    if( !apply_functor<Fn,any_in>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_in>();
	    break;
	case a_out:
	    typedef outdep<int> any_out;
	    if( !apply_functor<Fn,any_out>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_out>();
	    break;
	case a_inout:
	    typedef inoutdep<int> any_inout;
	    if( !apply_functor<Fn,any_inout>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_inout>();
	    break;
	case a_true:
	    typedef truedep<int> any_true;
	    if( !apply_functor<Fn,any_true>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_true>();
	    break;
#if OBJECT_COMMUTATIVITY
	case a_cinout:
	    typedef cinoutdep<int> any_cinout;
	    if( !apply_functor<Fn,any_cinout>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_cinout>();
	    break;
#endif
#if OBJECT_REDUCTION
	case a_reduct_cheap:
	{
	    struct any_monad {
		typedef int value_type;
		typedef cheap_reduction_tag reduction_tag;
	    };
	    typedef reduction<any_monad> any_reduct;
	    if( !apply_functor<Fn,any_reduct>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_reduct>();
	    break;
	}
	case a_reduct_expensive:
	{
	    struct any_monad {
		typedef int value_type;
		typedef expensive_reduction_tag reduction_tag;
	    };
	    typedef reduction<any_monad> any_reduct;
	    if( !apply_functor<Fn,any_reduct>( fn, argp, tagp ) )
		return false;
	    argp += at->arg_step;
	    tagp += arg_stored_size<any_reduct>();
	    break;
	}
#endif
	case a_NUM:
	    abort();
	}
    }
    return true;
}

template<typename Fn>
static inline bool arg_apply_stored_fn( Fn & fn, task_data_t & td ) {
    return arg_apply_stored_fn( fn, td.get_num_args(), td.get_args_ptr(),
				td.get_tags_ptr() );
}

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Then, traverse back the argument list, calling the undo() method.
// Allows building all kinds of functors to build callee-specific functions
// that check readiness, acquire and release objects, update object depths, etc.
// These functors step over a list of stack-stored arguments and a list
// of stack-stored dep_tags.
// ------------------------------------------------------------------------
template<typename Fn>
static inline bool arg_apply_stored_ufn( Fn & fn, size_t nargs,
					 char * __restrict__ argp,
					 char * __restrict__ tagp ) {
    if( nargs == 0 )
	return true;

    annotation_tags * at = reinterpret_cast<annotation_tags*>( tagp );
    size_t adelta = 0, tdelta = 0;
    // errs() << "apply " << at << ' ' << (int)at->annot << "\n";
    switch( at->annot ) {
    case a_none:
	adelta = at->arg_step;
	tdelta = sizeof(annotation_tags);
	break;
    case a_in:
	typedef indep<int> any_in;
	if( !apply_functor<Fn,any_in>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_in>();
	break;
    case a_out:
	typedef outdep<int> any_out;
	if( !apply_functor<Fn,any_out>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_out>();
	break;
    case a_inout:
	typedef inoutdep<int> any_inout;
	if( !apply_functor<Fn,any_inout>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_inout>();
	break;
    case a_true:
	typedef truedep<int> any_true;
	if( !apply_functor<Fn,any_true>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_true>();
	break;
#if OBJECT_COMMUTATIVITY
    case a_cinout:
	typedef cinoutdep<int> any_cinout;
	if( !apply_functor<Fn,any_cinout>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_cinout>();
	break;
#endif
#if OBJECT_REDUCTION
    case a_reduct_cheap:
    {
	struct any_monad {
	    typedef int value_type;
	    typedef cheap_reduction_tag reduction_tag;
	};
	typedef reduction<any_monad> any_reduct;
	if( !apply_functor<Fn,any_reduct>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_reduct>();
	break;
    }
    case a_reduct_expensive:
    {
	struct any_monad {
	    typedef int value_type;
	    typedef expensive_reduction_tag reduction_tag;
	};
	typedef reduction<any_monad> any_reduct;
	if( !apply_functor<Fn,any_reduct>( fn, argp, tagp ) )
	    goto FAIL;
	adelta = at->arg_step;
	tdelta = arg_stored_size<any_reduct>();
	break;
    }
#endif
    case a_NUM:
	abort();
    }

    if( !arg_apply_stored_ufn( fn, nargs-1, argp+adelta, tagp+tdelta ) ) {
    FAIL:
	annotation_tags * at = reinterpret_cast<annotation_tags*>( tagp );
	// errs() << "apply " << at << ' ' << (int)at->annot << "\n";
	switch( at->annot ) {
	case a_none:
	    break;
	case a_in:
	    typedef indep<int> any_in;
	    apply_undo<Fn,any_in>( fn, argp, tagp );
	    break;
	case a_out:
	    typedef outdep<int> any_out;
	    apply_undo<Fn,any_out>( fn, argp, tagp );
	    break;
	case a_inout:
	    typedef inoutdep<int> any_inout;
	    apply_undo<Fn,any_inout>( fn, argp, tagp );
	    break;
	case a_true:
	    typedef truedep<int> any_true;
	    apply_undo<Fn,any_true>( fn, argp, tagp );
	    break;
#if OBJECT_COMMUTATIVITY
	case a_cinout:
	    typedef cinoutdep<int> any_cinout;
	    apply_undo<Fn,any_cinout>( fn, argp, tagp );
	    break;
#endif
#if OBJECT_REDUCTION
	case a_reduct_cheap:
	{
	    struct any_monad {
		typedef int value_type;
		typedef cheap_reduction_tag reduction_tag;
	    };
	    typedef reduction<any_monad> any_reduct;
	    apply_undo<Fn,any_reduct>( fn, argp, tagp );
	    break;
	}
	case a_reduct_expensive:
	{
	    struct any_monad {
		typedef int value_type;
		typedef expensive_reduction_tag reduction_tag;
	    };
	    typedef reduction<any_monad> any_reduct;
	    apply_undo<Fn,any_reduct>( fn, argp, tagp );
	    break;
	}
#endif
	case a_NUM:
	    abort();
	}
	return false;
    }
    return true;
}


#else
// ------------------------------------------------------------------------
//      *** APPLICATOR IMPLEMENTATIONS FOR TYPE-BASED ANNOTATIONS ***
//
// Applicators provided:
// arg_apply3_fn<>(): for immediate issue with potential renaming
// arg_apply_fn<>(): operating on types, main case
// arg_apply_ufn<>(): same as above, but call undo function on failure
// arg_dapply_fn<>(): operating directly on argument list, e.g. ready check
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Allows building all kinds of functors to build callee-specific functions
// that access all three of copied arguments, tags and real arguments.
// ------------------------------------------------------------------------
template<typename Fn, typename AL>
static inline bool
arg_apply3_fnr( Fn & fn, AL al,
		char * __restrict__ argp, char * __restrict__ tagp ) {
    return true;
}

template<typename Fn, typename AL, typename T, typename... Tn>
static inline bool
arg_apply3_fnr( Fn & fn, AL al,
		char * __restrict__ argp, char * __restrict__ tagp,
		T & a0, Tn & ... an ) {
    typedef typename AL::template arg_locator_next<T>::type AL_next;
    return apply3_functor<Fn,T>( fn, argp+al.template get<T>(), tagp, a0 )
	&& arg_apply3_fnr<Fn,AL_next,Tn...>( fn, al.template step<T>(),
					     argp, tagp+arg_stored_size<T>(),
					     an... );
}

template<typename Fn, typename... Tn>
static inline bool arg_apply3_fn( Fn & fn, size_t (*off)(size_t), char * __restrict__ argp, char * __restrict__ tagp, Tn & ... an ) {
    return arg_apply3_fnr( fn, create_arg_locator<Tn...>(), argp, tagp, an... );
}

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Allows building all kinds of functors to build callee-specific functions
// that check readiness, acquire and release objects, update object depths, etc.
// These functors step over a list of stack-stored arguments and a list
// of stack-stored dep_tags.
// ------------------------------------------------------------------------
template<typename Fn, typename AL>
static inline bool arg_apply_fnr( Fn & fn, AL al, char * __restrict__ a, char * __restrict__ s ) {
    return true;
}

template<typename Fn, typename AL, typename T>
static inline bool arg_apply_fnr( Fn & fn, AL al, char * __restrict__ a, char * __restrict__ s ) {
    return apply_functor<Fn,T>( fn, a+al.template get<T>(), s );
}

template<typename Fn, typename AL, typename T, typename T1, typename... Tn>
static inline bool arg_apply_fnr( Fn & fn, AL al, char * __restrict__ a, char * __restrict__ s ) {
    typedef typename AL::template arg_locator_next<T>::type AL_next;
    return apply_functor<Fn,T>( fn, a+al.template get<T>(), s )
	&& arg_apply_fnr<Fn,AL_next,T1,Tn...>( fn, al.template step<T>(), a, s+arg_stored_size<T>() );
}

template<typename Fn, typename... Tn>
static inline bool arg_apply_fn( Fn & fn, char * __restrict__ a, char * __restrict__ s ) {
    return arg_apply_fnr<Fn,arg_locator<0,0>,Tn...>( fn, create_arg_locator<Tn...>(), a, s );
}

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Then, traverse back the argument list, calling the undo() method.
// Allows building all kinds of functors to build callee-specific functions
// that check readiness, acquire and release objects, update object depths, etc.
// These functors step over a list of stack-stored arguments and a list
// of stack-stored dep_tags.
// ------------------------------------------------------------------------
template<typename Fn, typename AL, typename T>
static inline bool arg_apply_ufnr( Fn & fn, AL al, char * __restrict__ a, char * __restrict__ s ) {
    return apply_functor<Fn,T>( fn, a+al.template get<T>(), s );
}

template<typename Fn, typename AL, typename T, typename T1, typename... Tn>
static inline bool arg_apply_ufnr( Fn & fn, AL al, char * __restrict__ a, char * __restrict__ s ) {
    if( apply_functor<Fn,T>( fn, a+al.template get<T>(), s ) ) {
	typedef typename AL::template arg_locator_next<T>::type AL_next;
	if( arg_apply_ufnr<Fn,AL_next,T1,Tn...>( fn, al.template step<T>(), a, s+arg_stored_size<T>() ) )
	    return true;
	apply_undo<Fn,T>( fn, a+al.template get<T>(), s );
    }
    return false;
}

template<typename Fn, typename... Tn>
static inline bool arg_apply_ufn( Fn & fn, char * __restrict__ a, char * __restrict__ s ) {
    return arg_apply_ufnr<Fn,arg_locator<0,0>,Tn...>( fn, create_arg_locator<Tn...>(), a, s );
}

// ------------------------------------------------------------------------
// Applicator. Apply the functor of type Fn to all arguments of is_object<>
// type. If the functor returns false, then stop evaluating arguments.
// Allows building all kinds of functors to build callee-specific functions
// that check readiness, acquire and release objects, update object depths, etc.
// These functors step over a real argument list (Tn... an) and a list
// of stack-stored dep_traits.
// ------------------------------------------------------------------------
template<typename Fn>
static inline bool arg_dapply_fn( Fn & fn, char * s ) { return true; }

template<typename Fn, typename T, typename... Tn>
static inline bool arg_dapply_fn( Fn & fn, char * s, T & a0, Tn & ... an ) {
    if( dapply_functor<Fn,T>( fn, s, a0 ) ) {
	if( arg_dapply_fn<Fn,Tn...>( fn, s+arg_stored_size<T>(), an... ) )
	    return true;
	dapply_undo<Fn,T>( fn, s, a0 );
    }
    return false;
}

#endif

} // namespace obj

#endif // ARGWALK_H

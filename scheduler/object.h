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
 * object.h
 *
 * This file provides an preliminary implementation of objects that allow
 * to test the mechanisms for compile-time type checking of spawn() arguments.
 *
 * BUGS (probably outdated comment):
 * + Currently, calls are only allowed to have outdep arguments.
 */
#ifndef OBJECT_H
#define OBJECT_H

#include "config.h"

#include <type_traits>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <vector>

#include "platform.h"
#include "wf_task.h"
#include "wf_frames.h"
#include "padding.h"
#include "lock.h"

#include "debug.h"

// Do we really need objects?
#if OBJECT_TASKGRAPH != 0

extern size_t nthreads;
extern __thread size_t threadid;

namespace obj {

// ------------------------------------------------------------------------
// Pre-declarations
// ------------------------------------------------------------------------
enum obj_modifiers_t {
    obj_none = 0,
    obj_recast = 1
};
template<typename T> class indep;
template<typename T> class outdep;
template<typename T> class inoutdep;
#if OBJECT_COMMUTATIVITY
template<typename T> class cinoutdep;
#endif
#if OBJECT_REDUCTION
template<typename T> class reduction;
#endif
template<typename T> class truedep;
template<typename MetaData>
class obj_version;
template<typename MetaData>
class obj_instance;

template<typename T, obj_modifiers_t OMod>
class object_t; // versioned;
template<typename T, obj_modifiers_t OMod>
class unversioned;

class obj_dep_traits;
//-----------------------------------------------------------------------
//------------------- DECLARATIONS FOR QUEUE_T --------------------------
//-----------------------------------------------------------------------
template<class MetaData> class queue_base;
template<class T> class pushdep;
template<class T> class popdep;
template<class T> class pushpopdep;
template<class T> class prefixdep;
template<class T> class hyperqueue;
template<class MetaData> class queue_version;
#include "queue/typeinfo.h"
//-----------------------------------------------------------------------
//------------------- DECLARATIONS FOR QUEUE_T - END --------------------
//-----------------------------------------------------------------------



#if STORED_ANNOTATIONS
enum annotation_t {
    a_none,
    a_in,
    a_out,
    a_inout,
    a_true,
#if OBJECT_COMMUTATIVITY
    a_cinout,
#endif
#if OBJECT_REDUCTION
    a_reduct_cheap,
    a_reduct_expensive,
#endif
    a_NUM
};

// @Note
//   This struct must always be inherited from first, i.e. for any pointer
//   to a XXdep::dep_tags structures, we must have the invariant:
//   tagp == static_cast<annotation_tags*>( tagp )
struct annotation_tags {
    size_t arg_step;
    annotation_t annot;
};

static inline std::ostream &
operator << ( std::ostream & os, annotation_t annot ) {
    switch( annot ) {
	case a_none:
	    return os << "a_none";
	case a_in:
	    return os << "a_in";
	case a_out:
	    return os << "a_out";
	case a_inout:
	    return os << "a_inout";
	case a_true:
	    return os << "a_true";
#if OBJECT_COMMUTATIVITY
	case a_cinout:
	    return os << "a_cinout";
#endif
#if OBJECT_REDUCTION
	case a_reduct_cheap:
	    return os << "a_reduct_cheap";
	case a_reduct_expensive:
	    return os << "a_reduct_expensive";
#endif
	case a_NUM:
	    abort();
    }
    return os;
}

static inline std::ostream &
operator << ( std::ostream & os, const annotation_tags & at ) {
    return os << "annot={step=" << at.arg_step << ", annot=" << at.annot << "}";
}

#endif

// ------------------------------------------------------------------------
// Auxiliaries
// ------------------------------------------------------------------------

// Patch sizeof(int[][2]) - return sizeof(int[2])
template<typename T, bool shall_we_override =
	 std::is_array<T>::value && (std::extent<T>::value == 0)>
struct size_struct {
    static const size_t value = sizeof(T);
};

template<typename T>
struct size_struct<T,true> {
    typedef typename std::remove_extent<T>::type ty;
    static const size_t value = sizeof(ty);
};

template<>
struct size_struct<void,false> {
    static const size_t value = 0;
};

// ------------------------------------------------------------------------
// Profiling
// ------------------------------------------------------------------------
#if PROFILE_OBJECT
struct statistics {
    size_t v_rename;
    size_t v_rename_inout;
    size_t v_rename_inout_task;
    size_t v_rename_unversioned;
    size_t v_arg_in;
    size_t v_arg_out;
    size_t v_arg_inout;
    size_t v_arg_in_ready;
    size_t v_arg_out_ready;
    size_t v_arg_inout_ready;
    size_t v_arg_inout_ready_may_rename;
    size_t v_arg_ini_ready;
    size_t v_arg_ini_ready_calls;
    size_t v_arg_issue;
    size_t v_arg_issue_leaf;

    statistics() {
	memset( reinterpret_cast<void *>(this), 0, sizeof(*this) );
    }

#define DUMP(x) "\n " #x "=" << v_##x
    void dump_statistics() {
	std::cout << "Object statistics:"
		  << DUMP(rename)
		  << DUMP(rename_inout)
		  << DUMP(rename_inout_task)
		  << DUMP(rename_unversioned)
		  << DUMP(arg_in)
		  << DUMP(arg_out)
		  << DUMP(arg_inout)
		  << DUMP(arg_in_ready)
		  << DUMP(arg_out_ready)
		  << DUMP(arg_inout_ready)
		  << DUMP(arg_inout_ready_may_rename)
		  << DUMP(arg_ini_ready)
		  << DUMP(arg_ini_ready_calls)
		  << DUMP(arg_issue)
		  << DUMP(arg_issue_leaf)
		  << '\n';
    }
#undef DUMP
};

extern void dump_statistics();

extern statistics statistic;

#define OBJ_PROF(x) do { __sync_fetch_and_add( &statistic.v_##x, 1 ); } while( 0 )


#else
#define OBJ_PROF(x)
#endif

// ------------------------------------------------------------------------
// Concepts
// ------------------------------------------------------------------------
// Tag types
struct indep_type_tag { };
struct outdep_type_tag { };
struct inoutdep_type_tag { };
struct cinoutdep_type_tag { };
struct reduction_type_tag { };
struct truedep_type_tag { };
//QUEUE_T
struct popdep_type_tag { };
struct pushdep_type_tag { };
struct pushpopdep_type_tag { };
struct prefixdep_type_tag { };

// Generic types to support concepts
typedef char small_type;
struct large_type
{
    small_type dummy[2];
};

// This code checks whether the type T is a class with a void is_object_decl()
// member. All checks are done at compile-time (type-based). None of the
// member functions in this class need to be defined.
template<class T>
struct class_has_object_decl {
    template<void (T::*)()> struct tester;

    template<typename U>
    static small_type has_matching_member(tester<&U::is_object_decl>*);
    template<typename U>
    static large_type has_matching_member(...);

    static const bool value = sizeof(has_matching_member<T>(0))==sizeof(small_type);
};

// Default case is no match (struct has bool value member = false).
template<typename T,bool is_class_type=std::is_class<T>::value>
struct is_object : std::false_type
{ };

// Case for structs where type matching with class_has_object_decl succeeds
template<typename T>
struct is_object<T,true>
    : std::integral_constant< bool, class_has_object_decl<T>::value >
{ };

// Helper -- only call it when T is a class type
template<typename T,typename U,bool hod=class_has_object_decl<T>::value>
struct is_object_with_tag_helper : std::false_type
{ };

template<typename T, typename U>
struct is_object_with_tag_helper<T,U,true>
    : std::integral_constant<bool,
			     std::is_same<typename T::_object_tag, U>::value>
{ };

// Check if type T is an object type (has a member is_object_decl()) and
// that the member type T::_object_tag is the same as U.
// All checks are done at compile-time (type-based).
template<typename T,typename U,bool is_class_type=std::is_class<T>::value>
struct is_object_with_tag : std::false_type
{ };

// Case for structs where type matching with class_has_indep_decl succeeds
template<typename T, typename U>
struct is_object_with_tag<T,U,true>
    : std::integral_constant<bool, class_has_object_decl<T>::value
			     && is_object_with_tag_helper<T,U>::value>
{ };

// Particular checks
template<typename T>
struct is_indep : is_object_with_tag<T, indep_type_tag> { };
template<typename T>
struct is_outdep : is_object_with_tag<T, outdep_type_tag> { };
template<typename T>
struct is_inoutdep : is_object_with_tag<T, inoutdep_type_tag> { };
template<typename T>
struct is_cinoutdep : is_object_with_tag<T, cinoutdep_type_tag> { };
template<typename T>
struct is_truedep : is_object_with_tag<T, truedep_type_tag> { };
template<typename T>
struct is_reduction : is_object_with_tag<T, reduction_type_tag> { };
template<typename T>
struct is_pushdep : is_object_with_tag<T, pushdep_type_tag> { };
template<typename T>
struct is_popdep : is_object_with_tag<T, popdep_type_tag> { };
template<typename T>
struct is_pushpopdep : is_object_with_tag<T, pushpopdep_type_tag> { };
template<typename T>
struct is_prefixdep : is_object_with_tag<T, prefixdep_type_tag> { };
template<typename T>
struct is_queue_dep
    : std::integral_constant<bool,
			     is_object_with_tag<T, pushpopdep_type_tag>::value
			     || is_object_with_tag<T, popdep_type_tag>::value
			     || is_object_with_tag<T, pushdep_type_tag>::value
			     || is_object_with_tag<T, prefixdep_type_tag>::value
			     > { };

// ------------------------------------------------------------------------
// Classes to support versioning of objects
// ------------------------------------------------------------------------
template<typename T, bool is_class=std::is_class<T>::value>
struct destructor_get {
    typedef void (*destructor_fn_ty)( void * );

    static destructor_fn_ty get_destructor() {
	return &call_destructor;
    }
private:
    static void call_destructor( void * ptr ) {
	reinterpret_cast<T *>( ptr )->T::~T();
    }
};

template<typename T>
struct destructor_get<T, false> {
    typedef void (*destructor_fn_ty)( void * );

    static destructor_fn_ty get_destructor() { return 0; }
};

class typeinfo {
    typedef void (*destructor_fn_ty)( void * );
    destructor_fn_ty dfn;

    typeinfo( destructor_fn_ty dfn_ ) : dfn( dfn_ ) { }

public:
    template<typename T>
    static typeinfo create() {
	typeinfo ti( destructor_get<T>::get_destructor() );
	return ti;
    }

    template<typename T>
    static void construct( void * ptr ) {
	new (ptr) T();
    }

    void destruct( void * ptr ) {
	if( dfn )
	    (*dfn)( ptr );
    }
};

// TODO: tight implementation of obj_version: allocate obj_version and
// obj_payload at once. Disadvantage: inefficient to nest (need to create
// new payload in child and copy-in/copy-out data), impossible to delegate
// (or really inefficient). Constant overhead when dereferencing version,
// e.g.: get_ptr(). Gain: efficiency of unversioned objects.
// We could detect the tight storage format by setting payload to null
// or equal to &obj_version<>::refcnt (to allow unchecked access to payload,
// but not very performance critical).
// Note: obj_payload::refcnt only accessed on create/destruct of obj_version<>,
// thus not very performance-critical.

// TODO: memory allocation overhead of obj_version<> is increased by
// splitting it into obj_version<> and obj_payload. Improve allocator, e.g.
// specialize obj_version<> allocator?

// obj_payload adds a reference counter to the data payload.
// The extra reference counter is required for the case of efficient nesting
// of task graphs and the passing of objects across levels.
class obj_payload {
    typedef uint32_t ctr_t;
private:
    ctr_t refcnt;
    typeinfo tinfo;
    pad_multiple<64, sizeof(ctr_t)> pad; // put payload at 64-byte boundary

    template<typename MetaData>
    friend class obj_version;

    obj_payload( typeinfo tinfo_, int refcnt_init=1 )
	: refcnt( refcnt_init ), tinfo( tinfo_ ) {
	// std::cerr << "Create obj_payload " << this << "\n";
    }
    ~obj_payload() {
	assert( refcnt == 0
		&& "Must have zero refcnt when destructing obj_payload" );
	// std::cerr << "Delete obj_payload " << this << "\n";
	tinfo.destruct( get_ptr() );
    }

public:
    // Dynamic memory allocation create function
    static obj_payload *
    create( size_t n, typeinfo tinfo ) {
	char * p = new char[sizeof(obj_payload)+n];
	return new (p) obj_payload( tinfo );
    }

    // In-place create function for unversioned objects
    static constexpr size_t
    size( size_t n ) {
	return n == 0 ? (sizeof(ctr_t)+sizeof(typeinfo))
	    : sizeof(obj_payload)+n;
    }
    static obj_payload *
    create( char * p, typeinfo tinfo ) {
	return new (p) obj_payload(tinfo, 2); // refcnt initialized to 2 to avoid free
    }

    // First byte after payload, allocated in one go with refcnt.
    // 1 is counted in units of sizeof(this).
    void * get_ptr() { return this+1; }
    const void * get_ptr() const { return this+1; }

    void add_ref() { __sync_fetch_and_add( &refcnt, 1 ); } // atomic!
    void del_ref() {
	assert( refcnt > 0 );
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &refcnt, -1 ) == 1 ) { // atomic!
	    delete this;
	}
    }

    void destruct() {
	tinfo.destruct( get_ptr() );
    }
};

// obj_reduction_md: metadata to maintain multiple instances of an object
// in case of executing a reduction.
// @Note:
//    This structure is created only when a reduction is computed
//    collaboratively between multiple threads. In a single-threaded execution,
//    there are no arg_issue() statements (for >1 depth of nesting under a
//    full frame), so there is also no reduction activity going on. There is
//    no need to optimize that case in the code of this class.
struct expensive_reduction_tag { };
struct cheap_reduction_tag { };

template<typename MetaData>
class obj_reduction_md {
    class private_info {
	typedef cas_mutex_v<uint8_t> assign_mutex;

	obj_version<MetaData> * version;
	assign_mutex assigned;
	bool need_identity;
	bool is_pref;

	bool pad0[2];
	pad_multiple<CACHE_ALIGNMENT, sizeof(void*)+sizeof(assign_mutex)
		     +sizeof(bool[4])> pad1;

    public:
	~private_info() {
	    static_assert( sizeof(private_info) % CACHE_ALIGNMENT == 0,
			   "Padding of obj_reduction_md::private_info failed" );
	    if( !is_pref )
		version->del_ref();
	}

	template<typename Monad>
	void set_identity() { Monad::identity( get_ptr<Monad>() ); }

	template<typename Monad>
	typename Monad::value_type * get_ptr() {
	    typedef typename Monad::value_type value_type;
	    return reinterpret_cast<value_type *>( version->get_ptr() );
	}

	template<typename Monad>
	void initialize( size_t sz, obj_version<MetaData> * pref, bool use_pref,
			 cheap_reduction_tag ) {
	    if( use_pref ) {
		version = pref;
		is_pref = true;
		assigned = assign_mutex();
	    } else {
		// Immediate allocate + set identity value (cheap anyway)
		version = obj_version<MetaData>::template create<typename Monad::value_type>( sz, 0 );
		set_identity<Monad>();
		assigned = assign_mutex();
		is_pref = false;
	    }
	}

	template<typename Monad>
	void initialize( size_t sz, obj_version<MetaData> * pref, bool use_pref,
			 expensive_reduction_tag ) {
	    if( use_pref ) {
		version = pref;
		is_pref = true;
		assigned = assign_mutex();
		need_identity = false;
	    } else {
		// Lazy initialize, should also lazily allocate
		version = obj_version<MetaData>::template create<typename Monad::value_type>( sz, 0 );
		need_identity = true;
		assigned = assign_mutex();
		is_pref = false;
	    }
	}

	template<typename Monad>
	obj_version<MetaData> * try_reserve( cheap_reduction_tag ) {
	    return assigned.try_lock() ? version : 0;
	}
	template<typename Monad>
	obj_version<MetaData> * try_reserve( expensive_reduction_tag ) {
	    if( assigned.try_lock() ) {
		if( need_identity ) {
		    set_identity<Monad>();
		    need_identity = false;
		}
		return version;
	    }
	    return 0;
	}

	void unreserve() { assigned.unlock(); }

	obj_version<MetaData> *
	need_reduction( obj_version<MetaData> * orig, cheap_reduction_tag ) {
	    return version && version != orig ? version : 0;
	}
	
	obj_version<MetaData> *
	need_reduction( obj_version<MetaData> * orig, expensive_reduction_tag ){
	    if( version && version != orig && !need_identity ) {
		need_identity = true;
		return version;
	    }
	    return 0;
	}
    };

    enum state_t {
	s_uninit,
	s_active,
	s_reduced
    };

    private_info * per_thread;
    void (*finalize_fn)( obj_version<MetaData> * ); // finalization hook
    void (*expand_fn)( obj_version<MetaData> * ); // expand hook
    cas_mutex mutex;
    state_t state;

public:
    typedef MetaData metadata_t;

public:
    obj_reduction_md() : per_thread( 0 ), finalize_fn( 0 ), expand_fn( 0 ), state( s_uninit ) { }
    ~obj_reduction_md();

    void finalize( obj_version<MetaData> * obj ) {
	// Checking the pointer before taking the lock works because of the
	// time-serial nature of task graph constructions: if it will be set,
	// it must have been set before we reach here. Only unsetting it may
	// be racy.
	if( (finalize_fn != 0) & (state == s_active) ) {
	    mutex.lock();
	    if( state == s_active )
		(*finalize_fn)( obj );
	    mutex.unlock();
	}
    }
    void expand( obj_version<MetaData> * obj ) {
	// This code is race-free because it is called from the thread that
	// generates the task graph.
	if( (expand_fn != 0) & (state == s_active) )
	    (*expand_fn)( obj );
    }


    template<typename Monad>
    bool initialize( size_t sz, obj_version<MetaData> * orig ) {
	bool ret = state != s_active;
	if( !per_thread ) {
	    typename Monad::reduction_tag tag;
	    per_thread = new private_info[::nthreads];
	    for( size_t i=0; i < ::nthreads; ++i )
		per_thread[i].initialize<Monad>(
		    sz, orig, i == ::threadid, tag );
	    set_callback( &execute_cb<Monad>, tag );
	}
	state = s_active;
	return ret;
    }

    template<typename Monad>
    obj_version<MetaData> *
    enter( size_t sz, obj_version<MetaData> * orig, int * idxp ) {
	typename Monad::reduction_tag tag;
	size_t tid = ::threadid;

	if( auto v = per_thread[tid].template try_reserve<Monad>( tag ) ) {
	    *idxp = tid;
	    return v;
	}
	for( size_t i=0; i < ::nthreads; ++i )
	    if( auto v = per_thread[i].template try_reserve<Monad>( tag ) ) {
		*idxp = i;
		return v;
	    }

	// Cannot possibly get here
	abort();
    }

    void leave( int idx ) {
	// errs() << "leave thread " << ::threadid << " idx=" << idx << "\n";
	assert( idx >= 0 && idx < int(::nthreads) && "idx out of range" );
	per_thread[idx].unreserve();
    }

    template<typename Monad>
    void execute( obj_version<MetaData> * tgt_pl ) {
	typedef typename Monad::reduction_tag reduction_tag;
	execute_impl<Monad>( tgt_pl, reduction_tag() );
    }

    int build_reduction_array( obj_version<MetaData> * orig,
			       obj_version<MetaData> * touched[] );

private:
    void set_callback( void (*cb)( obj_version<MetaData> * ),
		       cheap_reduction_tag ) { finalize_fn = cb; }
    void set_callback( void (*cb)( obj_version<MetaData> * ),
		       expensive_reduction_tag ) { expand_fn = cb; }

    template<typename Monad>
    static void execute_cb( obj_version<MetaData> * orig ) {
	orig->template get_reduction<Monad>()
	    ->template execute<Monad>( orig );
    }

    template<typename Monad>
    void execute_impl( obj_version<MetaData> * tgt_pl,
		       cheap_reduction_tag tag );
    template<typename Monad>
    void execute_impl( obj_version<MetaData> * tgt_pl,
		       expensive_reduction_tag tag );
};

template<typename MetaData>
obj_reduction_md<MetaData>::
~obj_reduction_md() {
    // assert( !finalize_fn && "obj_version must be finalized in destruct" );
    // assert( !expand_fn && "obj_version must be expanded in destruct" );
    assert( ( state == s_reduced || state == s_uninit )
	    && "obj_version must be reduced or uninitialized in destructor" );

    if( per_thread )
	delete[] per_thread;
}

template<typename MetaData>
template<typename Monad>
void
obj_reduction_md<MetaData>::
execute_impl( obj_version<MetaData> * tgt_pl, cheap_reduction_tag ) {
    typedef typename Monad::value_type T;
    typename Monad::reduction_tag tag;

    // errs() << "execute reduction in serial...\n";

    if( !per_thread )
	return;

    T * tgt = reinterpret_cast<T *>( tgt_pl->get_ptr() );
    for( size_t i=0; i < ::nthreads; ++i )
	if( per_thread[i].need_reduction( tgt_pl, tag ) ) {
	    Monad::reduce( tgt, per_thread[i].template get_ptr<Monad>() );
	    // TODO: reset or delete payload?
	    per_thread[i].template set_identity<Monad>();
	}

    state = s_reduced;
}

template<typename MetaData>
template<typename Monad>
void
obj_reduction_md<MetaData>::
execute_impl( obj_version<MetaData> * tgt_pl, expensive_reduction_tag ) {
    typedef inoutdep<typename Monad::value_type> AccumTy;
    typedef obj_reduction_md<MetaData> ReductionTy;
    state = s_reduced; // avoid recursion
    // errs() << "execute reduction in parallel...\n";
    create_parallel_reduction_task<Monad>( AccumTy::create( tgt_pl ), this );
}

template<typename MetaData>
int
obj_reduction_md<MetaData>::
build_reduction_array( obj_version<MetaData> * orig,
		       obj_version<MetaData> * touched[] ) {
    size_t nthreads = ::nthreads;
    int n = 1;

    if( !per_thread )
	return 0;

    // data[0] = orig;

    for( size_t i=0; i < nthreads; ++i )
	if( auto v = per_thread[i].need_reduction( orig,
						   expensive_reduction_tag() ) )
	    touched[n++] = v;
    
    return n;
}

template<typename MetaData>
union reduction_md {
    obj_reduction_md<MetaData> * reduc;

public:
    reduction_md() : reduc( 0 ) { }
    ~reduction_md() { if( unlikely( reduc != 0 ) ) delete reduc; }

    void finalize( obj_version<MetaData> * obj ) {
	if( unlikely( reduc != 0 ) )
	    reduc->finalize( obj );
    }
    void expand( obj_version<MetaData> * obj ) {
	if( unlikely( reduc != 0 ) )
	    reduc->expand( obj );
    }

    template<typename Monad>
    bool
    initialize( size_t sz, obj_version<MetaData> * orig ) {
	if( !reduc )
	    reduc = new obj_reduction_md<MetaData>();
	return reduc->initialize<Monad>( sz, orig );
    }

    template<typename Monad>
    obj_version<MetaData> *
    enter( size_t sz, obj_version<MetaData> * orig, int * idxp ) {
	return reduc->enter<Monad>( sz, orig, idxp );
    }

    void leave( int idx ) { reduc->leave( idx ); }

    template<typename Monad>
    void execute( obj_version<MetaData> * tgt_pl ) {
	reduc->execute<Monad>( tgt_pl );
    }

    template<typename Monad>
    obj_reduction_md<MetaData> * get_reduction()  {
	return reduc;
    }

    bool is_initialized() const { return reduc != 0; }
};

// obj_version: versioning of data objects + tracking readers/writer
// obj_version adds information to a copy of a data object.
// This information is allocated dynamically in-line with the actual data.
template<typename MetaData>
class obj_version {
    typedef uint32_t ctr_t;
    typedef MetaData metadata_t;
private:
    metadata_t meta;              // metadata for dependency tracking
    ctr_t refcnt;                 // reference count;guess from readers/writers?
    uint32_t size;                // size of the data space in bytes
    obj_payload * payload;        // data payload
    obj_instance<metadata_t> * obj; // pointer to the object for renaming purposes
    reduction_md<metadata_t> reduc; // hook for reduction-specific information

    template<typename T, obj_modifiers_t OMod>
    friend class object_t; // versioned;

    template<typename T, obj_modifiers_t OMod>
    friend class unversioned;

    template<typename MetaData_, typename T, size_t DataSize>
    friend class obj_unv_instance; // for constructor

    // First-create constructor
    obj_version( size_t sz, obj_instance<metadata_t> * obj_, typeinfo tinfo )
	: refcnt( 1 ), size( sz ), obj( obj_ ) {
	payload = obj_payload::create( sz, tinfo );
	// std::cerr << "Create obj_version " << this << " payload " << (void *)payload << "\n";
    }
    // First-create constructor for unversioned objects
    obj_version( size_t sz, char * payload_ptr, typeinfo tinfo )
	: refcnt( 1 ), size( sz ), obj( (obj_instance<metadata_t> *)0 ) {
	payload = obj_payload::create( payload_ptr, tinfo );
	// std::cerr << "Create obj_version " << this << " payload " << (void *)payload << "\n";
    }
    // Constructor for nesting
    obj_version( size_t sz, obj_instance<metadata_t> * obj_,
		 obj_payload * payload_ )
	: refcnt( 1 ), size( sz ), payload( payload_ ), obj( obj_ ) {
	payload->add_ref();
	// std::cerr << "Nest obj_version " << this << " payload " << (void *)payload << "\n";
    }
    inline ~obj_version() {
	// std::cerr << "Delete obj_version " << this << " payload " << (void *)payload << "\n";
	assert( refcnt == 0 && "obj_version::refcnt must be 0 in destruct" );
	payload->del_ref();
    }

public:
    template<typename T>
    static obj_version<metadata_t> *
    create( size_t n, obj_instance<metadata_t> * obj_ ) {
	obj_version<metadata_t> * v
	    = new obj_version<metadata_t>( n, obj_, typeinfo::create<T>() );
	typeinfo::construct<T>( v->get_ptr() );
	return v;
    }
    static obj_version<metadata_t> *
    nest( obj_instance<metadata_t> * obj_, obj_instance<metadata_t> * src ) {
	size_t n = src->get_version()->get_size();
	obj_payload * payload = src->get_version()->get_payload();
	return new obj_version<metadata_t>( n, obj_, payload );
    }
    static obj_version<metadata_t> *
    nest( obj_instance<metadata_t> * obj_,
	  obj_instance<metadata_t> * src,
	  obj_version<metadata_t> * v_new ) {
	size_t n = src->get_version()->get_size();
	obj_payload * payload = src->get_version()->get_payload();
	// errs() << "nest: payload=" << payload << " ext=" << src->get_version()
	  //      << " int=" << v_new << "\n";
	return new (v_new) obj_version<metadata_t>( n, obj_, payload );
    }
    static obj_version<metadata_t> *
    nest( obj_instance<metadata_t> * obj_,
	  obj_version<metadata_t> * v_new,
	  obj_payload * payload ) {
	size_t n = obj_->get_version()->get_size();
	return new (v_new) obj_version<metadata_t>( n, obj_, payload );
    }
    static void
    unnest( obj_instance<metadata_t> * obj_, obj_instance<metadata_t> * src ) {
	obj_version<metadata_t> * src_v = src->get_version();
	obj_version<metadata_t> * obj_v = obj_->get_version();
	if( src_v->get_ptr() != obj_v->get_ptr() )
	    obj_v->copy_to( src_v );
    }

    // Private: we don't want to expose the internal representation of the
    // payload any more than necessary.
    obj_payload * get_payload() { return payload; }
    const obj_payload * get_payload() const { return payload; }

public:
    void * get_ptr() { return get_payload()->get_ptr(); }
    const void * get_ptr() const { return get_payload()->get_ptr(); }

    size_t get_size() const { return size; }

    obj_instance<metadata_t> * get_instance() const { return obj; }

    metadata_t * get_metadata() { return &meta; }
    const metadata_t * get_metadata() const { return &meta; }

    void add_ref() { __sync_fetch_and_add( &refcnt, 1 ); } // atomic!
    void del_ref() {
	assert( refcnt > 0 );
	// Check equality to 1 because we check value before decrement.
	if( __sync_fetch_and_add( &refcnt, -1 ) == 1 ) { // atomic!
	    del_ref_delete();
	}
    }
private:
    // Setting noinline helps performance on AMD (Opteron 6100)
    // but not on Intel (Core i7).
    void del_ref_delete() __attribute__((noinline));

// protected:
public:
    // Optimized del_ref() call for unversioned objects.
    void nonfreeing_del_ref() {
	assert( refcnt > 0 );
	__sync_fetch_and_add( &refcnt, -1 ); // atomic!
	// One may desire to call get_payload()->destruct() when the unversioned
	// goes out of scope (in which case refcnt drops to 0). However, it is
	// equally good to do this unconditionally in the obj_unv_instance
	// destructor. That saves us a conditional here.

	// TODO: with split allocation of metadata and payload, free payload
	// or allocate payload inline in special case also?
    }

public:
    template<typename T>
    obj_version<metadata_t> * rename() {
	OBJ_PROF( rename );
	size_t osize = size;      // Save in case del_ref() frees this
	// TODO: could reset reduction info here: doing reduction is now
	// redundant: if it hasn't been done already, it won't be read.
	del_ref();                // The renamed instance no longer points here
	return create<T>( osize, obj );   // Create a clone of ourselves
    }
    // bool is_renamed() const { return obj && obj->get_version() != this; }

    bool is_versionable() const { return obj != 0; }

    // For inout renaming
    void copy_to( obj_version<metadata_t> * dst ) {
	assert( size == dst->size && "Copy of obj_version with != size" );
	memcpy( dst->get_ptr(), get_ptr(), dst->size );
    }

    // For reductions
    template<typename Monad, typename Frame>
    void register_callback( Frame * odt ) {
	if( reduc.template initialize<Monad>( size, this ) ) {
	    bool do_expand = std::is_same<typename Monad::reduction_tag,
		expensive_reduction_tag>::value;
	    odt->add_finalize_version( this, do_expand );
	}
    }

    template<typename Monad>
    obj_version<MetaData> * enter_reduction( int * idxp ) {
	return reduc.template enter<Monad>( size, this, idxp );
    }
    void leave_reduction( int idx ) { reduc.leave( idx ); }
    template<typename Monad>
    obj_reduction_md<MetaData> * get_reduction()  {
	return reduc.get_reduction<Monad>();
    }

    bool is_used_in_reduction() const { return reduc.is_initialized(); }

    // Some computations may not be complete (eg reductions). Finalize them
    // now or expand them, depending on the complexity of the reduction
    void finalize() { reduc.finalize( this ); }
    void expand() { reduc.expand( this ); } 
private:
    template<typename Monad>
    static void execute_reduction( obj_version<MetaData> * v ) {
	v->reduc.template execute<Monad>( v );
    }

public:
    // Debugging
    template<typename MetaData_>
    friend std::ostream & operator << ( std::ostream & os,
					const obj_version<MetaData_> & v );
};

template<typename MetaData>
void obj_version<MetaData>::del_ref_delete() {
    delete this;
}

// obj_instance: an instance of an object, base class for object_t,
// indep, outdep, inoutdep, cinoutdep and truedep.
// This class may not have non-trival constructors nor destructors in order to
// reap the simplified x86-64 calling conventions for small structures (the
// only case we support), in particular for passing indep, outdep, inoutdep
// and cinoutdep as direct arguments. We don't support this for object_t.
template<typename MetaData>
class obj_instance {
public:
    typedef MetaData metadata_t;

protected:
    obj_version<metadata_t> * version;

    template<typename T, typename Base, typename Final>
    friend class obj_access_traits;

    template<typename MetaData_>
    friend class auto_nested_tags;

public:
    const obj_version<metadata_t> * get_version() const { return version; }
    obj_version<metadata_t> * get_version() { return version; }

    // If obj is non-nil:
    //   Create a new version of the data structure and make it the new
    //   current version of the object. Every new task in the spawner and
    //   in the spawnee must see this new version, but not previously created
    //   tasks.
    //   Set the version of this dependence to the new current version.
    template<typename T>
    inline obj_version<metadata_t> * rename( obj_instance<metadata_t> * bro = 0 ) __attribute__((noinline));

    template<typename MetaData_>
    friend std::ostream & operator << ( std::ostream & os,
					const obj_instance<MetaData_> & o );

protected:
    obj_version<metadata_t> * get_nc_version() const { return version; }

public:
    void set_version( obj_version<metadata_t> * v ) { version = v; }

public:
    obj_instance<metadata_t> * get_object() {
	obj_instance<metadata_t> * inst = version->get_instance();
	if( inst == this )
	    return inst;
	else if( inst == 0 )
	    return this;
	else
	    return inst->get_object();
    }
};

template<typename MetaData>
template<typename T>
obj_version<MetaData> * obj_instance<MetaData>::rename( obj_instance<MetaData> * bro ) {
    obj_instance<metadata_t> * obj = version->get_instance();
    if( obj == this )
	version = version->rename<T>();
    else if( obj )
	version = obj->rename<T>();
    else { // case of unversioned objects
	OBJ_PROF( rename_unversioned );
	return version;
    }
    if( bro )
	bro->version = version;
    return version;
}

template<typename MetaData, typename T, size_t DataSize>
class obj_unv_instance : public obj_version<MetaData> {
    typedef MetaData metadata_t;

    char data[obj_payload::size(DataSize)];
public:
    obj_unv_instance()
	: obj_version<metadata_t>( DataSize, data, typeinfo::create<T>() ) {
	// Call the payload data constructor (T::T()) here, just like we
	// do it in obj_version::create() functions that allocate a new payload.
	// Same motivation applies: we don't want to pass around a pointer to
	// the default constructor function for the obj_payload constructor to
	// call, because very often the constructor will be a no-op.
	typeinfo::construct<T>( obj_version<metadata_t>::get_ptr() );
    }
    ~obj_unv_instance() {
	obj_version<metadata_t>::get_payload()->destruct();
    }

    const obj_version<metadata_t> * get_version() const { return this; }
    obj_version<metadata_t> * get_version() { return this; }

protected:
    obj_version<metadata_t> * get_nc_version() const {
	return const_cast<obj_version<metadata_t> *>(
	    static_cast<const obj_version<metadata_t> *>( this ) );
    }
};

// Accessor semantics for object_t and in/out/inout dependencies.
// Should be specialized specifically for cases where T is scalar, pointer,
// array, struct, etc.
template<typename T, typename Base, typename Final>
class obj_access_traits : public Base {
    typedef Final this_ty;
public:
    typedef T elem_type;

    // Scalar accessors. Assignment is delegated to leaf classes because
    // the assignment would otherwise require an upcast: e.g.:
    //    (obj_access_traits<int, obj_instance, outdep<int> >)valout = int(...);
    // instead of simply:
    //    valout = int(...);
    operator T *() const { return get_ptr(); }
    operator T &() const { return *get_ptr(); }
    const this_ty & operator ++ () { (*get_ptr())++; }
    this_ty operator ++ (int) {
	this_ty cp = this_ty::create( Base::get_version() );
	(*cp.get_ptr())++;
	return cp;
    }

    // Structure element and pointer dereference
    T * operator -> () { return get_ptr(); }
    const T * operator -> () const { return get_ptr(); }
    T & operator * () { return *get_ptr(); }
    const T & operator * () const { return *get_ptr(); }
    // bool operator ! () const { return get_ptr() == 0; }

    // Array accessors
    const T & operator[] ( size_t idx ) const { return get_ptr()[idx]; }
    T & operator[] ( size_t idx ) { return get_ptr()[idx]; }

protected:
// public:
    T * get_ptr() const {
	return reinterpret_cast<T*>( Base::get_nc_version()->get_ptr() );
    }

    template<typename DepTy>
    DepTy create_dep_ty() const {
	DepTy od;
	od.version = this->get_nc_version();
	return od;
    }
};

// ------------------------------------------------------------------------
// Some debugging support
// ------------------------------------------------------------------------
template<typename MetaData>
inline std::ostream & operator << ( std::ostream & os, const obj_version<MetaData> & v ) {
    os << "obj_version{@" << std::hex << v.get_ptr() << std::dec
       << ", refcnt=" << v.refcnt
       << ", metadata=" << *v.get_metadata() << " }";
    return os; 
}

template<typename MetaData>
inline std::ostream & operator << ( std::ostream & os, const obj_instance<MetaData> & o ) {
    return os << "obj_instance{version=" << *o.version << '}';
}

// ------------------------------------------------------------------------
// Renaming
// ------------------------------------------------------------------------
template<typename MetaData, typename T,
	 template<typename U> class DepTy>
static inline void rename( DepTy<T> & obj_ext, DepTy<T> & obj_int,
			   typename DepTy<T>::dep_tags & ) {
    // No renaming in default case
}

template<typename MetaData, typename T>
static inline void rename( outdep<T> & obj_ext, outdep<T> & obj_int,
			   typename outdep<T>::dep_tags & ) {
    obj_version<MetaData> * v = obj_ext.get_version();
    assert( v->is_versionable() ); // Guaranteed by applicators
    if( v->get_metadata()->rename_is_active() )
	v = obj_ext.rename<T>( &obj_int );
}

// @Note
//   Renaming for inout dependencies will work as intended only
//   for the tickets taskgraph (OBJECT_TASKGRAPH == 1) because the
//   rename_has_readers() and rename_has_writers() require the semantics
//   that any (prior) reader and writer, respectively, exists. The other
//   taskgraphs can efficiently check the current generation/group only.
//   Requiring such a check would demand a reverse chain of pointers through
//   the generations and/or a generation tail pointer, which would inadvertently
//   introduce additional synchronization complexity.
// @Note
//   Inout renaming has not been proven yet on real codes and it is turned
//   off by default.
template<typename MetaData, typename T>
static inline void
rename( inoutdep<T> & obj_ext, inoutdep<T> & obj_int,
	typename inoutdep<T>::dep_tags & ) {
#if OBJECT_INOUT_RENAME > 0
    obj_version<MetaData> * v = obj_ext.get_version();
    if( likely( v->is_versionable() ) && v->rename_has_readers() ) {
	obj_version<MetaData> * v_old = v;
	if( v_old->rename_has_writers() ) {
#if OBJECT_INOUT_RENAME > 1
	    v = obj_ext.rename<T>( &obj_int );
	    // Create delayed copy task. Does a grab on the old
	    // object as well as on the new.
	    outdep<T> v_dep = outdep<T>::create( v );
	    indep<T> v_old_dep = indep<T>::create( v_old );
	    create_copy_task( v_dep, v_old_dep );
	    OBJ_PROF(rename_inout_task);
#endif
	} else {
	    v = obj_ext.rename<T>( &obj_int );
	    v_old->copy_to( v );
	    OBJ_PROF(rename_inout);
	}
    }
#endif
}

#if OBJECT_REDUCTION
template<typename MetaData, typename M>
static inline
typename std::enable_if< std::is_class<M>::value >::type
reduction_init( reduction<M> & obj_int,
		typename reduction<M>::dep_tags & tags,
		obj_dep_traits * odt ) {
    // Make sure we have a privatized version for the reduction
    obj_version<MetaData> * v = obj_int.get_version();
    tags.ext_version = v; // Cache it for release

    // Register a callback to do the tree reduction at a sync
    v->template register_callback<M>( odt );
}

template<typename MetaData, typename M>
static inline
typename std::enable_if< std::is_class<M>::value >::type
privatize( reduction<M> & obj_ext, reduction<M> & obj_int,
	   typename reduction<M>::dep_tags & tags ) {
    // Select a privatized version for the reduction
    // If we are the first/only one to execute part of this reduction, then
    // use the original payload, to avoid overhead later.
    obj_version<MetaData> * v = obj_int.get_version();
    obj_version<MetaData> * d = v->template enter_reduction<M>( &tags.idx );
    obj_int.set_version( d ); // May be silent store
}
#endif

// ------------------------------------------------------------------------
// Dependency traits class
// ------------------------------------------------------------------------
template<typename MetaData, typename Task, template<class T> class DepTy>
struct dep_traits {
    template<typename T>
    static inline void arg_issue( Task * fr, DepTy<T> obj_ext,
				  typename DepTy<T>::dep_tags * sa );
    template<typename T>
    static inline bool arg_ini_ready( DepTy<T> obj_int );
    // undo only required for cinoutdep
    template<typename T>
    static inline bool arg_ini_ready_undo( DepTy<T> obj_int );
    template<typename T>
    static inline void arg_release( Task * fr, DepTy<T> obj,
				    typename DepTy<T>::dep_tags & sa );
};

// ------------------------------------------------------------------------
// Applicators - moved to different file
// ------------------------------------------------------------------------
} 
#include "argwalk.h"

namespace obj {

// ------------------------------------------------------------------------
// All kinds of functors to build callee-specific functions that check
// readiness, acquire and release objects, update object depths, etc.
// These functors step over a list of stack-stored arguments and a list
// of stack-stored dep_tags.
// ------------------------------------------------------------------------
// Tags initialization functor
struct initialize_tags_functor {
    // For most tags types, this will basically be a no-op
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> obj_int, typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::dep_tags tags_t;
	new (&tags) tags_t();
	return true;
    }

    // Queues store a new queue_version inside the tags, allocated together
    // with the tags, and this queue_version is initialized now. The constructor
    // requires the parent queue_version as an argument.
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> & obj_int, typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::dep_tags tags_t;
	new (&tags) tags_t( obj_int.get_version() );
	obj_int = DepTy<T>::create( tags.get_queue_version() );
	return true;
    }
};

// Tags cleanup functor
struct cleanup_tags_functor {
    bool is_stack;
    bool is_released;
    cleanup_tags_functor( bool is_stack_, bool is_released_ ) 
	: is_stack( is_stack_ ), is_released( is_released_ ) { }

    // For most tags types, this will basically be a no-op
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_cinoutdep<DepTy<T>>::value
			&& !is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> obj_ext, typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::dep_tags tags_t;
	tags.~tags_t();
	return true;
    }

    template<typename T>
    bool operator() ( cinoutdep<T> obj_ext,
		      typename cinoutdep<T>::dep_tags & tags ) {
	if( !is_released )
	    obj_ext.get_version()->get_metadata()->commutative_release();
	typedef typename cinoutdep<T>::dep_tags tags_t;
	tags.~tags_t();
	return true;
    }

    // For queue, also reduce hypermaps
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> obj_ext, typename DepTy<T>::dep_tags & tags ) {
	tags.get_queue_version()->reduce_hypermaps( is_pushdep<DepTy<T>>::value, is_stack );

	typedef typename DepTy<T>::dep_tags tags_t;
	tags.~tags_t();
	return true;
    }
};

// Release functor
template<typename MetaData_, typename Task>
struct release_functor {
    Task * fr;
    bool is_stack;
    release_functor( Task * fr_, bool is_stack_ )
	: fr( fr_ ), is_stack( is_stack_ ) { fr->start_deregistration(); }
    ~release_functor() { fr->stop_deregistration(); }

    // In the default case, the internal obj_instance equals the external
    // obj_instance.
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_queue_dep<DepTy<T>>::value, bool>::type
    operator () ( DepTy<T> obj_ext, typename DepTy<T>::dep_tags & sa ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	dep_traits<MetaData, Task, DepTy>::arg_release( fr, obj_ext, sa );
	if( !std::is_void< T >::value ) // tokens
	    obj_ext.get_version()->del_ref();
	return true;
    }

#if OBJECT_REDUCTION
    // In the case of a reduction, the internal obj_instance may differ
    // from the external one.
    template<typename M>
    typename std::enable_if<std::is_class<M>::value, bool>::type
    operator () ( reduction<M> obj_int,
		  typename reduction<M>::dep_tags & tags ) {
	typedef typename reduction<M>::metadata_t MetaData;
	obj_version<MetaData> * v = tags.ext_version;
	reduction<M> obj_ext = reduction<M>::create( v );
	v->leave_reduction( tags.idx );
	dep_traits<MetaData, Task, reduction>::arg_release( fr, obj_ext, tags );
	v->del_ref();

	// Pretend IO behavior on the internal variable, if it differs from
	// the external one.
	// typedef typename M::value_type T;
	// dep_traits<MetaData, Task, inoutdep<T> >::arg_release( fr, obj_int, tags );
	// obj_int.get_version()->del_ref();

	return true;
    }
#endif

    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T>>::value, bool>::type
    operator () ( DepTy<T> obj_int, typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	// Reduce queue hypermaps - reduce before allowing siblings to launch
	// TODO: What if returning from a stack frame and not calling release?
	// queue_version<MetaData> * qv = obj_int.get_version();
	// qv->lock(); // do not relinquish the lock anymore!
	// qv->reduce_hypermaps( is_pushdep<DepTy<T>>::value, is_stack );

	// For queues, release must always be performed on the external version
	// which is the "parent" of the internal version.
	// DepTy<T> obj_ext
	    // = DepTy<T>::create( obj_int.get_version()->get_parent() );
	dep_traits<MetaData, Task, DepTy>::arg_release( fr, obj_int, tags );
	return true;
    }
};

// Grab functor
template<typename MetaData_, typename Task>
class grab_functor {
    Task * fr;
    obj_dep_traits * odt;
public:
    grab_functor( Task * fr_, obj_dep_traits * odt_ )
	: fr( fr_ ), odt( odt_ ) { }
    
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj_int, typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	DepTy<T> obj_ext = DepTy<T>::create( obj_int.get_version() );
	// Renaming is impossible here: we have already started to work
	// on this object, so it is too late now to rename...
	// rename<MetaData, T>( obj_ext, obj_int, tags );
	dep_traits<MetaData, Task, DepTy>::template arg_issue( fr, obj_ext, &tags );
	if( !std::is_void< T >::value ) // token
	    obj_ext.get_version()->add_ref();
	if( !is_outdep< DepTy<T> >::value
	    && !is_truedep< DepTy<T> >::value // static checks
	    && !std::is_void< T >::value // token
	    && obj_ext.get_version()->is_used_in_reduction() )
	    fr->get_task_data().set_finalization_required();
	return true;
    }

    template<typename T>
    bool operator () ( pushdep<T> & obj_int,
		       typename pushdep<T>::dep_tags & tags ) {
	typedef typename pushdep<T>::metadata_t MetaData;
	// TODO: make sure this code gets called also on a stack frame,
	// in case the parent frame gets stolen and this one is converted to full!
	dep_traits<MetaData, Task, pushdep>::template arg_issue( fr, obj_int, &tags );
	return true;
    }
	
    template<typename T>
    bool operator () ( popdep<T> & obj_int,
		       typename popdep<T>::dep_tags & tags ) {
	typedef typename popdep<T>::metadata_t MetaData;
	dep_traits<MetaData, Task, popdep>::template arg_issue( fr, obj_int, &tags );

	return true;
    }

#if OBJECT_REDUCTION
    template<typename M>
    typename std::enable_if<std::is_class<M>::value, bool>::type
    operator () ( reduction<M> & obj_int,
		  typename reduction<M>::dep_tags & tags ) {
	typedef typename reduction<M>::metadata_t MetaData;
	reduction<M> obj_ext = reduction<M>::create( obj_int.get_version() );
	reduction_init<MetaData, M>( obj_int, tags, odt );
	privatize<MetaData, M>( obj_ext, obj_int, tags );
	obj_ext.get_version()->add_ref();
	dep_traits<MetaData, Task, reduction>::template arg_issue( fr, obj_ext, &tags );
	return true;
    }
#endif
};

// Hypermap reduce functor
struct hypermap_reduce_functor {
    // For most tags types, this will basically be a no-op
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> obj_int, typename DepTy<T>::dep_tags & tags ) {
	return true;
    }

    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T>>::value, bool>::type
    operator() ( DepTy<T> & obj_int, typename DepTy<T>::dep_tags & tags ) {
	obj_int.get_version()->reduce_sync();
	return true;
    }
};

#if STORED_ANNOTATIONS
// An "initialize tags function" to initialize tags.
template<typename Task>
static inline void arg_initialize_tags_fn( Task * fr ) {
    initialize_tags_functor fn;
    arg_apply_stored_fn( fn, fr->get_task_data() );
}

// A "cleanup tags function" to initialize tags.
template<typename Task>
static inline void arg_cleanup_tags_fn( Task * fr, bool is_stack ) {
    cleanup_tags_functor fn( is_stack, false );
    arg_apply_stored_fn( fn, fr->get_task_data() );
}

// A "hypermap reduce function".
template<typename Task, typename... Tn>
static inline void arg_hypermap_reduce_fn( Task * fr ) {
    hypermap_reduce_functor fn;
    arg_apply_stored_fn( fn, fr->get_task_data() );
}

// A "release function" to store inside the dep_traits.
template<typename MetaData, typename Task>
static inline void arg_release_fn( Task * fr, bool is_stack ) {
    release_functor<MetaData, Task> fn( fr, is_stack );
    arg_apply_stored_fn( fn, fr->get_task_data() );
    cleanup_tags_functor fn( is_stack, true );
    arg_apply_stored_fn( fn, fr->get_task_data() );
}

// A "grab function" to store inside the dep_traits.
template<typename MetaData, typename Task, typename... Tn>
static inline void arg_issue_fn( Task * fr, obj_dep_traits * odt ) {
    grab_functor<MetaData,Task> gfn( fr, odt );
    fr->template start_registration<Tn...>();
    arg_apply_stored_fn( gfn, fr->get_task_data() );
    fr->stop_registration();
}
#else
// An "initialize tags function" to initialize tags.
template<typename Task, typename... Tn>
static inline void arg_initialize_tags_fn( Task * fr ) {
    initialize_tags_functor fn;
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
    arg_apply_fn<initialize_tags_functor,Tn...>( fn, args, tags );
}

// A "cleanup tags function" to initialize tags.
template<typename Task, typename... Tn>
static inline void arg_cleanup_tags_fn( Task * fr, bool is_stack ) {
    cleanup_tags_functor fn( is_stack, false );
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
    arg_apply_fn<cleanup_tags_functor,Tn...>( fn, args, tags );
}

// A "hypermap reduce function".
template<typename Task, typename... Tn>
static inline void arg_hypermap_reduce_fn( Task * fr ) {
    hypermap_reduce_functor fn;
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
    arg_apply_fn<hypermap_reduce_functor,Tn...>( fn, args, tags );
}

// A "release function" to store inside the dep_traits.
template<typename MetaData, typename Task, typename... Tn>
static inline void arg_release_fn( Task * fr, bool is_stack ) {
    release_functor<MetaData, Task> fn( fr, is_stack );
    cleanup_tags_functor cf( is_stack, true );
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
    arg_apply_fn<release_functor<MetaData, Task>,Tn...>( fn, args, tags );
    arg_apply_fn<cleanup_tags_functor,Tn...>( cf, args, tags );
}

// A "grab function" to store inside the dep_traits.
template<typename MetaData, typename Task, typename... Tn>
static inline void arg_issue_fn( Task * fr, obj_dep_traits * odt ) {
    grab_functor<MetaData,Task> gfn( fr, odt );
    fr->template start_registration<Tn...>();
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
    arg_apply_fn<grab_functor<MetaData,Task>,Tn...>( gfn, args, tags );
    fr->stop_registration();
}
#endif

// ------------------------------------------------------------------------
// All kinds of functors to build callee-specific functions that check
// readiness, acquire and release objects, update object depths, etc.
// These functors step over a real argument list (Tn... an) and a list
// of stack-stored dep_traits.
// ------------------------------------------------------------------------
// Acquire-and-store functor
template<typename MetaData_, typename Task>
struct dgrab_functor {
    Task * fr;
    obj_dep_traits * odt;
    bool is_ready;
    template<typename... Tn>
    dgrab_functor( Task * fr_, obj_dep_traits * odt_, bool is_ready_ )
	: fr( fr_ ), odt( odt_ ), is_ready( is_ready_ ) { }

    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> obj_ext, DepTy<T> & obj_int,
		       typename DepTy<T>::dep_tags & tags ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	// No renaming yet, unless we pass the same argument multiple times
	// assert( obj_ext.get_version() == obj_int.get_version() );
	rename<MetaData, T>( obj_ext, obj_int, tags );
	dep_traits<MetaData, Task, DepTy>::template arg_issue( fr, obj_ext, &tags );
	if( !std::is_void< T >::value ) // token
	    obj_ext.get_version()->add_ref();
	return true;
    }

    template<typename T>
    bool operator () ( pushdep<T> obj_ext, pushdep<T> & obj_int,
		       typename pushdep<T>::dep_tags & tags ) {
	typedef typename pushdep<T>::metadata_t MetaData;
	// Most of the work done at moment of initialization of tags, and
	// creation of child queue_version.
	dep_traits<MetaData, Task, pushdep>::template arg_issue( fr, obj_int, &tags );
	return true;
    }
	
	template<typename T>
    bool operator () ( popdep<T> obj_ext, popdep<T> & obj_int,
		       typename popdep<T>::dep_tags & tags ) {
	typedef typename popdep<T>::metadata_t MetaData;
	// Most of the work done at moment of initialization of tags, and
	// creation of child queue_version.
	dep_traits<MetaData, Task, popdep>::template arg_issue( fr, obj_int, &tags );
	return true;
    }
	
#if OBJECT_REDUCTION
    template<typename M>
    typename std::enable_if<std::is_class<M>::value, bool>::type
    operator () ( reduction<M> & obj_ext, reduction<M> & obj_int,
		  typename reduction<M>::dep_tags & tags ) {
	typedef typename reduction<M>::metadata_t MetaData;
	// Create a private copy for this task, if it is_ready
	reduction_init<MetaData, M>( obj_int, tags, odt );
	if( is_ready )
	    privatize<MetaData, M>( obj_ext, obj_int, tags );
	// Normal issue sequence
	obj_ext.get_version()->add_ref();
	dep_traits<MetaData, Task, reduction>::template arg_issue( fr, obj_ext, &tags );
	return true;
    }
#endif

};

// Reduction expansion functor
template<typename MetaData_, typename Task>
struct dexpand_functor {
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!std::is_void<T>::value && !is_queue_dep<DepTy<T> >::value, bool>::type
	operator () ( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	obj_version<MetaData> & v = *obj_ext.get_version();
	v.expand();
	return true;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj_ext,
		       typename outdep<T>::dep_tags & sa ) {
	return true;
    }
    template<typename T>
    bool operator () ( truedep<T> & obj_ext, typename truedep<T>::dep_tags & sa ) {
    	return true;
    }
#if OBJECT_REDUCTION
    template<typename M>
    bool operator () ( reduction<M> & obj_ext,
		       typename reduction<M>::dep_tags & sa ) {
	return true;
    }
#endif

#if OBJECT_COMMUTATIVITY
    template<typename T>
    void undo( cinoutdep<T> & obj_ext, typename cinoutdep<T>::dep_tags & sa ) {
    }
#endif

    // Tokens -- ?? -- need to qualify *ALL* functors with is_void<T>
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<std::is_void<T>::value, bool>::type
    operator () ( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	return true;
    }
	
    // Queues -- queue deps not involved in expansion
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T> >::value, bool>::type
    operator () ( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	return true;
    }
};

template<typename MetaData>
struct privatize_functor {
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & tags ) {
	return true;
    }
#if OBJECT_REDUCTION
    template<typename M>
    bool operator () ( reduction<M> & obj_int, // int == ext so far
		       typename reduction<M>::dep_tags & tags ) {
	reduction<M> obj_ext = reduction<M>::create( obj_int.get_version() );
	privatize<MetaData, M>( obj_ext, obj_int, tags );
	return true;
    }
#endif

    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) { }
};

template<typename MetaData>
class finalize_functor {
private:
    bool do_finalize;
public:
    finalize_functor( const task_data_t & task_data )
	: do_finalize( task_data.is_finalization_required() ) { }

    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_queue_dep<DepTy<T>>::value, bool>::type
    operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & tags ) {
	if( !is_outdep< DepTy<T> >::value
	    && !is_truedep< DepTy<T> >::value // static checks
	    && !std::is_void< T >::value // tokens are never finalized
	    && do_finalize )
	    obj.get_version()->finalize();
	return true;
    }
	
#if OBJECT_REDUCTION
    template<typename M>
    bool operator () ( reduction<M> & obj_int, // int == ext so far
		       typename reduction<M>::dep_tags & tags ) {
	// Don't finalize - reduction is still going on
	return true;
    }
#endif

    // Queues -- queue deps not involved in privatization
    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_queue_dep<DepTy<T> >::value, bool>::type
    operator () ( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	return true;
    }

    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) { }
};

// Initial ready? functor
template<typename MetaData_, typename Task>
struct dini_ready_functor {
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	if( dep_traits<MetaData, Task, DepTy>::arg_ini_ready( obj_ext ) ) {
	    obj_ext.get_version()->finalize();
	    return true;
	}
	return false;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj_ext,
		       typename outdep<T>::dep_tags & sa ) {
	// Don't finalize an outdep, we discard the result!
	// We could reset the finalize/expand callback in the version here,
	// and even deregister the object from the task list (to be executed at 
	// sync time), but that would be overhead that is paid in the common
	// case, while the program idiom (compute reduction and discard) is
	// an anomaly.
	typedef typename outdep<T>::metadata_t MetaData;
	return dep_traits<MetaData, Task, outdep>::arg_ini_ready( obj_ext );
    }
	
    template<typename T>
    bool operator () ( truedep<T> & obj_ext, typename truedep<T>::dep_tags & sa ) {
    	return true;
    }
#if OBJECT_REDUCTION
    template<typename M>
    bool operator () ( reduction<M> & obj_ext,
		       typename reduction<M>::dep_tags & sa ) {
	// Don't finalize more reductions...
	// TODO: What if we change M (eg from + to *)?
	typedef typename reduction<M>::metadata_t MetaData;
	return dep_traits<MetaData, Task, reduction>::arg_ini_ready( obj_ext );
    }
#endif

    // Queues
    template<typename T>
    bool operator () ( obj::pushdep<T> & obj_ext,
		       typename obj::pushdep<T>::dep_tags & sa ) {
	typedef typename obj::pushdep<T>::metadata_t MetaData;
	return dep_traits<MetaData, Task, pushdep>::arg_ini_ready( obj_ext );
    }
	
    template<typename T>
    bool operator () ( obj::popdep<T> & obj_ext,
		       typename obj::popdep<T>::dep_tags & sa ) {
	typedef typename obj::popdep<T>::metadata_t MetaData;
	return dep_traits<MetaData, Task, popdep>::arg_ini_ready( obj_ext );
    }

    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	typedef typename DepTy<T>::metadata_t MetaData;
	return dep_traits<MetaData, Task, DepTy>::arg_ini_ready_undo( obj_ext );
    }
};

// Profiling ready? functor
template<typename MetaData, typename Task>
struct dprof_ready_functor {
    template<typename T>
    bool operator () ( indep<T> & obj, typename indep<T>::dep_tags & sa ) {
	OBJ_PROF(arg_in);
	if( dep_traits<MetaData, Task, indep>::arg_ini_ready( obj ) )
	    OBJ_PROF(arg_in_ready);
	return true;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj, typename outdep<T>::dep_tags & sa ) {
	OBJ_PROF(arg_out);
	if( dep_traits<MetaData, Task, outdep>::arg_ini_ready( obj ) )
	    OBJ_PROF(arg_out_ready);
	return true;
    }
    template<typename T>
    bool operator () ( inoutdep<T> & obj, typename inoutdep<T>::dep_tags & sa ) {
	OBJ_PROF(arg_inout);
	if( dep_traits<MetaData, Task, inoutdep>::arg_ini_ready( obj ) )
	    OBJ_PROF(arg_inout_ready);
	else {
	    if( !obj.get_version()->get_metadata()->rename_has_writers()
		&& obj.get_version()->get_metadata()->rename_has_readers() ) {
		OBJ_PROF(arg_inout_ready_may_rename);
	    }
	}
	return true;
    }
#if OBJECT_COMMUTATIVITY
    template<typename T>
    bool operator () ( cinoutdep<T> & obj, typename cinoutdep<T>::dep_tags & sa ) {
	return dep_traits<MetaData, Task, cinoutdep>::arg_ini_ready_undo( obj );
    }
#endif
#if OBJECT_REDUCTION
    template<typename T>
    bool operator () ( reduction<T> & obj, typename reduction<T>::dep_tags & sa ) {
	return dep_traits<MetaData, Task, reduction>::arg_ini_ready_undo( obj );
    }
#endif
    template<typename T, template<typename U> class DepTy>
    bool operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) {
	return true;
    } 

    template<typename T, template<typename U> class DepTy>
    void undo( DepTy<T> & obj_ext, typename DepTy<T>::dep_tags & sa ) {
	return dep_traits<MetaData, Task, DepTy>::arg_ini_ready_undo( obj_ext );
    }
};


// A "grab-and-store function" to store inside the dep_traits.
template<typename MetaData, typename Task, typename... Tn>
static inline void arg_dgrab_fn( Task * fr, obj_dep_traits * odt, bool wakeup, Tn & ... an ) {
	
    dgrab_functor<MetaData, Task> gfn( fr, odt, !wakeup );
    fr->template start_registration<Tn...>();
    char * args = fr->get_task_data().get_args_ptr();
    char * tags = fr->get_task_data().get_tags_ptr();
#if STORED_ANNOTATIONS
    size_t nargs = fr->get_task_data().get_num_args();
	
    arg_apply3_stored_fn<dgrab_functor<MetaData, Task>,Tn...>(
	gfn, nargs, args, tags, an... );
#else
	
    size_t (*off)(size_t) = &offset_of<Tn...>;
	
    arg_apply3_fn<dgrab_functor<MetaData, Task>,Tn...>(
	gfn, off, args, tags, an... );
#endif
	
    fr->stop_registration( wakeup );
}

#if STORED_ANNOTATIONS
// A "ini_ready function" to test readiness of objects at spawn-time.
template<typename MetaData, typename Task>
static inline bool arg_dini_ready_fn( task_data_t & td ) {
    dexpand_functor<MetaData, Task> xfn;
    arg_apply_stored_fn( xfn, td );
    dini_ready_functor<MetaData, Task> rfn;
    return arg_apply_stored_fn( rfn, td );
}

// A "profiling ini_ready function".
template<typename MetaData, typename Task>
static inline bool arg_dprof_ready_fn( task_data_t & td ) {
    dprof_ready_functor<MetaData, Task> rfn;
    return arg_apply_stored_fn( rfn, td );
}
#else
// A "ini_ready function" to test readiness of objects at spawn-time.
template<typename MetaData, typename Task, typename... Tn>
static inline bool arg_dini_ready_fn( Tn ... an ) {
    dexpand_functor<MetaData, Task> xfn;
    arg_dapply_fn( xfn, 0, an... );
    dini_ready_functor<MetaData, Task> rfn;
    return arg_dapply_fn( rfn, 0, an... );
}

// A "profiling ini_ready function".
template<typename MetaData, typename Task, typename... Tn>
static inline bool arg_dprof_ready_fn( Tn ... an ) {
    dprof_ready_functor<MetaData, Task> rfn;
    return arg_dapply_fn( rfn, 0, an... );
}
#endif

// ------------------------------------------------------------------------
// Interface for the spawn(), call(), leaf_call(), ssync() functions.
// ------------------------------------------------------------------------

// Check argument readiness
#if STORED_ANNOTATIONS
template<typename MetaData, typename Task>
static inline bool arg_ready( task_data_t & task_data ) {
    bool ready = arg_dini_ready_fn<MetaData, Task>( task_data );

    OBJ_PROF(arg_ini_ready_calls);
#if PROFILE_OBJECT
    arg_dprof_ready_fn<MetaData, Task>( task_data );
    if( ready )
	OBJ_PROF(arg_ini_ready);
#endif

    return ready;
}
#else
template<typename MetaData, typename Task, typename... Tn>
static inline bool arg_ready( Tn... an ) {
    bool ready = arg_dini_ready_fn<MetaData, Task, Tn...>( an... );

    OBJ_PROF(arg_ini_ready_calls);
#if PROFILE_OBJECT
    arg_dprof_ready_fn<MetaData, Task, Tn...>( an... );
    if( ready )
	OBJ_PROF(arg_ini_ready);
#endif

    return ready;
}
#endif

template<typename... Tn>
struct count_object;

template<>
struct count_object<> {
    static const size_t value = 0;
};

template<typename T, typename... Tn>
struct count_object<T, Tn...> : public count_object<Tn...> {
    static const size_t value =
	( is_object<T>::value ? 1 : 0 ) + count_object<Tn...>::value;
};

template<typename Frame, typename... Tn>
inline bool grab_now( Frame * fr ) {
    return fr->get_parent()->is_full();
}

template<>
inline bool grab_now<pending_frame>( pending_frame * fr ) {
    return true;
}

/*
>> define frame_metadata_traits<Frame> (Frame=SF,FF,PF)
met members: (varianten op enable_deps)
arg_issue<Tn...>( Frame *, bool immediate/delayed ) (rest kennen we lokaal)

>> er is een opkuis mogelijkheid in de arg_issue functies
>> arg_dgrab_and_store_fn vs arg_issue_fn: beiden doen store, naam verschilt
     + depth update op frame
     + arg_dgrab_and_store_fn samen met afzonderlijke frame update depth
>> arg_dgrab_fn: doet geen store, geen obj depth, geen frame depth update

wf_arg_ready_and_grab instead of wf_arg_ready.
wf_arg_issue remains for call/leaf_call interface?

wf_arg_release_and_get_ready_task instead of wf_arg_release...
right child stealing interface with depth disappears then
*/

template<typename Frame>
static inline
stack_frame * stack_frame_or_null( Frame * fr ) { return 0; }

template<>
inline
stack_frame * stack_frame_or_null<stack_frame>( stack_frame * fr ) {
    return fr;
}

template<typename StackFrame, typename FullFrame, typename Task>
static inline
Task * get_full_task( StackFrame * parent ) {
    FullFrame * fp = parent->get_full();
    return stack_frame_traits<FullFrame>::get_metadata( fp );
}

template<typename Frame, typename StackFrame, typename FullFrame,
	 typename QueuedFrame, typename MetaData, typename Task,
	 typename FullTask, typename... Tn>
inline void arg_issue( Frame * fr, StackFrame * parent, Tn & ... an ) {
    assert( fr && "Non-null frame pointer required" );
    OBJ_PROF(arg_issue);

    if( (count_object<Tn...>::value) > 0 ) {
	// Initialize the tags, where required.
	typename stack_frame_traits<Frame>::metadata_ty * ofr
	    = stack_frame_traits<Frame>::get_metadata( fr );
	arg_initialize_tags_fn<Task,Tn...>( ofr );

	if( grab_now( fr ) /*fr->get_parent()->is_full()*/ ) {
	    // errs() << "grab now " << fr << "\n";
	    typename stack_frame_traits<StackFrame>::metadata_ty * opr
		= stack_frame_traits<StackFrame>::get_metadata( parent );
	    ofr->enable_deps( true
#if !STORED_ANNOTATIONS
			      , (void(*)(Task *, obj_dep_traits *))0
			      , &arg_release_fn<MetaData,Task,Tn...>
			      , &arg_cleanup_tags_fn<Task,Tn...>
			      , &arg_hypermap_reduce_fn<Task,Tn...>
#endif
		);
	    ofr->template create< Tn...>(
		get_full_task<StackFrame, FullFrame, FullTask >( parent ) );

	    arg_dgrab_fn<MetaData, Task, Tn...>(
		ofr, opr, std::is_same<Frame,pending_frame>::value, an... );
	} else {
	    // errs() << "grab later " << fr << "\n";
	    ofr->enable_deps( false
#if !STORED_ANNOTATIONS
			      , &arg_issue_fn<MetaData,Task,Tn...>
			      , &arg_release_fn<MetaData,Task,Tn...>
			      , &arg_cleanup_tags_fn<Task,Tn...>
			      , &arg_hypermap_reduce_fn<Task,Tn...>
#endif
		);
	    ofr->template create<Tn...>(
		get_full_task<StackFrame, FullFrame, FullTask >( parent ) );
	}
    } else {
	// errs() << "grab not (no objects) " << fr << "\n";
    }
}

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------
#if STORED_ANNOTATIONS
struct all_tags_base : public annotation_tags { };
#else
struct all_tags_base { };
#endif

struct function_tags_base { };

struct indep_tags_base : public all_tags_base { };
struct outdep_tags_base : public all_tags_base { };
struct inoutdep_tags_base : public all_tags_base { };
struct cinoutdep_tags_base : public all_tags_base { };

template<typename MetaData>
struct reduction_tags_base : public all_tags_base {
    // obj_version<MetaData> int_version;
    obj_version<MetaData> * ext_version;
    int idx;
};

// ------------------------------------------------------------------------
// BOUNDARY BETWEEN OBJECT AND TICKET/TASKGRAPH SPECIALIZATION
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// Importing definitions for task graph
// ------------------------------------------------------------------------
}

#if OBJECT_TASKGRAPH == 1
#include "tickets.h"
#include "queue/queue_t.h"
#else
#if OBJECT_TASKGRAPH == 2
#include "taskgraph.h"
#else
#if OBJECT_TASKGRAPH == 3
#include "etaskgraph.h"
#else
#if OBJECT_TASKGRAPH == 4
#include "egtaskgraph.h"
#else
#if OBJECT_TASKGRAPH == 5 || OBJECT_TASKGRAPH == 9
#include "ctaskgraph.h"
#else
#if OBJECT_TASKGRAPH == 6 || OBJECT_TASKGRAPH == 7 \
    || OBJECT_TASKGRAPH == 10 || OBJECT_TASKGRAPH == 11
#include "ecgtaskgraph.h"
#else
#if OBJECT_TASKGRAPH == 8
#include "vtickets.h"
#else
#if OBJECT_TASKGRAPH == 12
#include "gtickets.h"
#endif
#endif
#endif
#endif
#endif
#endif
#endif
#endif

namespace obj {
#if OBJECT_TASKGRAPH == 3
typedef etg_metadata obj_metadata;
#else
#if OBJECT_TASKGRAPH == 4
typedef egtg_metadata obj_metadata;
#endif
#endif

class truedep_tags { };

// ------------------------------------------------------------------------
// The actual objects used in the programming model
// ------------------------------------------------------------------------
// object_t: object declaration-style interface to versioned objects.
template<typename T, obj_modifiers_t OMod = obj_none>
class object_t
    : public obj_access_traits<T, obj_instance<obj_metadata>,
			       object_t<T, OMod> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>,
			      object_t<T, OMod> > OAT;

private:
    obj_instance<obj_metadata> * copy_back;

public:
    explicit object_t(size_t n = 1) {
	static_assert( !(OMod & obj_recast), "constructor only allowed if..." );
	this->version = obj_version<obj_metadata>::create<T>(n*size_struct<T>::value, this);
    }
#if 0 // needed?
    object_t( const object_t<T, OMod> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	o.version->add_ref();
	this->version = o.version;
    }
#endif

    // For nesting. Nesting is only allowed if the obj_recast flag is set,
    // causing the destructor to copy back the contents of the data.
    object_t( indep<T> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	copy_back = 0;
	this->version = obj_version<obj_metadata>::nest( this, &o );
    }
    object_t( outdep<T> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	copy_back = o.get_object();
	this->version = obj_version<obj_metadata>::nest( this, &o );
    }
    object_t( inoutdep<T> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	copy_back = o.get_object();
	this->version = obj_version<obj_metadata>::nest( this, &o );
    }
#if OBJECT_COMMUTATIVITY
    object_t( cinoutdep<T> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	copy_back = o.get_object();
	this->version = obj_version<obj_metadata>::nest( this, &o );
    }
#endif
#if OBJECT_REDUCTION
    object_t( reduction<T> &o ) {
	static_assert( OMod & obj_recast, "constructor only allowed if..." );
	copy_back = o.get_object();
	this->version = obj_version<obj_metadata>::nest( this, &o );
    }
#endif

    ~object_t() {
	// If nesting applied, we do a simple flat byte copy of the data.
	if( OMod & obj_recast ) { // resolves to a compile-time constant
	    if( copy_back )
		obj_version<obj_metadata>::unnest( this, copy_back );
	}

	// Remove keep-alive reference
	this->version->del_ref();
    }

    const object_t<T, OMod> & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }

    const object_t<T, OMod> & operator += ( const T & t ) {
	*OAT::get_ptr() += t; return *this;
    }
    const object_t<T, OMod> & operator -= ( const T & t ) {
	*OAT::get_ptr() -= t; return *this;
    }
    const object_t<T, OMod> & operator *= ( const T & t ) {
	*OAT::get_ptr() *= t; return *this;
    }
    const object_t<T, OMod> & operator /= ( const T & t ) {
	*OAT::get_ptr() /= t; return *this;
    }

    operator indep<T> () const    { return create_dep_ty< indep<T> >();    }
    operator outdep<T> () const   { return create_dep_ty< outdep<T> >();   }
    operator inoutdep<T> () const { return create_dep_ty< inoutdep<T> >(); }
#if OBJECT_COMMUTATIVITY
    operator cinoutdep<T> () const { return create_dep_ty< cinoutdep<T> >(); }
#endif
#if OBJECT_REDUCTION
    template<typename M>
    operator reduction<M> () const { return create_dep_ty< reduction<M> >(); }
#endif
    operator truedep<T> () const { return create_dep_ty< truedep<T> >(); }

    const object_t<T, OMod> &
    operator = ( const object_t<T, OMod> & o ) {
	o.version->add_ref();
	this->version->del_ref();
	this->version = o.version;
	return *this;
    }

private:
    template<typename DepTy>
    DepTy create_dep_ty() const {
	return OAT::template create_dep_ty< DepTy >();
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

// unversioned: object declaration-style interface to unversioned objects.
template<typename T, obj_modifiers_t OMod = obj_none>
class unversioned
    : public obj_access_traits<T,
			       obj_unv_instance<obj_metadata, T,
						size_struct<T>::value>,
			       unversioned<T, OMod> > {
protected:
    typedef obj_access_traits<T,
			      obj_unv_instance<obj_metadata, T,
					       size_struct<T>::value>,
			      unversioned<T, OMod> > OAT;
    typedef unversioned<T, OMod> self_ty;

public:
    unversioned() { }
    ~unversioned() {
	this->get_version()->nonfreeing_del_ref();
    }

    const self_ty & operator = ( const self_ty & o ) = delete;

    const self_ty & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }

    operator indep<T> () const    { return create_dep_ty< indep<T> >();    }
    operator outdep<T> () const   { return create_dep_ty< outdep<T> >();   }
    operator inoutdep<T> () const   { return create_dep_ty< inoutdep<T> >();   }
#if OBJECT_COMMUTATIVITY
    operator cinoutdep<T> () const { return create_dep_ty< cinoutdep<T> >(); }
#endif
#if OBJECT_REDUCTION
    template<typename M>
    operator reduction<M> () const { return create_dep_ty< reduction<M> >(); }
#endif
    operator truedep<T> () const { return create_dep_ty< truedep<T> >(); }

private:
    template<typename DepTy>
    DepTy create_dep_ty() const {
	return OAT::template create_dep_ty< DepTy >();
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

// ------------------------------------------------------------------------
// The actions taken by the runtime system depend on these classes by their
// static types. Three classes are defined with different implementations of
// the actions.
// ------------------------------------------------------------------------
template<typename T>
class indep
    : public obj_access_traits<T, obj_instance<obj_metadata>, indep<T> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>,
			      indep<T> > OAT;
public:
    typedef obj_metadata metadata_t;
    typedef indep_tags dep_tags;
    typedef indep_type_tag _object_tag;
    
    // Should not assign to indep because it is read-only
    // const indep<T> & operator = ( const T & t ) = delete;
    // { *OAT::get_ptr() = t; return *this; }

    static indep<T> create( obj_version<obj_metadata> * v );
public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<typename T>
class outdep
    : public obj_access_traits<T, obj_instance<obj_metadata>, outdep<T> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>, outdep<T> > OAT;
public:
    typedef obj_metadata metadata_t;
    typedef outdep_tags dep_tags;
    typedef outdep_type_tag _object_tag;

    const outdep<T> & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }
	
    static outdep<T> create( obj_version<obj_metadata> * v );

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<typename T>
class inoutdep
    : public obj_access_traits<T, obj_instance<obj_metadata>, inoutdep<T> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>, inoutdep<T> > OAT;
public:
    typedef obj_metadata metadata_t;
    typedef inoutdep_tags dep_tags;
    typedef inoutdep_type_tag _object_tag;

    const inoutdep<T> & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }
	
    static inoutdep<T> create( obj_version<obj_metadata> * v );

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

#if OBJECT_COMMUTATIVITY
template<typename T>
class cinoutdep
    : public obj_access_traits<T, obj_instance<obj_metadata>, cinoutdep<T> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>, cinoutdep<T> > OAT;
public:
    typedef obj_metadata metadata_t;
    typedef cinoutdep_tags dep_tags; // -- not yet defined in all TG
    typedef cinoutdep_type_tag _object_tag;

    const cinoutdep<T> & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }
	
    static cinoutdep<T> create( obj_version<obj_metadata> * v );

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};
#endif

#if OBJECT_REDUCTION
template<typename M>
class reduction
    : public obj_access_traits<typename M::value_type, obj_instance<obj_metadata>, reduction<M> > {
protected:
    typedef obj_access_traits<typename M::value_type,
			      obj_instance<obj_metadata>, reduction<M> > OAT;
public:
    typedef reduction_tags dep_tags; // -- not yet defined in all TG
    typedef reduction_type_tag _object_tag;

    const reduction<M> & operator = ( const typename M::value_type & t ) {
	*OAT::get_ptr() = t; return *this;
    }
	
    static reduction<M> create( obj_version<obj_metadata> * v );

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};
#endif

template<typename T>
class truedep
    : public obj_access_traits<T, obj_instance<obj_metadata>, truedep<T> > {
protected:
    typedef obj_access_traits<T, obj_instance<obj_metadata>, truedep<T> > OAT;
public:
    typedef obj_metadata metadata_t;
    typedef truedep_tags dep_tags;
    typedef truedep_type_tag _object_tag;

    const truedep<T> & operator = ( const T & t ) {
	*OAT::get_ptr() = t; return *this;
    }

    static truedep<T> create( obj_version<obj_metadata> * v );
	
public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

// Creating indep and outdep classes.
template<typename T>
indep<T> indep<T>::create( obj_version<obj_metadata> * v ) {
    indep<T> od;
    od.version = v;
    return od;
}

template<typename T>
outdep<T> outdep<T>::create( obj_version<obj_metadata> * v ) {
    outdep<T> od;
    od.version = v;
    return od;
}

template<typename T>
inoutdep<T> inoutdep<T>::create( obj_version<obj_metadata> * v ) {
    inoutdep<T> od;
    od.version = v;
    return od;
}

#if OBJECT_COMMUTATIVITY
template<typename T>
cinoutdep<T> cinoutdep<T>::create( obj_version<obj_metadata> * v ) {
    cinoutdep<T> od;
    od.version = v;
    return od;
}
#endif

#if OBJECT_REDUCTION
template<typename T>
reduction<T> reduction<T>::create( obj_version<obj_metadata> * v ) {
    reduction<T> od;
    od.version = v;
    return od;
}
#endif

template<typename T>
truedep<T> truedep<T>::create( obj_version<obj_metadata> * v ) {
    truedep<T> od;
    od.version = v;
    return od;
}

template<>
struct dep_traits<obj_metadata, task_metadata, truedep> {
    template<typename T>
    static inline void arg_issue( task_metadata * fr, truedep<T> & obj_ext,
				  typename truedep<T>::dep_tags * sa ) { }
    template<typename T>
    static inline bool arg_ini_ready( const truedep<T> & obj_int ) {
	return true;
    }
    template<typename T>
    static inline void arg_release( task_metadata * fr, truedep<T> & obj,
				    typename truedep<T>::dep_tags & sa ) { }
};

// ------------------------------------------------------------------------
// Tokens - specialize object_t, unversioned, and dep types to void argument
// ------------------------------------------------------------------------
template<>
class indep<void> : public obj_instance<token_metadata> {
public:
    typedef token_metadata metadata_t;
    typedef indep_tags dep_tags;
    typedef indep_type_tag _object_tag;
    
    static indep<void> create( obj_version<token_metadata> * v ) {
	indep<void> od;
	od.version = v;
	return od;
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<>
class outdep<void> : public obj_instance<token_metadata> {
public:
    typedef token_metadata metadata_t;
    typedef outdep_tags dep_tags;
    typedef outdep_type_tag _object_tag;

    static outdep<void> create( obj_version<token_metadata> * v ) {
	outdep<void> od;
	od.version = v;
	return od;
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<>
class inoutdep<void> : public obj_instance<token_metadata> {
public:
    typedef token_metadata metadata_t;
    typedef inoutdep_tags dep_tags;
    typedef inoutdep_type_tag _object_tag;

    static inoutdep<void> create( obj_version<token_metadata> * v ) {
	inoutdep<void> od;
	od.version = v;
	return od;
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<>
class truedep<void> : public obj_instance<token_metadata> {
public:
    typedef token_metadata metadata_t;
    typedef truedep_tags dep_tags;
    typedef truedep_type_tag _object_tag;

    static truedep<void> create( obj_version<token_metadata> * v ) {
	truedep<void> od;
	od.version = v;
	return od;
    }
	
public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

// unversioned: object declaration-style interface to unversioned objects.
template<obj_modifiers_t OMod>
class unversioned<void, OMod>
    : public obj_unv_instance<token_metadata, void, 0> {
public:
    unversioned() { }
    ~unversioned() {
	this->get_version()->nonfreeing_del_ref();
    }

    operator indep<void> () const    { return create_dep_ty< indep >();    }
    operator outdep<void> () const   { return create_dep_ty< outdep >();   }
    operator inoutdep<void> () const { return create_dep_ty< inoutdep >();   }
    operator truedep<void> () const  { return create_dep_ty< truedep >(); }

private:
    template<template<typename U> class DepTy>
    DepTy<void> create_dep_ty() const {
	return DepTy<void>::create( get_nc_version() );
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

// ------------------------------------------------------------------------
// Classes to support dependence checking on pending frames and executing
// frames.
// ------------------------------------------------------------------------
class obj_dep_traits {
    typedef void (*issue_fn_t)( task_metadata *, obj_dep_traits * );
    typedef void (*release_fn_t)( task_metadata *, bool );
    typedef void (*cleanup_fn_t)( task_metadata *, bool );
    typedef void (*reduce_fn_t)( task_metadata * );

    enum state_t {
	s_nodep,
	s_issued,
	s_notissued
    };
#if !STORED_ANNOTATIONS
    issue_fn_t issue_fn;
    release_fn_t release_fn;
    cleanup_fn_t cleanup_fn;
    reduce_fn_t reduce_fn;
#endif
    state_t state;
    bool pad[7]; // this padding is here because inherited_size<> does not work

    typedef obj::obj_version<obj::obj_metadata> obj_version;
    std::vector<obj_version *> finalize[2];

protected:
    // Only initialize task_data_p. If task_data_p is non-zero, then other
    // fields must also be initialized.
    obj_dep_traits() : state( s_nodep ) { }

    // Initialize from pending frame
    void create_from_pending( obj_dep_traits * odt ) {
#if !STORED_ANNOTATIONS
	release_fn = odt->release_fn;
	cleanup_fn = odt->cleanup_fn;
	reduce_fn = odt->reduce_fn;
#endif
	state = s_issued;
	finalize[0].swap( odt->finalize[0] );
	finalize[1].swap( odt->finalize[1] );
    }

public:
    void release_deps( task_metadata * fr, bool is_stack ) {
	if( enabled() ) {
#if STORED_ANNOTATIONS
	    arg_release_fn<obj_metadata,task_metadata>( fr, is_stack );
#else
	    (*release_fn)( fr, is_stack );
#endif
	} else if( state != s_nodep ) {
#if STORED_ANNOTATIONS
	    arg_cleanup_tags_fn<obj_metadata,task_metadata>( fr );
#else
	    (*cleanup_fn)( fr, is_stack );
#endif
	}
    }

    void reduce( task_metadata * fr ) {
	if( enabled() ) {
#if STORED_ANNOTATIONS
	    arg_hypermap_reduce_fn<obj_metadata,task_metadata>( fr );
#else
	    (*reduce_fn)( fr );
#endif
	}
    }

    void enable_deps( bool already_enabled
#if !STORED_ANNOTATIONS
		      , issue_fn_t grfn
		      , release_fn_t refn
		      , cleanup_fn_t cffn
		      , reduce_fn_t rdfn
#endif
	) {
#if !STORED_ANNOTATIONS
	issue_fn = grfn;
	release_fn = refn;
	cleanup_fn = cffn;
	reduce_fn = rdfn;
#endif
	state = already_enabled ? s_issued : s_notissued;
    }

    void convert_to_full( task_metadata * fr, obj_dep_traits * parent ) {
	// do we have arguments with dependencies?
	if( state == s_notissued ) {
#if STORED_ANNOTATIONS
	    arg_issue_fn<obj_metadata, task_metadata>( fr, parent );
#else
	    (*issue_fn)( fr, parent );
#endif
	    state = s_issued;
	}
    }

    bool enabled() const { return state == s_issued; }

    void add_finalize_version( obj_version * v, bool tasking ) {
	// errs() << "add_finalize " << v << "\n";
	v->add_ref();
	finalize[tasking].push_back( v );
    }
    void run_finalizers( task_metadata * fr, bool tasking ) {
	// For every hyperqueue argument, perform the reduction 
	// USER <- CHILDREN + USER
	if( !tasking )
	    reduce( fr );

	// For reduction<M> object usage
	// errs() << "run_finalizers tasking=" << tasking << "\n";
#if OBJECT_REDUCTION
	if( tasking ) {
	    for( auto I=finalize[1].begin(), E=finalize[1].end(); I != E; ++I ) {
		obj_version * v = *I;
		// errs() << "expand " << *I << "\n";
		v->expand();
		v->del_ref();
	    }
	    finalize[1].clear();
	} else {
	    for( auto I=finalize[0].begin(), E=finalize[0].end(); I != E; ++I ) {
		obj_version * v = *I;
		// errs() << "finalize " << *I << "\n";
		v->finalize();
		v->del_ref();
	    }
	    finalize[0].clear();
	}
#endif
    }
};

// Attach state and functionality to pending_frame
class pending_frame_base_obj : public pending_metadata,
			       public obj_dep_traits {
protected:
    pending_frame_base_obj() { }
    ~pending_frame_base_obj() { }

public:
    const task_data_t & get_task_data() const {
	return *static_cast<const task_data_t*>( this );
    }
    task_data_t & get_task_data() {
	return *static_cast<task_data_t*>( this );
    }
};

// Attach state and functionality to stack_frame
class stack_frame_base_obj : public task_metadata,
			     public obj_dep_traits {
protected:
    stack_frame_base_obj() { }
    ~stack_frame_base_obj() { }

public:
    const task_data_t & get_task_data() const {
	return *static_cast<const task_data_t*>( this );
    }
    task_data_t & get_task_data() {
	return *static_cast<task_data_t*>( this );
    }

public:
    void create_from_pending( pending_frame_base_obj * pnd,
			      full_metadata * parent_full ) {
	obj_dep_traits::create_from_pending( pnd );
	task_metadata::create_from_pending( pnd, parent_full );
    }
    void convert_to_full( stack_frame_base_obj * fr,
			  stack_frame_base_obj * parent,
			  full_metadata * parent_full ) {
	obj_dep_traits::convert_to_full( fr, parent );
	task_metadata::convert_to_full( parent_full );
    }
};

// Attach state and functionality to full_frame
// typedef full_metadata full_frame_base_obj;
class full_frame_base_obj : public full_metadata { };

} // namespace obj

//----------------------------------------------------------------------
// Generic traits interface for object-level dependency tracking
//----------------------------------------------------------------------

template<typename StackFrame, typename FullFrame, typename PendingFrame, typename QueuedFrame>
struct task_graph_traits {
    // Actions on a stack_frame
    static void
    release_task( StackFrame * fr ) {
	typename stack_frame_traits<StackFrame>::metadata_ty * ofr
	    = stack_frame_traits<StackFrame>::get_metadata( fr );
	assert( !ofr->enabled() || fr->get_parent()->is_full() );
	ofr->release_deps( ofr, true );
    }
    static void
    release_task( PendingFrame * fr ) {
	typename stack_frame_traits<PendingFrame>::metadata_ty * ofr
	    = stack_frame_traits<PendingFrame>::get_metadata( fr );
	assert( ofr->enabled() );
	assert( 0 && "When does this get called?" );
	ofr->release_deps( ofr );
    }
    static QueuedFrame *
    release_task_and_get_ready( FullFrame * fr ) {
	typename stack_frame_traits<StackFrame>::metadata_ty * ofr
	    = stack_frame_traits<StackFrame>::get_metadata( fr->get_frame() );
	typename stack_frame_traits<FullFrame>::metadata_ty * opf
	    = stack_frame_traits<FullFrame>::get_metadata( fr->get_parent() );
	// assert( !ofr->enabled() || fr->get_parent()->is_full() );
	ofr->release_deps( ofr, false );
	return (QueuedFrame *)opf->get_ready_task_after( ofr );
    }
    static void
    convert_to_full( StackFrame * fr, FullFrame * ff ) {
	typename stack_frame_traits<StackFrame>::metadata_ty * ofr
	    = stack_frame_traits<StackFrame>::get_metadata( fr );
	typename stack_frame_traits<StackFrame>::metadata_ty * opr
	    = stack_frame_traits<StackFrame>::get_metadata( fr->get_parent() );
	typename stack_frame_traits<FullFrame>::metadata_ty * opf
	    = stack_frame_traits<FullFrame>::get_metadata( fr->get_parent()->get_full() );
	// Grabs the arguments, doesn't touch the full frame.
	// Note: obj_dep_traits::convert_to_full( task_metadata * )
	ofr->convert_to_full( ofr, opr, opf );
    }
    static void
    create_from_pending( StackFrame * fr, PendingFrame * pnd ) {
	typename stack_frame_traits<StackFrame>::metadata_ty * ofr
	    = stack_frame_traits<StackFrame>::get_metadata( fr );
	typename stack_frame_traits<FullFrame>::metadata_ty * ofp
	    = stack_frame_traits<FullFrame>::get_metadata(
		fr->get_parent()->get_full() );
	typename stack_frame_traits<PendingFrame>::metadata_ty * opnd
	    = stack_frame_traits<PendingFrame>::get_metadata( pnd );
	ofr->create_from_pending( opnd, ofp );
    }
    static void
    run_finalizers( StackFrame * fr, bool tasking ) {
	typename stack_frame_traits<StackFrame>::metadata_ty * ofr
	    = stack_frame_traits<StackFrame>::get_metadata( fr );
	ofr->run_finalizers( ofr, tasking );
    }

    // Actions on a full_frame
    static void
    push_pending( FullFrame * fr, QueuedFrame * qf ) {
	typename stack_frame_traits<FullFrame>::metadata_ty * ofr
	    = stack_frame_traits<FullFrame>::get_metadata( fr );
	typename stack_frame_traits<QueuedFrame>::metadata_ty * oqf
	    = stack_frame_traits<QueuedFrame>::get_metadata( qf );
	ofr->push_pending( oqf );
    }
    static QueuedFrame *
    get_ready_task( FullFrame * ff ) {
	typename stack_frame_traits<FullFrame>::metadata_ty * off
	    = stack_frame_traits<FullFrame>::get_metadata( ff );
	return (QueuedFrame *)off->get_ready_task();
    }

    // wf_interface hooks
#if STORED_ANNOTATIONS
    template<typename... Tn>
    static void
    arg_stored_initialize( task_data_t & task_data_p ) {
	obj::arg_stored_init_fn<Tn...>( task_data_p );
    }
    static bool
    arg_ready( task_data_t & task_data ) {
	return obj::arg_ready<obj::obj_metadata, obj::task_metadata>(
	    task_data );
    }
#else
    template<typename... Tn>
    static bool
    arg_ready( Tn... an ) {
	return obj::arg_ready<obj::obj_metadata, obj::task_metadata, Tn...>( an... );
    }
#endif
    template<typename Frame, typename... Tn>
    static void
    arg_issue( Frame * fr, stack_frame * parent, Tn & ... an ) {
	obj::arg_issue<Frame, StackFrame, FullFrame, QueuedFrame,
	    obj::obj_metadata, obj::task_metadata, obj::full_metadata,
	    Tn...>( fr, parent, an... );
    }
    template<typename... Tn>
    static void
    arg_release( Tn... an ) {
	if( (obj::count_object<Tn...>::value) > 0 ) {
	    fprintf( stderr, "ERROR: leaf_call() should never have "
		     "object usage declarations" );
	    abort();
	}

	// This case corresponds to leaf_call. Leaf_call's do not push
	//obj::arg_release<obj::obj_metadata, obj::task_metadata, Tn...>( an... );
    }
    template<typename... Tn>
    static size_t
    arg_stored_size() { return obj::arg_stored_size<Tn...>(); }
    static size_t
    fn_stored_size() { return obj::fn_stored_size<obj::function_tags>(); }
    template<typename... Tn>
    static bool
    arg_introduces_deps() { return obj::count_object<Tn...>::value > 0; }
};


//----------------------------------------------------------------------
// Traits for the delayed copy task
//----------------------------------------------------------------------
template<typename T>
struct delayed_copy_traits< obj::indep<T> > {
    static char * get_address( obj::indep<T> obj ) { 
	return (char *)obj.get_version()->get_ptr();
    }
    static size_t get_size( obj::indep<T> obj ) {
	return obj.get_version()->get_size();
    }
};

template<typename T>
struct delayed_copy_traits< obj::outdep<T> > {
    static char * get_address( obj::outdep<T> obj ) { 
	return (char *)obj.get_version()->get_ptr();
    }
    static size_t get_size( obj::outdep<T> obj ) {
	return obj.get_version()->get_size();
    }
};

//----------------------------------------------------------------------
// For x86_64 calling conventions
//----------------------------------------------------------------------
#ifdef __x86_64__
#include "swan/platform_x86_64.h"

namespace platform_x86_64 {

// Specialization - declare to the implementation of the calling convention
// that obj::indep a.o. are a struct with two members. The implementation
// of the calling convention will then figure out whether this struct is
// passed in registers or not. (If it is not - you should not pass it and
// a compile-time error will be triggered). This approach is a low-cost
// way to by-pass the lack of data member introspection in C++.
template<size_t ireg, size_t freg, size_t loff, typename IT, obj::obj_modifiers_t OMod>
struct arg_passing<ireg, freg, loff, obj::object_t<IT, OMod> >
    : arg_passing_struct1<ireg, freg, loff, obj::object_t<IT, OMod>, obj::obj_version<obj::obj_metadata> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename IT, obj::obj_modifiers_t OMod>
struct arg_passing<ireg, freg, loff, obj::unversioned<IT, OMod> >
    : arg_passing_struct1<ireg, freg, loff, obj::unversioned<IT, OMod>, obj::obj_version<obj::obj_metadata> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::indep<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::indep<IT>, obj::obj_version<obj::obj_metadata> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::outdep<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::outdep<IT>, obj::obj_version<obj::obj_metadata> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::inoutdep<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::inoutdep<IT>, obj::obj_version<obj::obj_metadata> *> {
};

#if OBJECT_COMMUTATIVITY
template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::cinoutdep<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::cinoutdep<IT>, obj::obj_version<obj::obj_metadata> *> {
};
#endif

#if OBJECT_REDUCTION
template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::reduction<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::reduction<IT>, obj::obj_version<obj::obj_metadata> *> {
};
#endif

template<size_t ireg, size_t freg, size_t loff, typename IT>
struct arg_passing<ireg, freg, loff, obj::truedep<IT> >
    : arg_passing_struct1<ireg, freg, loff, obj::truedep<IT>, obj::obj_version<obj::obj_metadata> *> {
};

} // namespace platform_x86_64

#endif

#else // ! OBJECT_TASKGRAPH != 0

//----------------------------------------------------------------------
// Define dummy base classes for stack_frame, pending_frame and full_frame
// to inherit from.
//----------------------------------------------------------------------

namespace obj {

static inline void dump_statistics() { }

struct pending_frame_base_obj { };
struct stack_frame_base_obj { };
struct full_frame_base_obj { };

}

template<typename StackFrame, typename FullFrame, typename PendingFrame, typename QueuedFrame>
struct task_graph_traits {
    // Actions on a stack_frame
    static void
    release_task( StackFrame * fr );
    static QueuedFrame *
    release_task_and_get_ready( StackFrame * fr );
    static void
    convert_to_full( StackFrame * fr, FullFrame * ff ) { }
    static void
    create_from_pending( StackFrame * fr, PendingFrame * pnd ) { }
    static void
    run_finalizers( StackFrame * fr, bool tasking ) { }

    // Actions on a full_frame
    static void
    push_pending( FullFrame * ff, QueuedFrame * qf ) {
	fprintf( stderr, "ERROR: attempting push_pending() in absence "
		 "of task graph\n" );
	abort();
    }
    static QueuedFrame *
    get_ready_task( FullFrame * ff ) { return 0; }

    // wf_interface hooks
    template<typename... Tn>
    static bool
    arg_ready( Tn... an ) { return true; }
    template<typename Frame, typename... Tn>
    static void
    arg_issue( Frame * fr, Tn & ... an ) { }
    template<typename... Tn>
    static void
    arg_release( Tn... an ) { }
    template<typename... Tn>
    static size_t
    arg_stored_size() { return 0; }
    static size_t
    fn_stored_size() { return 0; }
    template<typename... Tn>
    static bool
    arg_introduces_deps() { return false; }
};

#endif // OBJECT_TASKGRAPH == 0

#endif // OBJECT_H

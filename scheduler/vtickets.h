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
/* vtickets.h
 * This file implements a ticket-based task graph where edges between tasks
 * are not explicitly maintained. The head and tail counters are optimized
 * using vector operations (SSE) and they are optionally laid out in separate
 * cache blocks.
 *
 * @note
 *   The implementation differs slightly from that described in the PACT'11
 *   paper as both readers and writers are incremented for an outdep. The
 *   reasoning to this is that is cheaper to increment a counter during issue
 *   and release than it is to perform additional control flow during release
 *   to avoid those counter increments.
 *   Furthermore, we propose to remove the increment of the readers counter
 *   for an in/out dep because it is redundant.
 *
 * Note: this file has been updated with support for queues, but this has
 *       not been thoroughly tested. Support for tokens is there but not
 *       optimized.
 */
#ifndef VTICKETS_H
#define VTICKETS_H

#include <cstdint>
#include <iostream>

#include "swan/swan_config.h"
#include "swan/wf_frames.h"
#include "swan/lfllist.h"
#include "swan/lock.h"
#include "swan/functor/tkt_ready.h"

namespace obj {

// The type holding the depth of an object in the task graph.
// The depth should not wrap around. If so, adopted implementations
// of max should be used in several dependency action traits.
typedef uint64_t depth_t;

// ----------------------------------------------------------------------
// vtkt_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class vtkt_metadata {
    // typedef uint32_t ctr_t;
    typedef int32_t ctr_t;
    typedef ctr_t v4si __attribute__((vector_size(16)));
    typedef float v4sf __attribute__((vector_size(16)));
    typedef long v2di __attribute__((vector_size(16)));
public:
    typedef v4si tag_t;

private:
    template<size_t idx>
    static v4si mask() {
	return (v4si){ (idx&1)?~0:0, (idx&2)?~0:0,
		(idx&4)?~0:0, (idx&8)?~0:0 }; 
    }
    template<size_t idx>
    static inline bool pveq( v4si x, v4si y );

private:
    union {
	v4si vtail;
	struct {
	    ctr_t r_tail;
	    ctr_t w_tail;
	    ctr_t c_tail;
	    ctr_t d_tail;
	};
    };
    pad_multiple<CACHE_ALIGNMENT, sizeof(v4si)> pad0;
    union {
	v4si vhead;
	struct {
	    ctr_t r_head;
	    ctr_t w_head;
	    ctr_t c_head;
	    ctr_t d_head;
	};
    };
#if OBJECT_COMMUTATIVITY
    cas_mutex mutex;              // ensure exclusion on commutative operations
#endif
    depth_t depth;                // depth in task graph

public:
    enum vidx {
	v_readers = 1,
	v_writers = 2,
	v_commut  = 4,
	v_reduct  = 8
    };

    ctr_t adv( ctr_t & c ) { return __sync_fetch_and_add( &c, 1 ); }

public:
    vtkt_metadata() : vtail( (v4si){0,0,0,0} ), vhead( (v4si){0,0,0,0} ), depth( 0 ) { }
    ~vtkt_metadata() {
	assert( !rename_is_active()
		&& "Must have zero active tasks when destructing obj_version" );
    }

    // External interface
    bool rename_is_active() const {
	return has_any<v_readers|v_writers|v_commut|v_reduct>();
    }
    bool rename_has_readers() const { return has_any<v_readers>(); }
    bool rename_has_writers() const {
	return has_any<v_writers|v_commut|v_reduct>();
    }

    // Generic
    tag_t get_tag() const { return vtail; }

    template<int idx>
    bool has_any() const { return !pveq<idx>( vhead, vtail ); }
    template<int idx>
    bool has_none() const { return pveq<idx>( vhead, vtail ); }
    template<int idx>
    bool chk_tag( tag_t tag ) const { return pveq<idx>( vhead, tag ); }

    // Track oustanding readers with a head and tail counter
    void add_reader() { adv( r_tail ); }
    void del_reader() { adv( r_head ); }

    // Track oustanding writers with a head and tail counter
    void add_writer() { adv( w_tail ); }
    void del_writer() { adv( w_head ); }

#if OBJECT_COMMUTATIVITY
    // Track oustanding commutatives with a head and tail counter
    void add_commutative() { adv( c_tail ); }
    void del_commutative() { adv( c_head ); }

    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return mutex.try_lock(); }
    void commutative_release() { mutex.unlock(); }
#endif

#if OBJECT_REDUCTION
    // Track oustanding reductions with a head and tail counter
    void add_reduction() { adv( d_tail ); }
    void del_reduction() { adv( d_head ); }
#endif

    depth_t get_depth() const { return depth; }
    void update_depth( depth_t d ) {
	// In principle, an outdep has depth == 0 and an inoutdep has depth <= d
	// because d = max(depth_in) over all indep and inoutdep.
	// Note: an unversioned outdep has depth > 0 and must also be
	// included in the depth computation of the frame!
	// depth = std::max( depth, d );
	assert( depth <= d );
	depth = d;
    }

#ifdef UBENCH_HOOKS
    void update_depth_ubench( depth_t d ) { depth = d; }
#endif

    friend std::ostream & operator << ( std::ostream & os, const vtkt_metadata & md );
};

#if defined( __SSE4_2__ )
template<size_t idx>
bool vtkt_metadata::pveq( v4si x, v4si y ) {
    return __builtin_ia32_ptestz128( (v2di)(x-y), (v2di)mask<idx>() );
}
#else
#if defined( __SSE2__ )
template<size_t idx>
bool vtkt_metadata::pveq( v4si x, v4si y ) {
    v4si tmp = __builtin_ia32_pcmpeqd128( x, y );
    // Transforming this
    // return (__builtin_ia32_movmskps( (v4sf)tmp ) & idx) == idx;
    // into the following is beneficial because andl sets the flags
    return (~__builtin_ia32_movmskps( (v4sf)tmp ) & idx) == 0;
}
// Specialization: movmskps/v4si will set at most 4 bits and clear the rest
template<>
inline bool vtkt_metadata::pveq<15>( v4si x, v4si y ) {
    v4si tmp = __builtin_ia32_pcmpeqd128( x, y );
    return __builtin_ia32_movmskps( (v4sf)tmp ) == 15;
}
// Specialization: shr vs andl
// template<>
// inline bool vtkt_metadata::pveq<14>( v4si x, v4si y ) {
    // v4si tmp = __builtin_ia32_pcmpeqd128( x, y );
    // return (__builtin_ia32_movmskps( (v4sf)tmp ) >> 1) == 7;
// }
#else
#error SSE2 or SSE4.2 support is required for vector-tickets
#endif
#endif

// Some debugging support
inline std::ostream & operator << ( std::ostream & os, const vtkt_metadata & md ) {
    os << "ticket_md={readers={" << md.r_head << ", " << md.r_tail << '}'
       << ", writers={" << md.w_head << ", " << md.w_tail << '}'
#if OBJECT_COMMUTATIVITY
       << ", commutative={" << md.c_head << ", " << md.c_tail << '}'
#endif
#if OBJECT_REDUCTION
       << ", reductions={" << md.d_head << ", " << md.d_tail << '}'
#endif
       << '}';
    return os;
}

// ----------------------------------------------------------------------
// A function to do the first scan over the arguments in the computation
// of the depth of the newly created task and its arguments.
// The first scan updates the depth of the frame based on its in and inout
// arguments.
// The second scan is part of dep_traits<>::arg_issue() and updates the
// depth of the out and inout arguments.
// ----------------------------------------------------------------------
// Update-depth functor
template<typename MetaData, typename Task>
struct update_depth_functor {
    Task * fr;
    update_depth_functor( Task * fr_ ) : fr( fr_ ) { fr->reset_depth(); }

    template<typename T, template<typename U> class DepTy >
    inline
    bool operator () ( DepTy<T> & obj, typename DepTy<T>::dep_tags & sa ) {
	// Don't update depth for outdep dependencies
	if( !is_outdep< DepTy<T> >::value )
	    fr->update_depth( obj.get_version()->get_metadata()->get_depth() );
	return true;
    }
    template<typename T>
    inline
    bool operator () ( truedep<T> & obj, typename truedep<T>::dep_tags & sa ) {
	return true;
    }
};

// A "update depth" function to adjust the depth of the objects in the taskgraph
template<typename Task, typename MetaData, typename... Tn>
static inline void arg_update_depth_fn( Task * fr, char * argp, char * tagp ) {
    update_depth_functor<MetaData, Task> dfn( fr );
    arg_apply_fn<update_depth_functor<MetaData, Task>,Tn...>( dfn, argp, tagp );
}

// ----------------------------------------------------------------------
// task_metadata: dependency-tracking metadata for tasks (pending and stack)
// ----------------------------------------------------------------------
class full_metadata;

class task_metadata : public task_data_t {
    depth_t depth;

protected:
    // Default constructor
    task_metadata() { }
    // Constructor for creating stack frame from pending frame
    void create_from_pending( task_metadata * fr, full_metadata * ) {
	depth = fr->depth;
    }
    void convert_to_full( full_metadata * ) { }

public:
    template<typename... Tn>
    void create( full_metadata * ff ) { }

    void reset_depth() { depth = 0; }
    depth_t get_depth() const { return depth; }
    void update_depth( depth_t d ) {
	depth = std::max( depth, d+1 );
    }

    // Stubs required for the taskgraph variant, here meaningless.
    template<typename... Tn>
    void start_registration() {
	arg_update_depth_fn<task_metadata, vtkt_metadata, Tn...>(
	    this, get_task_data().get_args_ptr(),
	    get_task_data().get_tags_ptr() );
    }
    void stop_registration( bool wakeup = false ) { }

    void start_deregistration() { }
    inline void stop_deregistration( full_metadata * parent );
};

// ----------------------------------------------------------------------
// link_metadata: task graph metadata per stored frame
// ----------------------------------------------------------------------
class pending_metadata;

class link_metadata {
    pending_metadata * prev, * next; // no need to initialize
    friend class dl_list_traits<pending_metadata>;
};

// ----------------------------------------------------------------------
// pending_metadata: task graph metadata per pending frame
// ----------------------------------------------------------------------
class pending_metadata : public task_metadata, private link_metadata {
    typedef bool (*ready_fn_t)( const task_data_t & );

    ready_fn_t ready_fn;

protected:
    pending_metadata() : ready_fn( 0 ) { }

public:
    template<typename... Tn>
    void create( full_metadata * ff ) {
	ready_fn = &arg_ready_fn<vtkt_metadata, task_metadata, Tn...>;
	task_metadata::create<Tn...>( ff );
    }

    bool is_ready() const {
	return ready_fn && (*ready_fn)( get_task_data() );
    }

    static obj::link_metadata *
    get_link_md( pending_metadata * fr ) {
	return static_cast<obj::link_metadata *>( fr );
    }
    static const obj::link_metadata *
    get_link_md( const pending_metadata * fr ) {
	return static_cast<const obj::link_metadata *>( fr );
    }
    static obj::task_metadata *
    get_task_md( pending_metadata * fr ) {
	return static_cast<obj::task_metadata *>( fr );
    }
    static const obj::task_metadata *
    get_task_md( const pending_metadata * fr ) {
	return static_cast<const obj::task_metadata *>( fr );
    }
};

//----------------------------------------------------------------------
// Traits for accessing elements stored in a dl_list<>, in global
// namespace.
//----------------------------------------------------------------------

} // end namespace obj because the traits class must be in global namespace

template<>
struct dl_list_traits<obj::pending_metadata> {
    typedef obj::pending_metadata T;
    typedef obj::pending_metadata ValueType;

    static size_t get_depth( T const * elm ) { return TF( elm )->get_depth(); }
    static bool is_ready( T const * elm ) { return elm->is_ready(); }

    static void set_prev( T * elm, T * prev ) { LF( elm )->prev = prev; }
    static T * get_prev( T const * elm ) { return LF( elm )->prev; }
    static void set_next( T * elm, T * next ) { LF( elm )->next = next; }
    static T * get_next( T const * elm ) { return LF( elm )->next; }

    // static ValueType * get_value( const T * elm ); -- not implemented

private:
    static obj::link_metadata * LF( T * fr ) { return T::get_link_md( fr ); }
    static const obj::link_metadata * LF( T const * fr ) { return T::get_link_md( fr ); }
    static obj::task_metadata * TF( T * fr ) { return T::get_task_md( fr ); }
    static const obj::task_metadata * TF( T const * fr ) { return T::get_task_md( fr ); }
};

namespace obj { // reopen

// ----------------------------------------------------------------------
// full_metadata: task graph metadata per full frame
// ----------------------------------------------------------------------
class full_metadata {
protected:
    hashed_list<pending_metadata> * pending;
    size_t maybe_ready;

protected:
full_metadata() : pending( 0 ), maybe_ready( 0 ) { }
    ~full_metadata() { delete pending; }

public:
    void push_pending( pending_metadata * frame ) {
	allocate_pending();
	pending->prepend( frame );
    }

    pending_metadata *
    get_ready_task() {
	pending_metadata * rdy = 0;
	if( pending && gate_scan() ) { // no scan if nothing's there for sure
	    rdy = pending->get_ready();
	    if( rdy ) // maybe there's more
		enable_scan();
	}
	return rdy;
    }

    pending_metadata *
    get_ready_task_after( task_metadata * prev ) {
	pending_metadata * rdy = 0;
	if( pending && gate_scan() ) { // no scan if nothing's there for sure
	    depth_t prev_depth = prev->get_depth();
	    rdy = pending->get_ready( prev_depth );
	    if( rdy ) // maybe there's more
		enable_scan();
	}
	return rdy;
    }

#ifdef UBENCH_HOOKS
    void reset() {
	delete pending;
	pending = 0;
    }
#endif

private:
    void allocate_pending() {
	if( !pending )
	    pending = new hashed_list<pending_metadata>;
    }

    bool gate_scan() {
	if( !maybe_ready )
	    return false;
	return __sync_bool_compare_and_swap( &maybe_ready, true, false );
    }
public:
    void enable_scan() {
	maybe_ready = true;
    }
};

void task_metadata::stop_deregistration( full_metadata * parent ) {
    parent->enable_scan();
}

// ----------------------------------------------------------------------
// Generic fully-serial dependency handling traits
// ----------------------------------------------------------------------

// A fully serialized version
class serial_dep_tags {
protected:
    friend class serial_dep_traits;
    vtkt_metadata::tag_t tag;
};

struct serial_dep_traits {
    static
    void arg_issue( task_metadata * fr,
		    obj_instance<vtkt_metadata> & obj,
		    serial_dep_tags * tags ) {
	vtkt_metadata * md = obj.get_version()->get_metadata();
	tags->tag = md->get_tag();
	md->add_writer();
	md->update_depth( fr->get_depth() );
    }
    static
    bool arg_ready( obj_instance<vtkt_metadata> obj, serial_dep_tags & tags ) {
	vtkt_metadata * md = obj.get_version()->get_metadata();
	return md->chk_tag<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >( tags.tag );
    }
    static
    bool arg_ini_ready( const obj_instance<vtkt_metadata> obj ) {
	const vtkt_metadata * md = obj.get_version()->get_metadata();
	return md->has_none<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >();
    }
    static
    void arg_release( obj_instance<vtkt_metadata> obj ) {
	vtkt_metadata * md = obj.get_version()->get_metadata();
	md->del_writer();
    }
};

//----------------------------------------------------------------------
// Tags class for each dependency type
//----------------------------------------------------------------------
// Whole-function dependency tags
class function_tags : public function_tags_base { };

// Input dependency tags
class indep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    vtkt_metadata::tag_t tag;
};

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public serial_dep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public serial_dep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Commutative input/output dependency tags
#if OBJECT_COMMUTATIVITY
class cinoutdep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    vtkt_metadata::tag_t tag;
};
#endif

// Reduction dependency tags
#if OBJECT_REDUCTION
class reduction_tags : public reduction_tags_base<vtkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    vtkt_metadata::tag_t tag;
};
#endif

// Popdep (input) dependency tags - fully serialized with other pop and pushpop
class popdep_tags : public popdep_tags_base<vtkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    vtkt_metadata::tag_t tag;

public:
    popdep_tags( queue_version<vtkt_metadata> * parent )
	: popdep_tags_base( parent ) { }
};

// Pushpopdep (input/output) dependency tags - fully serialized with other
// pop and pushpop
class pushpopdep_tags : public pushpopdep_tags_base<vtkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    vtkt_metadata::tag_t tag;

public:
    pushpopdep_tags( queue_version<vtkt_metadata> * parent )
	: pushpopdep_tags_base( parent ) { }
};

// Pushdep (output) dependency tags
class pushdep_tags : public pushdep_tags_base<vtkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;

public:
    pushdep_tags( queue_version<vtkt_metadata> * parent )
	: pushdep_tags_base( parent ) { }
};


//----------------------------------------------------------------------
// Dependency handling traits to track task-object dependencies
//----------------------------------------------------------------------
// indep traits
template<>
struct dep_traits<vtkt_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj_ext,
			   typename indep<T>::dep_tags * tags ) {
	vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->tag = md->get_tag();
	md->add_reader();
    }
    template<typename T>
    static
    bool arg_ready( indep<T> & obj_int, typename indep<T>::dep_tags & tags ) {
	vtkt_metadata * md = obj_int.get_version()->get_metadata();
	return md->chk_tag<vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >( tags.tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const indep<T> & obj_ext ) {
	const vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	return md->has_none<vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, indep<T> & obj,
		      typename indep<T>::dep_tags & tags ) {
	obj.get_version()->get_metadata()->del_reader();
    }
};

// output dependency traits
template<>
struct dep_traits<vtkt_metadata, task_metadata, outdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, outdep<T> & obj_ext,
		    typename outdep<T>::dep_tags * tags ) {
	obj_version<vtkt_metadata> * v = obj_ext.get_version();
	assert( v->is_versionable() ); // enforced by applicators
	v->get_metadata()->add_writer();
    }
    template<typename T>
    static
    bool arg_ready( outdep<T> & obj, typename outdep<T>::dep_tags & tags ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicator
	return true;
    }
    template<typename T>
    static
    bool arg_ini_ready( const outdep<T> & obj ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicator
	return true;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, outdep<T> & obj,
		      typename outdep<T>::dep_tags & tags ) {
	serial_dep_traits::arg_release( obj );
    }
};

// inout dependency traits
template<>
struct dep_traits<vtkt_metadata, task_metadata, inoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, inoutdep<T> & obj_ext,
		    typename inoutdep<T>::dep_tags * tags ) {
	serial_dep_traits::arg_issue( fr, obj_ext, tags );
    }
    template<typename T>
    static
    bool arg_ready( inoutdep<T> & obj_ext,
		    typename inoutdep<T>::dep_tags & tags ) {
	return serial_dep_traits::arg_ready( obj_ext, tags );
    }
    template<typename T>
    static
    bool arg_ini_ready( const inoutdep<T> & obj_ext ) {
	return serial_dep_traits::arg_ini_ready( obj_ext );
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, inoutdep<T> & obj,
		      typename inoutdep<T>::dep_tags & tags ) {
	serial_dep_traits::arg_release( obj );
    }
};

// cinout dependency traits
#if OBJECT_COMMUTATIVITY
template<>
struct dep_traits<vtkt_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, cinoutdep<T> & obj_ext,
		    typename cinoutdep<T>::dep_tags * tags ) {
	vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->tag = md->get_tag();
	md->add_commutative();
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ready( cinoutdep<T> & obj,
		    typename cinoutdep<T>::dep_tags & tags ) {
	vtkt_metadata * md = obj.get_version()->get_metadata();
	if( md->chk_tag<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >( tags.tag ) )
	    return md->commutative_try_acquire();
	return false;
    }
    template<typename T>
    static
    bool arg_ini_ready( cinoutdep<T> obj_ext ) {
	vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	if( md->has_none<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_REDUCTION
	    | vtkt_metadata::v_reduct
#endif
	    >() )
	    return md->commutative_try_acquire();
	return false;
    }
    template<typename T>
    static
    void arg_ini_ready_undo( cinoutdep<T> obj_ext ) {
	obj_ext.get_version()->get_metadata()->commutative_release();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, cinoutdep<T> obj,
		      typename cinoutdep<T>::dep_tags & tags ) {
	vtkt_metadata * md = obj.get_version()->get_metadata();
	md->del_commutative();
	md->commutative_release();
    }
};
#endif

// reduction dependency traits
#if OBJECT_REDUCTION
template<>
struct dep_traits<vtkt_metadata, task_metadata, reduction> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, reduction<T> & obj_ext,
		    typename reduction<T>::dep_tags * tags ) {
	vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->tag = md->get_tag();
	md->add_reduction();
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ready( reduction<T> & obj_int,
		    typename reduction<T>::dep_tags & tags ) {
	vtkt_metadata * md = tags.ext_version->get_metadata();
	return md->chk_tag<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
	    >( tags.tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( reduction<T> obj_ext ) {
	const vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	return md->has_none<vtkt_metadata::v_readers
	    | vtkt_metadata::v_writers
#if OBJECT_COMMUTATIVITY
	    | vtkt_metadata::v_commut
#endif
	    >();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, reduction<T> obj_int,
		      typename reduction<T>::dep_tags & tags ) {
	vtkt_metadata * md = tags.ext_version->get_metadata();
	md->del_reduction();
    }
};
#endif

// popdep traits for queues
template<>
struct dep_traits<vtkt_metadata, task_metadata, popdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, popdep<T> & obj_int,
			   typename popdep<T>::dep_tags * tags ) {
	vtkt_metadata * md = obj_int.get_version()->get_metadata();
	tags->tag  = md->get_tag();
	md->add_reader();
    }
    template<typename T>
    static
    bool arg_ready( popdep<T> & obj_int, typename popdep<T>::dep_tags & tags ) {
	vtkt_metadata * md = obj_int.get_version()->
	    get_parent()->get_metadata();
	return md->chk_tag<vtkt_metadata::v_readers>( tags.tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const popdep<T> & obj_ext ) {
	const vtkt_metadata * md = obj_ext.get_version()->get_metadata();
	return md->has_none<vtkt_metadata::v_readers>();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, popdep<T> & obj_int,
		      typename popdep<T>::dep_tags & tags ) {
	obj_int.get_version()->get_metadata()->del_reader();
    }
};

// pushdep dependency traits for queues
template<>
struct dep_traits<vtkt_metadata, task_metadata, pushdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, pushdep<T> & obj_int,
		    typename pushdep<T>::dep_tags * tags ) {
    }
    template<typename T>
    static
    bool arg_ready( pushdep<T> & obj_int,
		    typename pushdep<T>::dep_tags & tags ) {
	return true;
    }
    template<typename T>
    static
    bool arg_ini_ready( const pushdep<T> & obj_ext ) {
	return true;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, pushdep<T> & obj_int,
		      typename pushdep<T>::dep_tags & tags ) {
    }
};


typedef vtkt_metadata obj_metadata;
typedef vtkt_metadata queue_metadata;
typedef vtkt_metadata token_metadata;

} // end of namespace obj

#endif // VTICKETS_H

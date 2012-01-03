// -*- c++ -*-
/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 Vassilis Papaefstathiou (papaef@ics.forth.gr)
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

/* gtickets.h
 * This file implements a ticket-based task graph where edges between tasks
 * are not explicitly maintained. Also, only a (number of) global queue(s) is
 * maintained to order all tasks.
 *
 * This system has a fundamental problem with wrap-around of the counters
 * because each object has a reference to its last-{writer,reader,...} counter
 * and we cannot keep track of all such references before allowing reuse.
 * On the other hand, if we allow aliasing on these counters, then we will
 * reduce parallelism but not make errors.
 */
#ifndef GTICKETS_H
#define GTICKETS_H

#include <cstdint>
#include <iostream>

#include "config.h"
#include "wf_frames.h"
#include "lfllist.h"
#include "lock.h"

namespace obj {

// The type holding the depth of an object in the task graph.
// The depth should not wrap around. If so, adopted implementations
// of max should be used in several dependency action traits.
typedef uint64_t depth_t;

class function_tags;

// Current implementation does not allow wrap-around of ctr_t
namespace gtickets {
struct rob_type {
    typedef int32_t ctr_t;
private:
    static const ctr_t size = 32768;
    volatile ctr_t head; // unsynchronized read->write dep from commit->issue
    ctr_t tail;
    cas_mutex mutex;
    // Entry completed[0] is the completed value for every unreferenced
    // object, i.e.\ true. The remaining size entries correspond to
    // variable completed values.
    bool completed[1+size];
public:
    rob_type() : head( 0 ), tail( 0 ) {
	completed[0] = true;
    }

    // Note: tail increment is covered by parent lock only if we use
    // hyperobjects intra-procedurally. If we use them "wrongly" then
    // the tail update must be atomic.
    ctr_t issue() {
	// Uuurgh - busy waiting - what else can we do in our scheduler?
	// Even the going deep path (work-first) comes here and requires
	// the allocation of a tag.
	while( tail - head >= size );

	mutex.lock();
	completed[1+(tail % size)] = false;
	ctr_t tag = tail++;
	mutex.unlock();
	return tag;
    }

    void commit( ctr_t tag ) {
	mutex.lock();
	completed[1+(tag % size)] = true;
	if( tag == head ) {
	    ctr_t max = tail - head;
	    ctr_t i;
	    for( i=0; i != max; ++i ) {
		ctr_t idx = ( head + i ) % size;
		if( !completed[1+idx] )
		    break;
	    }
	    head += i;
	}
	mutex.unlock();
    }

    bool is_ready( ctr_t tag ) const volatile {
	return tag < head; // completed[1+(tag % size)];
    }

    friend std::ostream &
    operator << ( std::ostream & os, const rob_type & f );

    friend class ::obj::function_tags;
};

extern rob_type rob; // TODO: make rob private to get_full() on parent frame

// Some debugging support
inline std::ostream &
operator << ( std::ostream & os, const rob_type & f ) {
    return os << '{' << f.head << ", " << f.tail << '}';
}

}

// ----------------------------------------------------------------------
// gtkt_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class gtkt_metadata {
public:
    typedef gtickets::rob_type::ctr_t ctr_t;
    typedef ctr_t tag_t;

private:
    ctr_t last_writer;
    ctr_t last_reader;
#if OBJECT_COMMUTATIVITY
    ctr_t last_commutative;
#endif
#if OBJECT_REDUCTION
    ctr_t last_reduction;
#endif
#if OBJECT_COMMUTATIVITY
    cas_mutex mutex;              // ensure exclusion on commutative operations
#endif
    depth_t depth;                // depth in task graph

public:
    gtkt_metadata() : last_writer( -1 ), last_reader( -1 ),
#if OBJECT_COMMUTATIVITY
		      last_commutative( -1 ),
#endif
#if OBJECT_REDUCTION
		      last_reduction( -1 ),
#endif
		      depth( 0 ) { }

    // External interface
    bool rename_is_active() const volatile {
        return rename_has_readers() || rename_has_writers();
    }
    bool rename_has_readers() const volatile { return has_readers(); }
    bool rename_has_writers() const volatile {
	return has_writers()
#if OBJECT_COMMUTATIVITY
	    | has_commutative()
#endif
#if OBJECT_REDUCTION
	    | has_reductions()
#endif
	    ;
    }

    // Track oustanding readers with a head and tail counter
    void set_last_reader( ctr_t w ) { last_reader = w; }
    bool is_reader_ready( ctr_t w ) const volatile { return gtickets::rob.is_ready( w ); }
    bool has_readers() const volatile {
	return !is_reader_ready( last_reader );
    }

    // Track oustanding writers with a head and tail counter
    void set_last_writer( ctr_t w ) { last_writer = w; }
    bool is_writer_ready( ctr_t w ) const volatile { return gtickets::rob.is_ready( w ); }
    bool has_writers() const volatile {
	return !is_writer_ready( last_writer );
    }

#if OBJECT_COMMUTATIVITY
    // Track oustanding commutatives with a head and tail counter
    void set_last_commutative( ctr_t w ) { last_commutative = w; }
    bool is_commutative_ready( ctr_t w ) const volatile {
	return gtickets::rob.is_ready( w );
    }
    bool has_commutative() const volatile {
	return !is_commutative_ready( last_commutative );
    }

    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return mutex.try_lock(); }
    void commutative_release() { mutex.unlock(); }
#endif

#if OBJECT_REDUCTION
    // Track oustanding reductions with a head and tail counter
    void set_last_reduction( ctr_t w ) { last_reduction = w; }
    bool is_reduction_ready( ctr_t w ) const volatile {
	return gtickets::rob.is_ready( w );
    }
    bool has_reductions() const volatile {
	return !is_reduction_ready( last_reduction );
    }
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

    friend std::ostream & operator << ( std::ostream & os, const gtkt_metadata & md );
};

// Some debugging support
inline std::ostream & operator << ( std::ostream & os, const gtkt_metadata & md ) {
    os << "gticket_md={last_rd=" << md.last_reader
       << ", last_wr=" << md.last_writer
#if OBJECT_COMMUTATIVITY
       << ", last_c=" << md.last_commutative
#endif
#if OBJECT_REDUCTION
       << ", last_r=" << md.last_reduction
#endif
       << '}';
    return os;
}

// ----------------------------------------------------------------------
// Whole-function dependency tags
// ----------------------------------------------------------------------
class function_tags : public function_tags_base {
public:
    gtkt_metadata::tag_t get_tag() const { return tag; }

private:
    gtkt_metadata::tag_t tag; // The current task sequence number
    // The tag is the max of this type of tags across all args
    gtkt_metadata::tag_t rd_tag;
    gtkt_metadata::tag_t wr_tag;
#if OBJECT_COMMUTATIVITY
    gtkt_metadata::tag_t c_tag;
#define NUM_OC 1
#else
#define NUM_OC 0
#endif
#if OBJECT_REDUCTION
    gtkt_metadata::tag_t r_tag;
#define NUM_OR 1
#else
#define NUM_OR 0
#endif

    pad_multiple<16, sizeof(gtkt_metadata::tag_t)*(3+NUM_OC+NUM_OR)> padding;
#undef NUM_OC
#undef NUM_OR

public:
    function_tags() : rd_tag( 0 ), wr_tag( 0 )
#if OBJECT_COMMUTATIVITY
		    , c_tag( 0 )
#endif
#if OBJECT_REDUCTION
		    , r_tag( 0 )
#endif
	{
	    static_assert( (sizeof(*this) % 16) == 0,
                           "Padding of gtickets::function_tags failed" );
	}

    gtkt_metadata::tag_t issue() { return tag = gtickets::rob.issue(); }
    void commit() const { gtickets::rob.commit( tag ); }

    void wait_reader() { rd_tag = std::max( rd_tag, tag ); }
    bool is_reader_ready() const { return rd_tag <= gtickets::rob.head; }

    void wait_writer() { wr_tag = std::max( wr_tag, tag ); }
    bool is_writer_ready() const { return wr_tag <= gtickets::rob.head; }

#if OBJECT_COMMUTATIVITY
    void wait_commutative() { c_tag = std::max( c_tag, tag ); }
    bool is_commutative_ready() const { return c_tag <= gtickets::rob.head; }
#endif

#if OBJECT_REDUCTION
    void wait_reduction() { r_tag = std::max( r_tag, tag ); }
    bool is_reduction_ready() const { return r_tag <= gtickets::rob.head; }
#endif
};

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

#if STORED_ANNOTATIONS
// A "update depth" function to adjust the depth of the objects in the taskgraph
template<typename Task, typename MetaData, typename... Tn>
static inline void arg_update_depth_fn( Task * fr, task_data_t & td ) {
    update_depth_functor<MetaData, Task> dfn( fr );
    arg_apply_stored_fn<update_depth_functor<MetaData, Task>,Tn...>( dfn, td );
}
#else
// A "update depth" function to adjust the depth of the objects in the taskgraph
template<typename Task, typename MetaData, typename... Tn>
static inline void arg_update_depth_fn( Task * fr, task_data_t & td ) {
    update_depth_functor<MetaData, Task> dfn( fr );
    arg_apply_fn<update_depth_functor<MetaData, Task>,Tn...>(
	dfn, td.get_args_ptr(), td.get_tags_ptr() );
}
#endif

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
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( get_task_data().get_tags_ptr() );
	new (fn_tags) function_tags();
	fn_tags->issue();

#if STORED_ANNOTATIONS
	arg_update_depth_fn<task_metadata, gtkt_metadata>( this, get_task_data() );
#else
	arg_update_depth_fn<task_metadata, gtkt_metadata, Tn...>(
	    this, get_task_data() );
#endif
    }
    void stop_registration( bool wakeup = false ) { }

    void start_deregistration() {
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( get_task_data().get_tags_ptr() );
	fn_tags->commit();
    }
    void stop_deregistration() { }
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
// Checking readiness of pending_metadata
// ----------------------------------------------------------------------
// A "ready function" to check readiness, based on per-function tags
template<typename MetaData, typename Task>
static inline bool arg_ready_fn( const task_data_t & task_data_p ) {
    function_tags * fn_tags
	= get_fn_tags<function_tags>( task_data_p.get_tags_ptr() );
    return fn_tags->is_reader_ready()
#if OBJECT_COMMUTATIVITY
	& fn_tags->is_commutative_ready()
#endif
#if OBJECT_REDUCTION
	& fn_tags->is_reduction_ready()
#endif
	& fn_tags->is_writer_ready();
}

// ----------------------------------------------------------------------
// pending_metadata: task graph metadata per pending frame
// ----------------------------------------------------------------------
class pending_metadata : public task_metadata, private link_metadata {
protected:
    pending_metadata() { }

public:
    template<typename... Tn>
    void create( full_metadata * ff ) {
	task_metadata::create<Tn...>( ff );
    }

    bool is_ready() const {
	return arg_ready_fn<gtkt_metadata, task_metadata>( get_task_data() );
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

protected:
    full_metadata() : pending( 0 ) { }
    ~full_metadata() { delete pending; }

public:
    void push_pending( pending_metadata * frame ) {
	allocate_pending();
	pending->prepend( frame );
    }

    pending_metadata *
    get_ready_task() {
	return pending ? pending->get_ready() : 0;
    }

    pending_metadata *
    get_ready_task_after( task_metadata * prev ) {
	depth_t prev_depth = prev->get_depth();
	return pending ? pending->get_ready( prev_depth ) : 0;
    }

#ifdef UBENCH_HOOKS
    void reset() {
	if( pending )
	    pending->reset();
    }
#endif

private:
    void allocate_pending() {
	if( !pending )
	    pending = new hashed_list<pending_metadata>;
    }

};

// ----------------------------------------------------------------------
// Generic fully-serial dependency handling traits
// ----------------------------------------------------------------------
// A fully serialized version
class serial_dep_tags { };

struct serial_dep_traits {
    static
    void arg_issue( task_metadata * fr,
		    obj_instance<gtkt_metadata> & obj,
		    serial_dep_tags * tags ) {
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( fr->get_tags_ptr() );
	gtkt_metadata * md = obj.get_version()->get_metadata();
	fn_tags->wait_reader();
	fn_tags->wait_writer();
#if OBJECT_COMMUTATIVITY
	fn_tags->wait_commutative();
#endif
#if OBJECT_REDUCTION
	fn_tags->wait_reduction();
#endif
	md->set_last_writer( fn_tags->get_tag() );
	md->update_depth( fr->get_depth() );
    }
    static
    bool arg_ini_ready( const obj_instance<gtkt_metadata> obj ) {
	const gtkt_metadata * md = obj.get_version()->get_metadata();
	return !md->has_readers()
#if OBJECT_COMMUTATIVITY
	    & !md->has_commutative()
#endif
#if OBJECT_REDUCTION
	    & !md->has_reductions()
#endif
	    & !md->has_writers();
    }
};

//----------------------------------------------------------------------
// Tags class for each dependency type
//----------------------------------------------------------------------
// Input dependency tags
class indep_tags : public indep_tags_base {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public outdep_tags_base, public serial_dep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public inoutdep_tags_base, public serial_dep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Commutative input/output dependency tags
#if OBJECT_COMMUTATIVITY
class cinoutdep_tags : public cinoutdep_tags_base {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};
#endif

// Reduction dependency tags
#if OBJECT_REDUCTION
class reduction_tags : public reduction_tags_base<gtkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};
#endif

//----------------------------------------------------------------------
// Dependency handling traits to track task-object dependencies
//----------------------------------------------------------------------
// indep traits
template<>
struct dep_traits<gtkt_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj_ext,
			   typename indep<T>::dep_tags * tags ) {
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( fr->get_task_data().get_tags_ptr() );
	gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	fn_tags->wait_writer();
#if OBJECT_COMMUTATIVITY
	fn_tags->wait_commutative();
#endif
#if OBJECT_REDUCTION
	fn_tags->wait_reduction();
#endif
	md->set_last_reader( fn_tags->get_tag() );
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ini_ready( const indep<T> & obj_ext ) {
	const gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_writers()
#if OBJECT_COMMUTATIVITY
	    & !md->has_commutative()
#endif
#if OBJECT_REDUCTION
	    & !md->has_reductions()
#endif
	    ;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, indep<T> & obj,
		      typename indep<T>::dep_tags & tags ) { }
};

// output dependency traits
template<>
struct dep_traits<gtkt_metadata, task_metadata, outdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, outdep<T> & obj_ext,
		    typename outdep<T>::dep_tags * tags ) {
	assert( obj_ext.get_version()->is_versionable() ); // enforced by applicators
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( fr->get_task_data().get_tags_ptr() );
	gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	md->set_last_writer( fn_tags->get_tag() );
    }
    template<typename T>
    static
    bool arg_ini_ready( const outdep<T> & obj ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicators
	return true;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, outdep<T> & obj,
		      typename outdep<T>::dep_tags & tags ) { }
};

// inout dependency traits
template<>
struct dep_traits<gtkt_metadata, task_metadata, inoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, inoutdep<T> & obj_ext,
		    typename inoutdep<T>::dep_tags * tags ) {
	serial_dep_traits::arg_issue( fr, obj_ext, tags );
    }
    template<typename T>
    static
    bool arg_ini_ready( const inoutdep<T> & obj_ext ) {
	return serial_dep_traits::arg_ini_ready( obj_ext );
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, inoutdep<T> & obj,
		      typename inoutdep<T>::dep_tags & tags ) { }
};

// cinout dependency traits
#if OBJECT_COMMUTATIVITY
template<>
struct dep_traits<gtkt_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, cinoutdep<T> & obj_ext,
		    typename cinoutdep<T>::dep_tags * tags ) {
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( fr->get_task_data().get_tags_ptr() );
	gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	fn_tags->wait_reader();
	fn_tags->wait_writer();
#if OBJECT_REDUCTION
	fn_tags->wait_reduction();
#endif
	md->set_last_commutative( fn_tags->get_tag() );
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ini_ready( cinoutdep<T> obj_ext ) {
	gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	if( !md->has_readers() & !md->has_writers()
#if OBJECT_REDUCTION
	    & !md->has_reductions()
#endif
	    )
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
	gtkt_metadata * md = obj.get_version()->get_metadata();
	md->commutative_release();
    }
};
#endif

// reduction dependency traits
#if OBJECT_REDUCTION
template<>
struct dep_traits<gtkt_metadata, task_metadata, reduction> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, reduction<T> & obj_ext,
		    typename reduction<T>::dep_tags * tags ) {
	function_tags * fn_tags
	    = get_fn_tags<function_tags>( fr->get_task_data().get_tags_ptr() );
	gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	fn_tags->wait_reader();
	fn_tags->wait_writer();
#if OBJECT_COMMUTATIVITY
	fn_tags->wait_commutative();
#endif
	md->set_last_reduction( fn_tags->get_tag() );
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ini_ready( reduction<T> obj_ext ) {
	const gtkt_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_readers() & !md->has_writers()
#if OBJECT_COMMUTATIVITY
	    & !md->has_commutative()
#endif
	    ;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, reduction<T> obj_int,
		      typename reduction<T>::dep_tags & tags ) { }
};
#endif

typedef gtkt_metadata obj_metadata;

} // end of namespace obj

#endif // TICKETS_H

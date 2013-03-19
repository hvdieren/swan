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

/* tickets.h
 * This file implements a ticket-based task graph where edges between tasks
 * are not explicitly maintained.
 *
 * @note
 *   The implementation differs slightly from that described in the PACT'11
 *   paper as both readers and writers are incremented for an outdep. The
 *   reasoning to this is that is cheaper to increment a counter during issue
 *   and release than it is to perform additional control flow during release
 *   to avoid those counter increments.
 *   Furthermore, we propose to remove the increment of the readers counter
 *   for an in/out dep because it is redundant.
 */
#ifndef TICKETS_H
#define TICKETS_H

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

// ----------------------------------------------------------------------
// tkt_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class tkt_metadata {
public:
    struct fifo_like {
	typedef uint32_t ctr_t;
	ctr_t head;
	ctr_t tail;
    public:
	fifo_like() : head( 0 ), tail( 0 ) {
	    assert( (intptr_t(&head) & (sizeof(head)-1)) == 0
		    && "Alignment of head field not respected" );
	    assert( (intptr_t(&tail) & (sizeof(tail)-1)) == 0
		    && "Alignment of tail field not respected" );
	}

	ctr_t adv_head() { return __sync_fetch_and_add( &head, 1 ); }
	// Note: tail increment is covered by parent lock only if we use
	// hyperobjects intra-procedurally. If we use them "wrongly" then
	// the tail update must be atomic.
	// ctr_t adv_tail() { return tail++; } // covered by parent lock
	ctr_t adv_tail() { return __sync_fetch_and_add( &tail, 1 ); }
	bool empty() const volatile { return head == tail; }
	bool chk_tag( ctr_t tag ) const volatile { return head == tag; }
	ctr_t get_tag() const { return tail; }

	friend std::ostream &
	operator << ( std::ostream & os, const fifo_like & f );
    };
    typedef fifo_like::ctr_t tag_t;

private:
    fifo_like writers;            // head and tail counter for writers
    fifo_like readers;            // head and tail counter for readers
#if OBJECT_COMMUTATIVITY
    fifo_like commutative;        // head and tail counter for commutative IO
    cas_mutex mutex;              // ensure exclusion on commutative operations
#endif
#if OBJECT_REDUCTION
    fifo_like reductions;         // head and tail counter for readers
#endif
    depth_t depth;                // depth in task graph

public:
    tkt_metadata() : depth( 0 ) { }
    ~tkt_metadata() {
	assert( readers.empty()
		&& "Must have zero readers when destructing obj_version" );
	assert( writers.empty()
		&& "Must have zero writers when destructing obj_version" );
    }

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
    void add_reader() { readers.adv_tail(); }
    void del_reader() { readers.adv_head(); }
    bool chk_reader_tag( tag_t w ) const volatile { return readers.chk_tag(w); }
    bool has_readers() const volatile { return !readers.empty(); }
    tag_t get_reader_tag() const { return readers.get_tag(); }

    // Track outstanding writers with a head and tail counter, both adding up.
    // This organization allows us to easily implement inout dependencies where
    // the object with an inout dependency is never renamed. Each successive
    // inout dependency gets a different value of the writers variable, a "tag"
    // that must be matched for waking up pending children.
    void add_writer() { writers.adv_tail(); } 
    void del_writer() { writers.adv_head(); }
    bool chk_writer_tag( tag_t w ) const volatile { return writers.chk_tag(w); }
    bool has_writers() const volatile { return !writers.empty(); }
    tag_t get_writer_tag() const { return writers.get_tag(); }

#if OBJECT_COMMUTATIVITY
    // Track commutative IO
    void add_commutative() { commutative.adv_tail(); }
    void del_commutative() { commutative.adv_head(); }
    bool chk_commutative_tag( tag_t w ) const volatile {
	return commutative.chk_tag(w);
    }
    bool has_commutative() const volatile { return !commutative.empty(); }
    tag_t get_commutative_tag() const { return commutative.get_tag(); }

    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return mutex.try_lock(); }
    void commutative_release() { mutex.unlock(); }
#endif

#if OBJECT_REDUCTION
    // Track reductions
    void add_reduction() { reductions.adv_tail(); }
    void del_reduction() { reductions.adv_head(); }
    bool chk_reduction_tag( tag_t w ) const volatile {
	return reductions.chk_tag(w);
    }
    bool has_reductions() const volatile { return !reductions.empty(); }
    tag_t get_reduction_tag() const { return reductions.get_tag(); }
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

    friend std::ostream & operator << ( std::ostream & os, const tkt_metadata & md );
};

class token_metadata {
public:
    struct fifo_like {
	typedef uint32_t ctr_t;
	ctr_t head;
	ctr_t tail;
    public:
	fifo_like() : head( 0 ), tail( 0 ) {
	    assert( (intptr_t(&head) & (sizeof(head)-1)) == 0
		    && "Alignment of head field not respected" );
	    assert( (intptr_t(&tail) & (sizeof(tail)-1)) == 0
		    && "Alignment of tail field not respected" );
	}

	ctr_t adv_head() { return __sync_fetch_and_add( &head, 1 ); }
	// Note: tail increment is covered by parent lock only if we use
	// hyperobjects intra-procedurally. If we use them "wrongly" then
	// the tail update must be atomic.
	// ctr_t adv_tail() { return tail++; } // covered by parent lock
	ctr_t adv_tail() { return __sync_fetch_and_add( &tail, 1 ); }
	bool empty() const volatile { return head == tail; }
	bool chk_tag( ctr_t tag ) const volatile { return head == tag; }
	ctr_t get_tag() const { return tail; }

	friend std::ostream &
	operator << ( std::ostream & os, const fifo_like & f );
    };
    typedef fifo_like::ctr_t tag_t;

private:
    fifo_like writers;            // head and tail counter for writers
    fifo_like readers;            // head and tail counter for readers
    depth_t depth;                // depth in task graph

public:
    token_metadata() : depth( 0 ) { }
    ~token_metadata() {
	assert( readers.empty()
		&& "Must have zero readers when destructing obj_version" );
	assert( writers.empty()
		&& "Must have zero writers when destructing obj_version" );
    }

    // External interface - never do renaming
    bool rename_is_active() const volatile { return true; }
    bool rename_has_readers() const volatile { return true; }
    bool rename_has_writers() const volatile { return true; }

    // Track oustanding readers with a head and tail counter
    void add_reader() { readers.adv_tail(); }
    void del_reader() { readers.adv_head(); }
    bool chk_reader_tag( tag_t w ) const volatile { return readers.chk_tag(w); }
    bool has_readers() const volatile { return !readers.empty(); }
    tag_t get_reader_tag() const { return readers.get_tag(); }

    // Track outstanding writers with a head and tail counter, both adding up.
    // This organization allows us to easily implement inout dependencies where
    // the object with an inout dependency is never renamed. Each successive
    // inout dependency gets a different value of the writers variable, a "tag"
    // that must be matched for waking up pending children.
    void add_writer() { writers.adv_tail(); } 
    void del_writer() { writers.adv_head(); }
    bool chk_writer_tag( tag_t w ) const volatile { return writers.chk_tag(w); }
    bool has_writers() const volatile { return !writers.empty(); }
    tag_t get_writer_tag() const { return writers.get_tag(); }

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
};

// Some debugging support
inline std::ostream &
operator << ( std::ostream & os, const tkt_metadata::fifo_like & f ) {
    return os << '{' << f.head << ", " << f.tail << '}';
}

inline std::ostream & operator << ( std::ostream & os, const tkt_metadata & md ) {
    os << "ticket_md={readers=" << md.readers
       << ", writers=" << md.writers
#if OBJECT_COMMUTATIVITY
       << ", commutative=" << md.commutative
#endif
#if OBJECT_REDUCTION
       << ", reductions=" << md.reductions
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
#if STORED_ANNOTATIONS
	arg_update_depth_fn<task_metadata, tkt_metadata>( this, get_task_data() );
#else
	arg_update_depth_fn<task_metadata, tkt_metadata, Tn...>(
	    this, get_task_data() );
#endif
    }
    void stop_registration( bool wakeup = false ) { }

    void start_deregistration() { }
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

// ----------------------------------------------------------------------
// pending_metadata: task graph metadata per pending frame
// ----------------------------------------------------------------------
class pending_metadata : public task_metadata, private link_metadata {
#if !STORED_ANNOTATIONS
    typedef bool (*ready_fn_t)( const task_data_t & );

    ready_fn_t ready_fn;
#endif

protected:
    pending_metadata()
#if !STORED_ANNOTATIONS
	: ready_fn( 0 )
#endif
	{ }

public:
    template<typename... Tn>
    void create( full_metadata * ff ) {
#if !STORED_ANNOTATIONS
	ready_fn = &arg_ready_fn<tkt_metadata, task_metadata, Tn...>;
#endif
	task_metadata::create<Tn...>( ff );
    }

    bool is_ready() const {
#if STORED_ANNOTATIONS
	return arg_ready_fn<tkt_metadata, task_metadata>( get_task_data() );
#else
	return ready_fn && (*ready_fn)( get_task_data() );
#endif
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
	if( !pending ) {
	    // errs() << "ALLOC PENDING\n";
	    pending = new hashed_list<pending_metadata>;
	    }
    }

};

// ----------------------------------------------------------------------
// Generic fully-serial dependency handling traits
// ----------------------------------------------------------------------

// A fully serialized version
class serial_dep_tags {
protected:
    friend class serial_dep_traits;
    tkt_metadata::tag_t rd_tag;
    tkt_metadata::tag_t wr_tag;
#if OBJECT_COMMUTATIVITY
    tkt_metadata::tag_t c_tag;
#endif
#if OBJECT_REDUCTION
    tkt_metadata::tag_t r_tag;
#endif
};

struct serial_dep_traits {
    static
    void arg_issue( task_metadata * fr,
		    obj_instance<tkt_metadata> & obj,
		    serial_dep_tags * tags ) {
	tkt_metadata * md = obj.get_version()->get_metadata();
	tags->rd_tag  = md->get_reader_tag();
	tags->wr_tag  = md->get_writer_tag();
#if OBJECT_COMMUTATIVITY
	tags->c_tag  = md->get_commutative_tag();
#endif
#if OBJECT_REDUCTION
	tags->r_tag  = md->get_reduction_tag();
#endif
	md->add_writer();
	md->update_depth( fr->get_depth() );
    }
    static
    bool arg_ready( obj_instance<tkt_metadata> obj, serial_dep_tags & tags ) {
	tkt_metadata * md = obj.get_version()->get_metadata();
	return md->chk_reader_tag( tags.rd_tag )
#if OBJECT_COMMUTATIVITY
	    & md->chk_commutative_tag( tags.c_tag )
#endif
#if OBJECT_REDUCTION
	    & md->chk_reduction_tag( tags.r_tag )
#endif
	    & md->chk_writer_tag( tags.wr_tag );
    }
    static
    bool arg_ini_ready( const obj_instance<tkt_metadata> obj ) {
	const tkt_metadata * md = obj.get_version()->get_metadata();
	return !md->has_readers()
#if OBJECT_COMMUTATIVITY
	    & !md->has_commutative()
#endif
#if OBJECT_REDUCTION
	    & !md->has_reductions()
#endif
	    & !md->has_writers();
    }
    static
    void arg_release( obj_instance<tkt_metadata> obj ) {
	tkt_metadata * md = obj.get_version()->get_metadata();
	md->del_writer();
    }
};

//----------------------------------------------------------------------
// Tags class for each dependency type
//----------------------------------------------------------------------
// Whole-function dependency tags
class function_tags : public function_tags_base { };

// Input dependency tags
class indep_tags : public indep_tags_base {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    tkt_metadata::tag_t wr_tag;
#if OBJECT_COMMUTATIVITY
    tkt_metadata::tag_t c_tag;
#endif
#if OBJECT_REDUCTION
    tkt_metadata::tag_t r_tag;
#endif
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
    tkt_metadata::tag_t wr_tag;
    tkt_metadata::tag_t rd_tag;
#if OBJECT_REDUCTION
    tkt_metadata::tag_t r_tag;
#endif
};
#endif

// Reduction dependency tags
#if OBJECT_REDUCTION
class reduction_tags : public reduction_tags_base<tkt_metadata> {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    tkt_metadata::tag_t wr_tag;
    tkt_metadata::tag_t rd_tag;
#if OBJECT_COMMUTATIVITY
    tkt_metadata::tag_t c_tag;
#endif
};
#endif

//----------------------------------------------------------------------
// Dependency handling traits to track task-object dependencies
//----------------------------------------------------------------------
// indep traits for objects
template<>
struct dep_traits<tkt_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj_ext,
			   typename indep<T>::dep_tags * tags ) {
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->wr_tag  = md->get_writer_tag();
#if OBJECT_COMMUTATIVITY
	tags->c_tag  = md->get_commutative_tag();
#endif
#if OBJECT_REDUCTION
	tags->r_tag  = md->get_reduction_tag();
#endif
	md->add_reader();
    }
    template<typename T>
    static
    bool arg_ready( indep<T> & obj_int, typename indep<T>::dep_tags & tags ) {
	tkt_metadata * md = obj_int.get_version()->get_metadata();
	return md->chk_writer_tag( tags.wr_tag )
#if OBJECT_COMMUTATIVITY
	    & md->chk_commutative_tag( tags.c_tag )
#endif
#if OBJECT_REDUCTION
	    & md->chk_reduction_tag( tags.r_tag )
#endif
	    ;
    }
    template<typename T>
    static
    bool arg_ini_ready( const indep<T> & obj_ext ) {
	const tkt_metadata * md = obj_ext.get_version()->get_metadata();
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
		      typename indep<T>::dep_tags & tags ) {
	obj.get_version()->get_metadata()->del_reader();
    }
};

// indep traits for tokens
template<>
struct dep_traits<token_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj_ext,
			   typename indep<T>::dep_tags * tags ) {
	token_metadata * md = obj_ext.get_version()->get_metadata();
	tags->wr_tag  = md->get_writer_tag();
	md->add_reader();
    }
    template<typename T>
    static
    bool arg_ready( indep<T> & obj_int, typename indep<T>::dep_tags & tags ) {
	token_metadata * md = obj_int.get_version()->get_metadata();
	return md->chk_writer_tag( tags.wr_tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const indep<T> & obj_ext ) {
	const token_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_writers();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, indep<T> & obj,
		      typename indep<T>::dep_tags & tags ) {
	obj.get_version()->get_metadata()->del_reader();
    }
};


// output dependency traits for objects
template<>
struct dep_traits<tkt_metadata, task_metadata, outdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, outdep<T> & obj_ext,
		    typename outdep<T>::dep_tags * tags ) {
	assert( obj_ext.get_version()->is_versionable() ); // enforced by applicators
	obj_ext.get_version()->get_metadata()->add_writer();
    }
    template<typename T>
    static
    bool arg_ready( outdep<T> & obj, typename outdep<T>::dep_tags & tags ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicators
	return true;
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
		      typename outdep<T>::dep_tags & tags ) {
	serial_dep_traits::arg_release( obj );
    }
};

// inout dependency traits for objects
template<>
struct dep_traits<tkt_metadata, task_metadata, inoutdep> {
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

// inout dependency traits for tokens
template<>
struct dep_traits<token_metadata, task_metadata, inoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, inoutdep<T> & obj_ext,
		    typename inoutdep<T>::dep_tags * tags ) {
	token_metadata * md = obj_ext.get_version()->get_metadata();
	tags->rd_tag  = md->get_reader_tag();
	tags->wr_tag  = md->get_writer_tag();
	md->add_writer();
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ready( inoutdep<T> & obj_ext,
		    typename inoutdep<T>::dep_tags & tags ) {
	token_metadata * md = obj_ext.get_version()->get_metadata();
	return md->chk_reader_tag( tags.rd_tag )
	    & md->chk_writer_tag( tags.wr_tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const inoutdep<T> & obj_ext ) {
	const token_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_readers() & !md->has_writers();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, inoutdep<T> & obj,
		      typename inoutdep<T>::dep_tags & tags ) {
	token_metadata * md = obj.get_version()->get_metadata();
	md->del_writer();
    }
};

// cinout dependency traits
#if OBJECT_COMMUTATIVITY
template<>
struct dep_traits<tkt_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, cinoutdep<T> & obj_ext,
		    typename cinoutdep<T>::dep_tags * tags ) {
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->rd_tag = md->get_reader_tag();
	tags->wr_tag = md->get_writer_tag();
#if OBJECT_REDUCTION
	tags->r_tag  = md->get_reduction_tag();
#endif
	md->add_commutative();
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ready( cinoutdep<T> & obj,
		    typename cinoutdep<T>::dep_tags & tags ) {
	tkt_metadata * md = obj.get_version()->get_metadata();
	if( md->chk_reader_tag( tags.rd_tag )
	    & md->chk_writer_tag( tags.wr_tag )
#if OBJECT_REDUCTION
	    & md->chk_reduction_tag( tags.r_tag )
#endif
	    )
	    return md->commutative_try_acquire();
	return false;
    }
    template<typename T>
    static
    bool arg_ini_ready( cinoutdep<T> obj_ext ) {
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
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
	tkt_metadata * md = obj.get_version()->get_metadata();
	md->del_commutative();
	md->commutative_release();
    }
};
#endif

// reduction dependency traits
#if OBJECT_REDUCTION
template<>
struct dep_traits<tkt_metadata, task_metadata, reduction> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, reduction<T> & obj_ext,
		    typename reduction<T>::dep_tags * tags ) {
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->rd_tag = md->get_reader_tag();
#if OBJECT_COMMUTATIVITY
	tags->c_tag  = md->get_commutative_tag();
#endif
	tags->wr_tag = md->get_writer_tag();
	md->add_reduction();
	md->update_depth( fr->get_depth() );
    }
    template<typename T>
    static
    bool arg_ready( reduction<T> & obj_int,
		    typename reduction<T>::dep_tags & tags ) {
	tkt_metadata * md = tags.ext_version->get_metadata();
	return md->chk_reader_tag( tags.rd_tag )
	    & md->chk_writer_tag( tags.wr_tag )
#if OBJECT_COMMUTATIVITY
	    & md->chk_commutative_tag( tags.c_tag )
#endif
	    ;
    }
    template<typename T>
    static
    bool arg_ini_ready( reduction<T> obj_ext ) {
	const tkt_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_readers() & !md->has_writers()
#if OBJECT_COMMUTATIVITY
	    & !md->has_commutative()
#endif
	    ;
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, reduction<T> obj_int,
		      typename reduction<T>::dep_tags & tags ) {
	tkt_metadata * md = tags.ext_version->get_metadata();
	md->del_reduction();
    }
};
#endif

// popdep traits for queues
template<>
struct dep_traits<tkt_metadata, task_metadata, popdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, popdep<T> & obj_int,
			   typename popdep<T>::dep_tags * tags ) {
	popdep<T> obj_ext
	    = popdep<T>::create( obj_int.get_version()->get_parent() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->rd_tag  = md->get_reader_tag();
	md->add_reader();
    }
    template<typename T>
    static
    bool arg_ready( popdep<T> & obj_int, typename popdep<T>::dep_tags & tags ) {
	popdep<T> obj_ext
	    = popdep<T>::create( obj_int.get_version()->get_parent() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	return md->chk_reader_tag( tags.rd_tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const popdep<T> & obj_ext ) {
	const tkt_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_readers();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, popdep<T> & obj_int,
		      typename popdep<T>::dep_tags & tags ) {
	popdep<T> obj_ext
	    = popdep<T>::create( obj_int.get_version()->get_parent() );
	obj_ext.get_version()->get_metadata()->del_reader();
    }
};

// pushdep dependency traits for queues
template<>
struct dep_traits<tkt_metadata, task_metadata, pushdep> {
    template<typename T>
    static
    void arg_issue( task_metadata * fr, pushdep<T> & obj_int,
		    typename pushdep<T>::dep_tags * tags ) {
/*
	pushdep<T> obj_ext
	    = pushdep<T>::create( obj_int.get_version()->get_parent() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	md->add_writer();
*/
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
/*
	pushdep<T> obj_ext
	    = pushdep<T>::create( obj_int.get_version()->get_parent() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	md->del_writer();
*/
    }
};

// prefixdep traits for queues
template<>
struct dep_traits<tkt_metadata, task_metadata, prefixdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, prefixdep<T> & obj_int,
			   typename prefixdep<T>::dep_tags * tags ) {
	prefixdep<T> obj_ext
	    = prefixdep<T>::create( obj_int.get_version()->get_parent(),
				    obj_int.get_length() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	tags->rd_tag  = md->get_reader_tag();
    }
    template<typename T>
    static
    bool arg_ready( prefixdep<T> & obj_int, typename prefixdep<T>::dep_tags & tags ) {
	prefixdep<T> obj_ext
	    = prefixdep<T>::create( obj_int.get_version()->get_parent(),
				    obj_int.get_length() );
	tkt_metadata * md = obj_ext.get_version()->get_metadata();
	return md->chk_reader_tag( tags.rd_tag );
    }
    template<typename T>
    static
    bool arg_ini_ready( const prefixdep<T> & obj_ext ) {
	const tkt_metadata * md = obj_ext.get_version()->get_metadata();
	return !md->has_readers();
    }
    template<typename T>
    static
    void arg_release( task_metadata * fr, prefixdep<T> & obj_int,
		      typename prefixdep<T>::dep_tags & tags ) { }
};

typedef tkt_metadata obj_metadata;

} // end of namespace obj

#endif // TICKETS_H

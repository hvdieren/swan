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
/* ctaskgraph.h
 * This file implements an explicit task graph where edges between tasks
 * are explicitly maintained. It supports commutativity and reductions, but
 * is perhaps slightly less efficient on out and inout compared to taskgraph.h
 */
#ifndef CTASKGRAPH_H
#define CTASKGRAPH_H

#include <cstdint>
#include <iostream>
#include <list>

#include "swan/wf_frames.h"
#include "swan/lock.h"
#include "swan/queue/taskgraph.h"
#include "swan/functor/acquire.h"

#if OBJECT_TASKGRAPH == 9
#include "swan/lfllist.h"
#endif

// ERASE_NULL: When erasing a reader from the readers list, don't unlink,
// but set pointer to null.
#define ERASE_NULL 0

namespace obj {

// ----------------------------------------------------------------------
// Auxiliary type
// ----------------------------------------------------------------------
enum group_t {
    g_read,
    g_write,
#if OBJECT_COMMUTATIVITY
    g_commut,
#endif
#if OBJECT_REDUCTION
    g_reduct,
#endif
    g_NUM
};

inline
std::ostream &
operator << ( std::ostream & os, group_t g ) {
    switch( g ) {
    case g_read: return os << "read";
    case g_write: return os << "write";
#if OBJECT_COMMUTATIVITY
    case g_commut: return os << "commut";
#endif
#if OBJECT_REDUCTION
    case g_reduct: return os << "reduct";
#endif
    default:
    case g_NUM: return os << "<NUM>";
    }
}

// ----------------------------------------------------------------------
// tags to link task on generation's task list
// ----------------------------------------------------------------------
#if OBJECT_TASKGRAPH == 5
class gen_tags { };
#else //  OBJECT_TASKGRAPH == 9
class task_metadata;

class gen_tags {
    task_metadata * task;
    gen_tags * prev, * next;

    friend class dl_list_traits<gen_tags>;
    friend class ctg_metadata;
};

} // end namespace obj because the traits class must be in global namespace

template<>
struct dl_list_traits<obj::gen_tags> {
    typedef obj::gen_tags T;
    typedef obj::task_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    static void set_prev( T * elm, T * prev ) { elm->prev = prev; }
    static T * get_prev( T const * elm ) { return elm->prev; }
    static void set_next( T * elm, T * next ) { elm->next = next; }
    static T * get_next( T const * elm ) { return elm->next; }

    static ValueType * get_value( T const * elm ) { return elm->task; }
};

namespace obj { // reopen
#endif

// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
class pending_metadata;

class taskgraph {
    typedef cas_mutex mutex_t;

private:
    std::list<pending_metadata *> ready_list; // implement using prev/next in link_md?
    mutable mutex_t mutex;

public:
    ~taskgraph() {
	assert( ready_list.empty() && "Pending tasks at destruction time" );
    }

    inline pending_metadata * get_ready_task();

    void add_ready_task( pending_metadata * fr ) {
	// errs() << "add_ready_task to TG " << this << ": " << fr << "\n";
	lock();
	ready_list.push_back( fr );
	unlock();
    }

    bool empty() const {
	lock();
	bool ret = ready_list.empty();
	unlock();
	return ret;
    }

private:
    void lock() const { mutex.lock(); }
    void unlock() const { mutex.unlock(); }
};


// ----------------------------------------------------------------------
// Functor for replacing the pointers to a pending frame with pointers
// to the corresponding full frame.
// ----------------------------------------------------------------------
// Replace frame functor
template<typename MetaData, typename Task>
struct replace_frame_functor {
    Task * from, * to;
    replace_frame_functor( Task * from_, Task * to_ )
	: from( from_ ), to( to_ ) { }

    template<typename T>
    bool operator () ( indep<T> & obj, typename indep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_task( from, to, g_read, &sa );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj, typename outdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_task( from, to, g_write, &sa );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( inoutdep<T> & obj, typename inoutdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	// md->replace_readers( from, to ); // -- always mute ?
	md->replace_task( from, to, g_write, &sa );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( truedep<T> & obj, typename truedep<T>::dep_tags & sa ) {
	return true;
    }
#if OBJECT_COMMUTATIVITY
    template<typename T>
    bool operator () ( cinoutdep<T> & obj,
		       typename cinoutdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_task( from, to, g_commut, &sa );
	md->unlock();
	return true;
    }
#endif
#if OBJECT_REDUCTION
    template<typename T>
    bool operator () ( reduction<T> & obj,
		       typename reduction<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_task( from, to, g_reduct, &sa );
	md->unlock();
	return true;
    }
#endif
};

// A replace frame function
#if STORED_ANNOTATIONS
template<typename MetaData, typename Task>
static inline void arg_replace_fn( Task * from, Task * to, task_data_t & td ) {
    replace_frame_functor<MetaData, Task> fn( from, to );
    char * args = td.get_args_ptr();
    char * tags = td.get_tags_ptr();
    size_t nargs = td.get_num_args();
    arg_apply_stored_fn( fn, nargs, args, tags );
}
#else
template<typename MetaData, typename Task, typename... Tn>
static inline void arg_replace_fn( Task * from, Task * to, task_data_t & td ) {
    replace_frame_functor<MetaData, Task> fn( from, to );
    char * args = td.get_args_ptr();
    char * tags = td.get_tags_ptr();
    arg_apply_fn<replace_frame_functor<MetaData, Task>,Tn...>( fn, args, tags );
}
#endif

// ----------------------------------------------------------------------
// ctg_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class task_metadata;

class ctg_metadata {
    typedef cas_mutex_v<char> mutex_t;

private:
#if OBJECT_TASKGRAPH == 5
    std::vector<task_metadata *> task_set[2]; // set of tasks with equal annot
#else // OBJECT_TASKGRAPH == 9
    dl_list<gen_tags> task_set[2]; // set of tasks with equal annot
#endif
    int cur;
    group_t group[2];
    mutex_t mutex;
    mutex_t cmutex;

public:
    ctg_metadata() : cur( 0 ) {
	// errs() << "ctg_metadata create: " << this << "\n";
	group[cur] = g_read;
    }
    ~ctg_metadata() {
	// errs() << "ctg_metadata delete: " << this << "\n";
	assert( !has_writers()
		&& "Must have zero writers when destructing ctg_metadata" );
	assert( !has_readers()
		&& "Must have zero readers when destructing ctg_metadata" );
    }

    // External inferface
    bool rename_is_active() const { return has_tasks(); }
    bool rename_has_readers() const { return has_readers(); }
    bool rename_has_writers() const { return has_writers(); }

    bool new_group( group_t g ) const { return g == g_write || group[cur] != g; }
    bool match_group( group_t g ) const {
	// Apply distribution
	// return ( !new_group( g ) || !has_tasks( cur ) ) && !has_tasks( 1-cur );
	return ( !new_group( g ) && !has_tasks( 1-cur ) ) || !has_tasks( cur );
    }

    // Register users of this object.
    void add_task( task_metadata * t, group_t g, gen_tags * tags ) { // requires lock on this
	// errs() << "obj " << this << " add task " << t << " g=" << g
	// << " cur=" << group[cur] << "\n";
	if( new_group( g ) ) {
	    if( has_tasks( cur ) ) {
		cur = 1 - cur;
		task_set[cur].clear();
	    }
	    group[cur] = g;
	}
#if OBJECT_TASKGRAPH == 5
	task_set[cur].push_back( t );
#else // OBJECT_TASKGRAPH == 9
	tags->task = t;
	task_set[cur].push_back( tags );
#endif
    }

    // Only intended for output, so g == g_write
    void force_task( task_metadata * t, gen_tags * tags ) {
	assert( !has_tasks( cur ) );
	cur = 0;
	group[0] = g_write;
	group[1] = g_read;
#if OBJECT_TASKGRAPH == 5
	task_set[0].push_back( t );
#else // OBJECT_TASKGRAPH == 9
	tags->task = t;
	task_set[0].push_back( tags );
#endif
    }

    // Erase links if we are about to destroy a task
#if OBJECT_TASKGRAPH == 5
private:
    bool del_task( task_metadata * fr, int idx ) {
	// It ain't here if it's a different type
	auto I = std::find( task_set[idx].begin(), task_set[idx].end(), fr );
	if( I != task_set[idx].end() ) {
	    // errs() << "del_task " << fr << " at idx=" << idx << "\n";
#if ERASE_NULL
	    *I = 0;
#else
	    task_set[idx].erase( I );
#endif
	    return true;
	}
	return false;
    }
public:
    void del_task( task_metadata * fr, group_t g, gen_tags * tags ) {
	if( group[0] == g ) { // It ain't here if it's a different type
	    if( del_task( fr, 0 ) )
		return;
	}
	if( group[1] == g )
	    del_task( fr, 1 );
    }
#else // OBJECT_TASKGRAPH == 9
    void del_task( task_metadata * fr, group_t g, gen_tags * tags ) {
	if( g == g_write ) {
	    // If we delete a writer, we may have two consecutive write
	    // generations, which means we may have to erase from both lists.
	    // Also, each list can have at most one element.
	    if( group[0] == g )
		task_set[0].erase_half( dl_list<gen_tags>::iterator( tags ) );
	    if( group[1] == g )
		task_set[1].erase_half( dl_list<gen_tags>::iterator( tags ) );
	} else {
	    if( group[0] == g )
		task_set[0].erase( dl_list<gen_tags>::iterator( tags ) );
	    else // if( group[1] == g ) -- del w/o knowing list head if needed
		task_set[1].erase( dl_list<gen_tags>::iterator( tags ) );
	}
	    
    }
#endif

    // Replace writer/readers
#if OBJECT_TASKGRAPH == 5
private:
    bool replace_task( task_metadata * from, task_metadata * to, int idx ) {
	auto I = std::find( task_set[idx].begin(), task_set[idx].end(), from );
	if( I != task_set[idx].end() ) {
	    *I = to;
	    return true;
	}
	return false;
    }
public:
    void replace_task( task_metadata * from, task_metadata * to, group_t g,
		       gen_tags * tags ) {
	if( group[0] == g ) {
	    if( replace_task( from, to, 0 ) )
		return;
	}
	if( group[1] == g )
	    replace_task( from, to, 1 );
    }
#else // OBJECT_TASKGRAPH == 9
public:
    void replace_task( task_metadata * from, task_metadata * to, group_t g,
		       gen_tags * tags ) {
	assert( tags->task == from && "generation list error" );
	tags->task = to;
    }
#endif

    // Dependency queries on last_readers and last_writer
private:
    bool has_tasks( int idx ) const {
#if ERASE_NULL
	for( auto I=task_set[idx].begin(), E=task_set[idx].end(); I != E; ++I )
	    if( *I )
		return true;
	return false;
#else
	return !task_set[idx].empty();
#endif
    }
public:
    bool has_tasks() const { return has_tasks( cur ); }
    bool has_readers() const {
	return ( group[0] == g_read && has_tasks( 0 ) )
	    || ( group[1] == g_read && has_tasks( 1 ) );
    }
    bool has_writers() const {
	return ( group[0] != g_read && has_tasks( 0 ) )
	    || ( group[1] != g_read && has_tasks( 1 ) );
    }

    // Link to readers
    inline void link_task( task_metadata * fr ); // lock required on this

#if OBJECT_COMMUTATIVITY
    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return cmutex.try_lock(); }
    void commutative_release() { cmutex.unlock(); }
#endif

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }
};

// Some debugging support. Const-ness of printed argument is a C++ library
// requirement, but we want to keep the lock as non-const.
inline std::ostream &
operator << ( std::ostream & os, const ctg_metadata & md_const ) {
    return os << "taskgraph_md={...}";
}

// ----------------------------------------------------------------------
// full_metadata: task graph metadata per full frame
// ----------------------------------------------------------------------
class full_metadata {
    taskgraph graph;

protected:
    full_metadata() { }

public:
    // This functionality is implemented through arg_issue of the arguments
    void push_pending( pending_metadata * frame ) { }

    pending_metadata * get_ready_task() {
	return graph.get_ready_task();
    }
    pending_metadata * get_ready_task_after( task_metadata * prev ) {
	return graph.get_ready_task();
    }

    taskgraph * get_graph() { return &graph; }

    // void lock() { graph.lock(); }
    // void unlock() { graph.unlock(); }
};

// ----------------------------------------------------------------------
// task_metadata: dependency-tracking metadata for tasks (pending and stack)
//     We require the list of objects that are held by this task. This is
//     obtained through the actual argument list of the task.
// ----------------------------------------------------------------------
class task_metadata : public task_data_t {
    typedef cas_mutex mutex_t;
#if !STORED_ANNOTATIONS
    typedef void (*replace_fn_t)( task_metadata *, task_metadata *, task_data_t & );
    typedef bool (*acquire_fn_t)( task_data_t & );
#endif

private:
    std::vector<task_metadata *> deps;
    taskgraph * graph;
#if !STORED_ANNOTATIONS
    replace_fn_t replace_fn;
    acquire_fn_t acquire_fn;
#endif
    size_t incoming_count;
    mutex_t mutex;

protected:
    // Default constructor
    task_metadata() : graph( 0 ),
#if !STORED_ANNOTATIONS
		      replace_fn( 0 ),
		      acquire_fn( 0 ),
#endif
		      incoming_count( 0 ) {
	// errs() << "task_metadata create: " << this << '\n';
    }
    ~task_metadata() {
	// errs() << "task_metadata delete: " << this << '\n';
    }
    // Constructor for creating stack frame from pending frame
    void create_from_pending( task_metadata * from, full_metadata * ff ) {
	graph = ff->get_graph();
	assert( graph != 0 && "create_from_pending with null graph" );
	// errs() << "task_metadata create " << this << " from pending " << from << " graph " << graph << "\n";

	// First change the task pointer for each argument's dependency list,
	// then assign all outgoing dependencies from <from> to us.
	lock();
#if STORED_ANNOTATIONS
	arg_replace_fn<ctg_metadata,task_metadata>( from, this, get_task_data() );
#else
	assert( from->replace_fn && "Don't have a replace frame function" );
	(*from->replace_fn)( from, this, get_task_data() );
#endif

	assert( deps.empty() );
	from->lock();
	deps.swap( from->deps );
	from->unlock();
	unlock();
    }

    void convert_to_full( full_metadata * ff ) {
	graph = ff->get_graph();
	assert( graph != 0 && "create_from_pending with null graph" );
    }

public:
    template<typename... Tn>
    void create( full_metadata * ff ) {
	graph = ff->get_graph();
	// assert( graph != 0 && "Create with null graph" );
#if !STORED_ANNOTATIONS
	replace_fn = &arg_replace_fn<ctg_metadata,task_metadata,Tn...>;
	acquire_fn = &arg_acquire_fn<ctg_metadata,Tn...>;
#endif
	// errs() << "set graph in " << this << " to " << graph << "\n";
    }

public:
    // Add edge: Always first increment incoming count, then add edge.
    // Reason: we don't have a lock on "to" and edge should not be observable
    // before we increment the incoming count. Otherwise, another (racing)
    // thread could erroneously conclude that task "to" is ready.
    // Note: we should have a lock on this such that we are thread-safe on deps
    void add_edge( task_metadata * to ) { // lock required on this, not on to
	to->add_incoming();
	assert( mutex.test_lock() );
	deps.push_back( to );
    }

    taskgraph * get_graph() { return graph; }

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

    // Iterating
    typedef std::vector<task_metadata *>::const_iterator dep_iterator;
    dep_iterator dep_begin() const { return deps.begin(); }
    dep_iterator dep_end()   const { return deps.end();   }

    // Waking up dependents - only for a stack_frame
    inline void wakeup_deps();

#ifdef UBENCH_HOOKS
    // Because the ubench driver will not always call the destructor
    void reset_deps() { deps.clear(); }
#endif

    // Self wakeup (change of state between arg_ready() and arg_issue())
    inline void add_to_graph();

    // Acquire and privatize for commutativity and reductions
    bool acquire() {
#if STORED_ANNOTATIONS
	return arg_acquire_fn<ctg_metadata>( get_task_data() );
#else
	assert( acquire_fn && "Don't have an acquire frame function" );
	return (*acquire_fn)( get_task_data() );
#endif
    }

    // Ready counter
    void add_incoming() { __sync_fetch_and_add( &incoming_count, 1 ); }
    bool del_incoming() {
	return __sync_fetch_and_add( &incoming_count, -1 ) == 1;
    }
    size_t get_incoming() const volatile { return incoming_count; }

    // To avoid races when adding a node to the graph, we must increment
    // the incoming_count to make sure that the task is not considered ready
    // when one of the inputs becomes available, but the others have not been
    // added yet.
    template<typename... Tn>
    void start_registration() { add_incoming(); }
    void stop_registration( bool wakeup = false ) {
	if( del_incoming() && wakeup )
	    add_to_graph();
    }

    void start_deregistration() { }
    void stop_deregistration( full_metadata * parent ) {
	lock(); wakeup_deps(); unlock();
    }
};

void
ctg_metadata::link_task( task_metadata * fr ) {
    for( auto I=task_set[1-cur].begin(), E=task_set[1-cur].end(); I != E; ++I ){
	task_metadata * t = *I;
#if ERASE_NULL
	if( !t )
	    continue;
#endif
	// errs() << "obj " << this << " link task " << t << " to " << fr  << '\n';
	t->lock();
	t->add_edge( fr ); // requires lock on t, not on fr
	t->unlock();
    }
}

// ----------------------------------------------------------------------
// link_metadata: task graph metadata per stored frame
// ----------------------------------------------------------------------
class link_metadata {
};

// ----------------------------------------------------------------------
// pending_metadata: task graph metadata per pending frame
// ----------------------------------------------------------------------
class pending_metadata : public task_metadata, public link_metadata {
public:
    static inline
    pending_metadata * get_from_task( task_metadata * task ) {
	return static_cast<pending_metadata *>( task );
    }
};

pending_metadata *
taskgraph::get_ready_task() {
    lock();
    pending_metadata * task = 0;
    if( !ready_list.empty() ) {
	for( auto I=ready_list.begin(), E=ready_list.end(); I != E; ++I ) {
	    pending_metadata * t = *I;
	    if( t->acquire() ) {
		ready_list.erase( I );
		task = t;
		break;
	    }
	}
    }
    unlock();
    // errs() << "get_ready_task from TG " << this << ": " << task << "\n";
    return task;
}

void
task_metadata::wakeup_deps() {
    assert( mutex.test_lock() );
    for( dep_iterator I=dep_begin(), E=dep_end(); I != E; ++I ) {
	task_metadata * t = *I;
	// Don't need a lock: only one task can be the last to
	// wakeup t (atomic decrement of join counter)
	// t->lock();
	// errs() << "task " << this << " dec_incoming " << t << '\n';
	if( t->del_incoming() ) {
	    graph->add_ready_task( pending_metadata::get_from_task( t ) );
	    // errs() << "task " << this << " wakes up " << t << '\n';
	}
	// t->unlock();
    }
}

void
task_metadata::add_to_graph() {
    graph->add_ready_task( pending_metadata::get_from_task( this ) );
}

// ----------------------------------------------------------------------
// Dependency handling traits
// ----------------------------------------------------------------------
// Whole-function dependency tags
class function_tags : public function_tags_base { };

// A fully serialized version
class serial_dep_tags : public gen_tags { };

struct serial_dep_traits {
// serial dependency traits (inout and unversioned out)
    static void
    arg_issue( task_metadata * fr, obj_instance<ctg_metadata> & obj,
	       serial_dep_tags * sa, group_t g ) {
	ctg_metadata * md = obj.get_version()->get_metadata();
	md->lock();
	md->add_task( fr, g, sa );
	md->link_task( fr );
	md->unlock();
    }
    static bool
    arg_ini_ready( const obj_instance<ctg_metadata> & obj, group_t g ) {
	const ctg_metadata * md = obj.get_version()->get_metadata();
	return md->match_group( g );
    }
    static void
    arg_release( task_metadata * fr, obj_instance<ctg_metadata> & obj,
		 serial_dep_tags * sa, group_t g ) {
	ctg_metadata * md = obj.get_version()->get_metadata();
	md->lock();
	md->del_task( fr, g, sa );
	md->unlock();
    }
};

// Input dependency tags
class indep_tags : public indep_tags_base, public serial_dep_tags { };

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public outdep_tags_base, public serial_dep_tags { };

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public inoutdep_tags_base, public serial_dep_tags { };

#if OBJECT_COMMUTATIVITY
// Commutative input/output dependency tags
class cinoutdep_tags : public cinoutdep_tags_base, public serial_dep_tags { };
#endif

#if OBJECT_REDUCTION
// Reduction dependency tags
class reduction_tags : public reduction_tags_base<ctg_metadata>,
		       public serial_dep_tags { };
#endif

/* @note
 * Locking strategy when doing arg_issue:
 *    Set incoming +1 artificially early in arg_issue_fn or in constructor
 *    and then add dependencies *without* a lock. When done, dec incoming,
 *    allowing the task to become ready (and also move to graph if it happens,
 *    which may be the case).
 *    Make sure that we always increment incoming before storing the link
 *    (see task_metadata::add_edge).
 */


// indep traits
template<>
struct dep_traits<ctg_metadata, task_metadata, indep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, indep<T> & obj,
	       typename indep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_read );
    }
    template<typename T>
    static bool
    arg_ini_ready( const indep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj, g_read );
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, indep<T> & obj,
		 typename indep<T>::dep_tags & sa ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_read );
    }
};

// output dependency traits
template<>
struct dep_traits<ctg_metadata, task_metadata, outdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, outdep<T> & obj,
	       typename outdep<T>::dep_tags * sa ) {
	ctg_metadata * md = obj.get_version()->get_metadata();
	// In principle, no lock required, we are the first and only so far.
	// However, because rename_is_active() is not protected by a lock,
	// it may race with a release (it only tests for head == 0, tail = 0
	// may be pending). Hence, we take the lock here anyway. The alternative
	// is to lock rename_is_active().
	md->lock();
	md->force_task( fr, sa );
	md->unlock();
    }
    template<typename T>
    static bool
    arg_ini_ready( const outdep<T> & obj ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicator
	return true;
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, outdep<T> & obj,
		 typename outdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_write );
    }
};

// inout dependency traits
template<>
struct dep_traits<ctg_metadata, task_metadata, inoutdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, inoutdep<T> & obj,
	       typename inoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_write );
    }
    template<typename T>
    static bool
    arg_ini_ready( const inoutdep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj, g_write );
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, inoutdep<T> & obj,
		 typename inoutdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_write );
    }
};

#if OBJECT_COMMUTATIVITY
template<>
struct dep_traits<ctg_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, cinoutdep<T> & obj,
	       typename cinoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_commut );
    }
    template<typename T>
    static bool
    arg_ini_ready( cinoutdep<T> & obj ) {
	ctg_metadata * md = obj.get_version()->get_metadata();
	if( md->match_group( g_commut ) ) {
	    if( md->commutative_try_acquire() )
		return true;
	}
	return false;
    }
    template<typename T>
    static void
    arg_ini_ready_undo( cinoutdep<T> & obj ) {
	obj.get_version()->get_metadata()->commutative_release();
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, cinoutdep<T> & obj,
		 typename cinoutdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_commut );
	obj.get_version()->get_metadata()->commutative_release();
    }
};
#endif

#if OBJECT_REDUCTION
template<>
struct dep_traits<ctg_metadata, task_metadata, reduction> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, reduction<T> & obj,
	       typename reduction<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_reduct );
    }
    template<typename T>
    static bool
    arg_ini_ready( const reduction<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj, g_reduct );
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, reduction<T> & obj,
		 typename reduction<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_reduct );
    }
};
#endif

typedef ctg_metadata obj_metadata;
typedef ctg_metadata token_metadata;

} // end of namespace obj

#endif // CTASKGRAPH_H

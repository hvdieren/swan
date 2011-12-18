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
/* taskgraph.h
 * This file implements an explicit task graph where edges between tasks
 * are explicitly maintained.
 *
 * TODO:
 * + finalize() when adding to ready list
 */
#ifndef TASKGRAPH_H
#define TASKGRAPH_H

#include <cstdint>
#include <iostream>
#include <queue>

#include "wf_frames.h"
#include "lock.h"

// ERASE_NULL: When erasing a reader from the readers list, don't unlink,
// but set pointer to null.
#define ERASE_NULL 0

#if OBJECT_COMMUTATIVITY
#error taskgraph does not support commutativity
#endif

#if OBJECT_REDUCTION
#error taskgraph does not support reductions
#endif

namespace obj {

// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
class pending_metadata;

class taskgraph {
    typedef cas_mutex mutex_t;

private:
    std::queue<pending_metadata *> ready_list; // implement using prev/next in link_md?
    mutable mutex_t mutex;

public:
    ~taskgraph() {
	assert( ready_list.empty() && "Pending tasks at destruction time" );
    }

    pending_metadata * get_ready_task() {
	lock();
	pending_metadata * task = 0;
	if( !ready_list.empty() ) {
	    task = ready_list.front();
	    ready_list.pop();
	}
	unlock();
	// errs() << "get_ready_task from TG " << this << ": " << task << "\n";
	return task;
    }

    void add_ready_task( pending_metadata * fr ) {
	// errs() << "add_ready_task to TG " << this << ": " << fr << "\n";
	lock();
	ready_list.push( fr );
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
	md->replace_readers( from, to );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj, typename outdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_writer( from, to );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( inoutdep<T> & obj, typename inoutdep<T>::dep_tags & sa ) {
	MetaData * md = obj.get_version()->get_metadata();
	md->lock();
	md->replace_readers( from, to ); // -- always mute ?
	md->replace_writer( from, to );
	md->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( truedep<T> & obj, typename truedep<T>::dep_tags & sa ) {
	return true;
    }
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
// tg_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class task_metadata;

class tg_metadata {
    typedef cas_mutex mutex_t;

private:
    task_metadata * last_writer; // last writer
    std::vector<task_metadata *> last_readers; // set of readers after the writer
    mutex_t mutex;

public:
    tg_metadata() : last_writer( 0 ) {
	// errs() << "tg_metadata create: " << this << "\n";
    }
    ~tg_metadata() {
	// errs() << "tg_metadata delete: " << this << "\n";
	assert( last_writer == 0
		&& "Must have zero writers when destructing tg_metadata" );
	assert( !has_readers()
		&& "Must have zero readers when destructing tg_metadata" );
    }

    // External inferface
    bool rename_has_readers() const { return has_readers(); }
    bool rename_has_writers() const { return has_writers(); }

    // Register users of this object.
    void add_reader( task_metadata * t ) { // requires lock on this
	// errs() << "obj " << this << " add reader " << t << "\n";
	last_readers.push_back( t );
    }
    void add_writer( task_metadata * t ) { // requires lock on this
	// errs() << "obj " << this << " add writer " << t << "\n";
	last_readers.clear();
	last_writer = t;
    }

    // Erase links if we are about to destroy a task
    void del_writer( task_metadata * fr ) {
	if( last_writer == fr )
	    last_writer = 0;
	// errs() << "* removing writer " << fr << " from "
	// << this << " last_writer is now " << last_writer << "\n";
    }
    void del_reader( task_metadata * fr ) {
	// errs() << "* removing reader " << fr << " from " << this << "\n";
#if ERASE_NULL
	auto I = std::find( last_readers.begin(), last_readers.end(), fr );
	if( I != last_readers.end() )
	    *I = 0;
#else
	auto I = std::find( last_readers.begin(), last_readers.end(), fr );
	if( I != last_readers.end() )
	    last_readers.erase( I );
#endif
    }

    // Replace writer/readers
    void replace_writer( task_metadata * from, task_metadata * to ) {
	if( last_writer == from )
	    last_writer = to;
    }
    void replace_readers( task_metadata * from, task_metadata * to ) {
	auto I = std::find( last_readers.begin(), last_readers.end(), from );
	if( I != last_readers.end() )
	    *I = to;
    }

    // Dependency queries on last_readers and last_writer
    bool has_readers() const {
#if ERASE_NULL
	for( auto I=reader_begin(), E=reader_end(); I != E; ++I )
	    if( *I )
		return true;
	return false;
#else
	return !last_readers.empty();
#endif
    }
    bool has_writers() const { return get_last_writer() != 0; }
    task_metadata * get_last_writer() const { return last_writer; }

    // Dependency queries on last_readers
    typedef std::vector<task_metadata *>::const_iterator reader_iterator;
    reader_iterator reader_begin() const { return last_readers.begin(); }
    reader_iterator reader_end() const { return last_readers.end(); }

    // Link to readers
    inline void link_readers( task_metadata * fr ); // lock required on this
    inline void link_writer( task_metadata * fr ); // lock required on this

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }
};

// Some debugging support. Const-ness of printed argument is a C++ library
// requirement, but we want to keep the lock as non-const.
inline std::ostream &
operator << ( std::ostream & os, const tg_metadata & md_const ) {
    tg_metadata & md = *const_cast<tg_metadata *>( &md_const );
    os << "taskgraph_md={dep_tasks=";
    md.lock();
    for( tg_metadata::reader_iterator
	     I=md.reader_begin(), E=md.reader_end(); I != E; ++I ) {
#if ERASE_NULL
	if( !*I )
	    continue;
#endif
	os << *I << ", ";
    }
    md.unlock();
    os << '}';
    return os;
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
#endif

private:
    std::vector<task_metadata *> deps;
    taskgraph * graph;
#if !STORED_ANNOTATIONS
    replace_fn_t replace_fn;
#endif
    size_t incoming_count;
    mutex_t mutex;

protected:
    // Default constructor
    task_metadata() : graph( 0 ),
#if !STORED_ANNOTATIONS
		      replace_fn( 0 ),
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
	// errs() << "task_metadata create " << this << " from pending " << from << " graph " << graph << "\n";

	// First change the task pointer for each argument's dependency list,
	// then assign all outgoing dependencies from <from> to us.
	lock();
#if STORED_ANNOTATIONS
	arg_replace_fn<tg_metadata,task_metadata>( from, this, get_task_data() );
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
#if !STORED_ANNOTATIONS
	replace_fn = &arg_replace_fn<tg_metadata,task_metadata,Tn...>;
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

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

    // Iterating
    typedef std::vector<task_metadata *>::const_iterator dep_iterator;
    dep_iterator dep_begin() const { return deps.begin(); }
    dep_iterator dep_end()   const { return deps.end();   }

    // Waking up dependents - only for a stack_frame
    inline void wakeup_deps();

    // Self wakeup (change of state between arg_ready() and arg_issue())
    inline void add_to_graph();

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
    void stop_deregistration() { lock(); wakeup_deps(); unlock(); }
};

void
tg_metadata::link_readers( task_metadata * fr ) {
    for( reader_iterator I=reader_begin(), E=reader_end(); I != E; ++I ) {
	task_metadata * t = *I;
#if ERASE_NULL
	if( !t )
	    continue;
#endif
	// errs() << "obj " << this << " link reader " << t << " to " << fr  << '\n';
	t->lock();
	t->add_edge( fr ); // requires lock on t, not on fr
	t->unlock();
    }
}

void
tg_metadata::link_writer( task_metadata * fr ) {
    if( last_writer ) {
	// errs() << "obj " << this << " link writer " << last_writer << " to " << fr  << '\n';
	last_writer->lock();
	last_writer->add_edge( fr ); // requires lock on t, not on fr
	last_writer->unlock();
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

// A fully serialized version
class serial_dep_tags { };

struct serial_dep_traits {
// serial dependency traits (inout and unversioned out)
    static void
    arg_issue( task_metadata * fr, obj_instance<tg_metadata> & obj,
	       serial_dep_tags * sa ) {
	obj_version<tg_metadata> * v = obj.get_version();
	tg_metadata * md = v->get_metadata();
	md->lock();

	if( md->has_readers() || md->has_writers() ) {
	    md->link_writer( fr );
	    md->link_readers( fr );
	}
	md->add_writer( fr );

	md->unlock();
    }
    static bool
    arg_ini_ready( const obj_instance<tg_metadata> & obj ) {
	return !obj.get_version()->get_metadata()->has_readers()
	    & !obj.get_version()->get_metadata()->has_writers();
    }
    static void
    arg_release( task_metadata * fr, obj_instance<tg_metadata> & obj ) {
	obj_version<tg_metadata> * v = obj.get_version();
	tg_metadata * md = v->get_metadata();

	md->lock();
	md->del_writer( fr );
	md->unlock();
    }
};

// Input dependency tags
class indep_tags : public indep_tags_base, public serial_dep_tags { };

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public outdep_tags_base, public serial_dep_tags { };

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public inoutdep_tags_base, public serial_dep_tags { };

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
struct dep_traits<tg_metadata, task_metadata, indep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, indep<T> & obj,
	       typename indep<T>::dep_tags * sa ) {
	obj_version<tg_metadata> * v = obj.get_version();
	tg_metadata * md = v->get_metadata();
	md->lock();

	if( md->has_writers() )
	    md->link_writer( fr ); // requires lock on md
	md->add_reader( fr ); // requires lock on md

	md->unlock();
    }
    template<typename T>
    static bool
    arg_ini_ready( const indep<T> & obj ) {
	return !obj.get_version()->get_metadata()->has_writers();
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, indep<T> & obj,
		 typename indep<T>::dep_tags & sa ) {
	obj_version<tg_metadata> * v = obj.get_version();
	tg_metadata * md = v->get_metadata();

	md->lock();
	md->del_reader( fr ); // requires lock on md
	md->unlock();
    }
};

// output dependency traits
template<>
struct dep_traits<tg_metadata, task_metadata, outdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, outdep<T> & obj,
	       typename outdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa );
    }
    template<typename T>
    static bool
    arg_ini_ready( const outdep<T> & obj ) {
	assert( v->is_versionable() ); // enforced by applicators
	return true;
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, outdep<T> & obj,
		 typename outdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj );
    }
};

// inout dependency traits
template<>
struct dep_traits<tg_metadata, task_metadata, inoutdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, inoutdep<T> & obj,
	       typename inoutdep<T>::dep_tags * sa ) {
	obj_version<tg_metadata> * v = obj.get_version();

	tg_metadata * md = v->get_metadata();
	md->lock();

	if( md->has_readers() || md->has_writers() ) {
	    md->link_writer( fr );
	    md->link_readers( fr );
	}
	md->add_writer( fr );

	md->unlock();
    }
    template<typename T>
    static bool
    arg_ini_ready( const inoutdep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj );
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, inoutdep<T> & obj,
		 typename inoutdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj );
    }
};

typedef tg_metadata obj_metadata;

} // end of namespace obj

#endif // TASKGRAPH_H

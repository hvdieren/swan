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
/* egtaskgraph.h
 * This file implements an embedded generational task graph where edges
 * between tasks are explicitly maintained. They are not gathered
 * on a single dependee list per task, but they are kept on a readers
 * list per generation.
 */
#ifndef EGTASKGRAPH_H
#define EGTASKGRAPH_H

#include <cstdint>
#include <iostream>
#include <queue>

#include "wf_frames.h"
#include "lock.h"
#include "lfllist.h"
#include "padding.h"
#include "alc_allocator.h"
#include "alc_mmappol.h"
#include "alc_flpol.h"

namespace obj {

// ----------------------------------------------------------------------
// link_metadata: task graph metadata per stored frame
// ----------------------------------------------------------------------
class link_metadata {
    link_metadata * next;
    friend class sl_list_traits<link_metadata>;
};

//----------------------------------------------------------------------
// Traits for accessing elements stored in a dl_list<>, in global
// namespace.
//----------------------------------------------------------------------

} // end namespace obj because the traits class must be in global namespace

template<>
struct sl_list_traits<obj::link_metadata> {
    typedef obj::link_metadata T;
    typedef obj::link_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    // static void set_prev( T * elm, T * prev ) { elm->prev = prev; }
    // static T * get_prev( T const * elm ) { return elm->prev; }
    static void set_next( T * elm, T * next ) { elm->next = next; }
    static T * get_next( T const * elm ) { return elm->next; }
};

namespace obj { // reopen
// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
class taskgraph {
    typedef cas_mutex mutex_t;

private:
    sl_list<link_metadata> ready_list;
    mutable mutex_t mutex;

public:
    ~taskgraph() {
	assert( ready_list.empty() && "Pending tasks at destruction time" );
    }

    link_metadata * get_ready_task() {
	lock();
	link_metadata * task = 0;
	if( !ready_list.empty() ) {
	    task = ready_list.front();
	    ready_list.pop();
	}
	unlock();
	// errs() << "get_ready_task from TG " << this << ": " << task << "\n";
	return task;
    }

    void add_ready_task( link_metadata * fr ) {
	// Translate from task_metadata to link_metadata
	// Can only be successfully done in case of queued_frame!
	// errs() << "add_ready_task to TG " << this << ": " << fr << "\n";
	lock();
	ready_list.push_back( fr );
	unlock();
    }

    // Don't need a lock in this check because it is based on polling a
    // single variable
    bool empty() const {
	// lock();
	bool ret = ready_list.empty();
	// unlock();
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
	sa.gen->lock();
	sa.gen->replace_readers( from, to, &sa );
	sa.gen->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( outdep<T> & obj, typename outdep<T>::dep_tags & sa ) {
	sa.gen->lock();
	sa.gen->replace_writer( from, to );
	sa.gen->unlock();
	return true;
    }
    template<typename T>
    bool operator () ( inoutdep<T> & obj, typename inoutdep<T>::dep_tags & sa ) {
	sa.gen->lock();
	sa.gen->replace_writer( from, to );
	sa.gen->unlock();
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
// input_tags, gentags: tag storage for all dependency usage types with
// ----------------------------------------------------------------------
class task_metadata;
class generation;

// gen_tags stores the accessed generation. Required for all types
class gen_tags {
public:
    generation * gen;
};

// input_tags applies to indep tasks, providing a way to link the indep
// task on the readers list of an object, and/or providing a way to point
// to the single following inoutdep task that may exist. indep tasks can wakeup
// at most one task and that's an inoutdep.
class input_tags {
    input_tags * it_prev, * it_next;
    task_metadata * st_task;

    friend class generation;
    friend class dl_list_traits<input_tags>;
};

} // end namespace obj because the traits class must be in global namespace

template<>
struct dl_list_traits<obj::input_tags> {
    typedef obj::input_tags T;
    typedef obj::task_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    static void set_prev( T * elm, T * prev ) { elm->it_prev = prev; }
    static T * get_prev( T const * elm ) { return elm->it_prev; }
    static void set_next( T * elm, T * next ) { elm->it_next = next; }
    static T * get_next( T const * elm ) { return elm->it_next; }

    static ValueType * get_value( T const * elm ) { return elm->st_task; }
};

namespace obj { // reopen

// ----------------------------------------------------------------------
// generation: represent a single generation in an object's taskgraph
// Note: Deallocation of generations
//    * We could use a reference counter, but this requires atomic inc/dec
//    * We can infer when to delete a generation: conjunction of:
//      + when there are no more writers (after a deletion)
//      + when there are no more readers (after a deletion)
//      + when the next pointer is non-null (if it is null, then the
//        object is holding a pointer to this generation).
// ----------------------------------------------------------------------
class generation {
    typedef cas_mutex mutex_t;
    typedef uint32_t ctr_t;

private:
    generation * next;
    task_metadata * writer; // last writer
    dl_head_list<input_tags> readers; // set of readers after the writer
    size_t num_readers;
    mutex_t mutex;

    pad_multiple<64, 2*sizeof(void *)+sizeof(dl_head_list<input_tags>)
		 + sizeof(mutex_t) + sizeof(ctr_t)> padding;

protected:
    friend class egtg_metadata;

    generation( task_metadata * w = 0 )
	: next( 0 ), writer( w ), num_readers( 0 ) {
	// errs() << "create generation " << this << "\n";
    }
    ~generation() {
	// errs() << "delete generation " << this << "\n";
	assert( next && "egtg_metadata must not link to us" );
	assert( !has_writers()
		&& "Must have zero writers when destructing egtg_metadata" );
	assert( !has_readers()
		&& "Must have zero readers when destructing egtg_metadata" );
    }

    inline void * operator new ( size_t size );
    inline void operator delete( void * p );

public:
    // Deallocation
    bool consider_delete() {
	if( next && !has_writers() && !has_readers() ) {
	    delete this;
	    return true;
	} else
	    return false;
    }

    // Access to generation
    generation * get_next_generation() { return next; }
    void set_next_generation( generation * g ) { next = g; }

    // Dependency queries on readers
    typedef dl_head_list<input_tags>::const_iterator reader_iterator;
    reader_iterator reader_begin() const { return readers.begin(); }
    reader_iterator reader_end() const { return readers.end(); }

    // Register users of this object.
    void add_reader( task_metadata * t, input_tags * tags ) {
	// errs() << "gen " << this << " add reader " << t << "\n";
	tags->st_task = t;
	readers.push( tags );
	++num_readers;
    }
    generation * add_writer( task_metadata * t ) {
	// errs() << "gen " << this << " add writer " << t << "\n";
	// readers.clear();
	assert( !has_readers() && "Assume new generation on add-writer" );
	writer = t;
	return this;
    }

    // Erase links if we are about to destroy a task
    void del_writer( task_metadata * fr, taskgraph * graph ) {
	assert( writer == fr );
	writer = 0;
	wakeup_readers( graph );
	// errs() << "* removing writer " << fr << " from "
	       // << this << " writer is now " << writer << "\n";
    }
    void del_reader( task_metadata * fr, taskgraph * graph,
		     input_tags * tags ) {
	// This is already blazing fast ;-). The generic solution cannot
	// provide the pointer to the list node to delete by itself.
	// errs() << "gen " << this << " del reader " << fr << " @" << tags << "\n";
	readers.erase( dl_head_list<input_tags>::iterator( tags ) );
	--num_readers;
	wakeup_next_writer( graph );
    }

    // Replace writer/readers
    void replace_writer( task_metadata * from, task_metadata * to ) {
	// errs() << "gen " << this << " replace writer from=" << from
	       // << " to=" << to << '\n';
	assert( writer == from );
	// if( writer == from )
	writer = to;
    }
    void replace_readers( task_metadata * from, task_metadata * to,
			  input_tags * tags ) {
	// This is already blazing fast ;-). The generic solution cannot
	// provide the pointer to the list node to update by itself.
	assert( tags->st_task == from && "replace_readers list corrupt" );
	tags->st_task = to;
    }

    // Dependency queries on readers and writer
    bool has_readers() const { return get_num_readers() != 0; }
    bool has_writers() const { return get_last_writer() != 0; }
    size_t get_num_readers() const { return num_readers; }
    task_metadata * get_last_writer() const { return writer; }

    // Link to readers
    inline void link_readers( task_metadata * fr );
    inline void link_writer( task_metadata * fr );

    // Waking up dependents - only for a stack_frame
    inline void wakeup_readers( taskgraph * graph );
    inline void wakeup_next_writer( taskgraph * graph );

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }
};

namespace generation_allocator_ns {
typedef alc::mmap_alloc_policy<generation, sizeof(generation)> mmap_align_pol;
typedef alc::freelist_alloc_policy<generation, mmap_align_pol,
				   64> list_align_pol;
typedef alc::allocator<generation, list_align_pol> alloc_type;
}

typedef generation_allocator_ns::alloc_type generation_allocator;

extern __thread generation_allocator * tls_egtg_allocator;

void * generation::operator new ( size_t size ) {
    if( !tls_egtg_allocator )
	tls_egtg_allocator = new generation_allocator();
    return tls_egtg_allocator->allocate( 1 );
}
void generation::operator delete( void * p ) {
    if( !tls_egtg_allocator ) // we may deallocate blocks allocated elsewhere
	tls_egtg_allocator = new generation_allocator();
    return tls_egtg_allocator->deallocate( (generation *)p, 1 );
}

// ----------------------------------------------------------------------
// egtg_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class egtg_metadata {
    typedef cas_mutex mutex_t;

private:
    // Every task can be on one readers list for every in/inout
    // dependency that it has. Embed this list in the tags with next/prev
    // links and a pointer to the task.
    generation * gen;
    mutex_t mutex;

public:
    egtg_metadata() : gen( new generation() ) {
	// errs() << "egtg_metadata create: " << this << "\n";
    }
    ~egtg_metadata() {
	// errs() << "egtg_metadata delete: " << this << "\n";
	gen->lock();
	gen->set_next_generation( (generation *)1 );
	if( !gen->consider_delete() )
	    gen->unlock();
    }

public:
    // External inferface
    bool rename_has_readers() const { return gen->has_readers(); }
    bool rename_has_writers() const { return gen->has_writers(); }

    generation * get_generation() { return gen; }

    // Register users of this object.
    generation * push_generation( task_metadata * t ) {
	// errs() << "obj " << this << " push generation " << t << "\n";
	// lock();
	assert( gen->has_writers() || gen->has_readers() );
	generation * new_gen = new generation( t );
	gen->set_next_generation( new_gen );
	gen = new_gen;
	// unlock();
	return new_gen;
    }

    // Dependency queries on current generation
    bool has_readers() const { return gen->has_readers(); }
    bool has_writers() const { return gen->has_writers(); }

    // Locking
    // void lock() { mutex.lock(); }
    // void unlock() { mutex.unlock(); }
};

// Some debugging support. Const-ness of printed argument is a C++ library
// requirement, but we want to keep the lock as non-const.
inline std::ostream &
operator << ( std::ostream & os, const egtg_metadata & md_const ) {
    egtg_metadata & md = *const_cast<egtg_metadata *>( &md_const );
    generation * gen = md.get_generation();
    gen->lock();
    os << "taskgraph_md={gen=" << gen << ", dep_tasks=";
    for( generation::reader_iterator
	     I=gen->reader_begin(), E=gen->reader_end(); I != E; ++I )
	os << *I << ", ";
    gen->unlock();
    os << '}';
    return os;
}

// ----------------------------------------------------------------------
// task_metadata: dependency-tracking metadata for tasks (pending and stack)
//     We require the list of objects that are held by this task. This is
//     obtained through the actual argument list of the task.
// ----------------------------------------------------------------------
class full_metadata;

class task_metadata : public task_data_t {
    typedef cas_mutex mutex_t;
#if !STORED_ANNOTATIONS
    typedef void (*replace_fn_t)( task_metadata *, task_metadata *, task_data_t & );
#endif

private:
    // Each task can be part of one "deps" list per argument (only in/inout).
    // Thus, we can implement this list using prev/next links in the
    // per-argument tags. Question then is how to recompute the task from
    // the tags address. Need to replicate it inside each of the tags.
    // Note that this is not worse than the std::vector<> solution, which
    // basically does the same thing.
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
    inline void create_from_pending( task_metadata * from, full_metadata * ff );
    inline void convert_to_full( full_metadata * ff );

public:
    template<typename... Tn>
    inline void create( full_metadata * ff );

public:
    taskgraph * get_graph() { return graph; }

    // Locking
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

    // Self wakeup (change of state between arg_ready() and arg_issue())
    inline void add_to_graph();

    // Ready counter
    void add_incoming() {
	__sync_fetch_and_add( &incoming_count, 1 );
	// errs() << "add incoming " << this << ": " << incoming_count << "\n";
    }
    void add_incoming( size_t n ) {
	__sync_fetch_and_add( &incoming_count, n );
	// errs() << "add incoming " << this << ": " << incoming_count << "\n";
    }
    bool del_incoming() {
	// errs() << "del incoming " << this << ": " << incoming_count << "\n";
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
    void stop_deregistration() { }
};

void
generation::link_readers( task_metadata * fr ) {
    fr->add_incoming( get_num_readers() );
}

void
generation::link_writer( task_metadata * fr ) {
    // errs() << "obj " << this << " link writer " << last_writer << " to " << fr  << '\n';
    if( writer )
	fr->add_incoming();
}

// ----------------------------------------------------------------------
// pending_metadata: task graph metadata per pending frame
// ----------------------------------------------------------------------
class pending_metadata : public task_metadata, public link_metadata {
public:
    static inline
    pending_metadata * get_from_task( task_metadata * task ) {
	return static_cast<pending_metadata *>( task );
    }
    static inline
    pending_metadata * get_from_link( link_metadata * link ) {
	return static_cast<pending_metadata *>( link );
    }
};

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
	return pending_metadata::get_from_link( graph.get_ready_task() );
    }
    pending_metadata * get_ready_task_after( task_metadata * prev ) {
	return pending_metadata::get_from_link( graph.get_ready_task() );
    }

    taskgraph * get_graph() { return &graph; }

    // void lock() { graph.lock(); }
    // void unlock() { graph.unlock(); }
};

// ----------------------------------------------------------------------
// Member function implementations with cyclic dependencies on class
// definitions.
// ----------------------------------------------------------------------
void
task_metadata::create_from_pending( task_metadata * from, full_metadata * ff ) {
    graph = ff->get_graph();
    
    // First change the task pointer for each argument's dependency list,
    // then assign all outgoing dependencies from <from> to us.
    lock();
#if STORED_ANNOTATIONS
    arg_replace_fn<egtg_metadata,task_metadata>( from, this, get_task_data() );
#else
    assert( from->replace_fn && "Don't have a replace frame function" );
    (*from->replace_fn)( from, this, get_task_data() );
#endif
    unlock();
}

void
task_metadata::convert_to_full( full_metadata * ff ) {
    graph = ff->get_graph();
    assert( graph != 0 && "create_from_pending with null graph" );
}

template<typename... Tn>
void
task_metadata::create( full_metadata * ff ) {
    graph = ff->get_graph();
#if !STORED_ANNOTATIONS
    replace_fn = &arg_replace_fn<egtg_metadata,task_metadata,Tn...>;
    // errs() << "set graph in " << this << " to " << graph << "\n";
#endif
}

void
generation::wakeup_next_writer( taskgraph * graph ) {
    if( next ) {
	// First reasoning:
	// We need to lock next to avoid races between adding a writer in
	// the current generation and properly releasing it. If we don't
	// lock next, then we might add a writer that is never woken up.
	// Second reasoning:
	// BUT, on the other hand, next is null as long as there is no
	// other writer, meaning that when our next pointer becomes non-null,
	// then the writer is already initialized (store to this->next->writer
	// is performed before store to this->next).
	// next->lock();
	if( task_metadata * t = next->get_last_writer() ) {
	    // Don't need a lock: only one task can be the last to
	    // wakeup the writer (atomic decrement of join counter)
	    // t->lock();
	    // errs() << "task " << this << " dec_incoming " << t << '\n';
	    if( t->del_incoming() ) {
		graph->add_ready_task( pending_metadata::get_from_task( t ) );
		// errs() << "task " << this << " wakes up " << t << '\n';
	    }
	    // t->unlock();
	}
	// next->unlock();
    }
}

void
generation::wakeup_readers( taskgraph * graph ) {
    bool has_readers = false;

    for( reader_iterator I=reader_begin(), E=reader_end(); I != E; ++I ) {
	task_metadata * t = *I;
	has_readers = true;
	// TODO: probably don't need to lock here because del_incoming is atomic
	// t->lock();
	// errs() << "task " << this << " dec_incoming " << t << '\n';
	if( t->del_incoming() ) {
	    graph->add_ready_task( pending_metadata::get_from_task( t ) );
	    // errs() << "task " << this << " wakes up " << t << '\n';
	}
	// t->unlock();
    }

    // If we did not have readers, we must wakeup the writer in the next 
    // generation
    if( !has_readers )
	wakeup_next_writer( graph );
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
    static
    void arg_issue( task_metadata * fr,
		   obj_instance<egtg_metadata> & obj,
		   gen_tags * sa ) {
	egtg_metadata * md = obj.get_version()->get_metadata();
	generation * gen = md->get_generation();
	gen->lock();

	// Don't add an edge to the last writer if there are last
	// readers, because that edge is redundant.
	if( gen->has_readers() ) {
	    gen->link_readers( fr );
	    sa->gen = md->push_generation( fr );
	} else if( gen->has_writers() ) {
	    gen->link_writer( fr );
	    sa->gen = md->push_generation( fr );
	} else
	    sa->gen = gen->add_writer( fr );

	gen->unlock();
    }
    static
    bool arg_ini_ready( const obj_instance<egtg_metadata> & obj ) {
	return !obj.get_version()->get_metadata()->has_readers()
	    & !obj.get_version()->get_metadata()->has_writers();
    }
    static
    void arg_release( task_metadata * fr, obj_instance<egtg_metadata> & obj,
		      gen_tags * tags ) {
	tags->gen->lock();
	tags->gen->del_writer( fr, fr->get_graph() );
	if( !tags->gen->consider_delete() )
	    tags->gen->unlock();
    }
};

// Input dependency tags
class indep_tags : public indep_tags_base, public gen_tags, public input_tags,
		   public serial_dep_tags { };

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public outdep_tags_base, public gen_tags,
		    public serial_dep_tags { };

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public inoutdep_tags_base, public gen_tags,
		      public serial_dep_tags { };

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
struct dep_traits<egtg_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj,
			  typename indep<T>::dep_tags * sa ) {
	egtg_metadata * md = obj.get_version()->get_metadata();
	generation * gen = md->get_generation();
	gen->lock();
	sa->gen = gen;

	if( gen->has_writers() )
	    gen->link_writer( fr );
	gen->add_reader( fr, sa );

	gen->unlock();
    }
    template<typename T>
    static bool arg_ini_ready( const indep<T> & obj ) {
	return !obj.get_version()->get_metadata()->has_writers();
    }
    template<typename T>
    static void arg_release( task_metadata * fr, indep<T> & obj,
			     typename indep<T>::dep_tags & sa  ) {
	sa.gen->lock();
	sa.gen->del_reader( fr, fr->get_graph(), &sa );
	if( !sa.gen->consider_delete() )
	    sa.gen->unlock();
    }
};

// output dependency traits
template<>
struct dep_traits<egtg_metadata, task_metadata, outdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, outdep<T> & obj,
			  typename outdep<T>::dep_tags * sa ) {
	// Assuming we always rename if there are outstanding readers/writers,
	// we know that there are no prior writers or readers.
	egtg_metadata * md = obj.get_version()->get_metadata();
	generation * gen = md->get_generation();
	gen->lock();
	if( gen->has_writers() || gen->has_readers() )
	    sa->gen = md->push_generation( fr );
	else
	    sa->gen = gen->add_writer( fr );
	gen->unlock();
    }
    template<typename T>
    static bool arg_ini_ready( const outdep<T> & obj ) {
	assert( v->is_versionable() ); // enforced by applicators
	return true;
    }
    template<typename T>
    static void arg_release( task_metadata * fr, outdep<T> & obj,
			     typename outdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa );
    }
};

// inout dependency traits
template<>
struct dep_traits<egtg_metadata, task_metadata, inoutdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, inoutdep<T> & obj,
			  typename inoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa );
    }
    template<typename T>
    static bool arg_ini_ready( const inoutdep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj );
    }
    template<typename T>
    static void arg_release( task_metadata * fr, inoutdep<T> & obj,
			     typename inoutdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa );
    }
};

} // end of namespace obj

#endif // EGTASKGRAPH_IMPL_H

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

/* ecgtaskgraph.h
 * This file implements an embedded generational task graph where edges
 * between tasks are explicitly maintained. They are not gathered
 * on a single dependee list per task, but they are kept on a task
 * list per generation. The scheme supports commutativity and reductions.
 */
#ifndef ECGTASKGRAPH_H
#define ECGTASKGRAPH_H

#include <cstdint>
#include <iostream>

#define EMBED_LISTS  ( OBJECT_TASKGRAPH == 7 || OBJECT_TASKGRAPH == 11 )
#define EDGE_CENTRIC ( OBJECT_TASKGRAPH == 6 || OBJECT_TASKGRAPH ==  7 )

#if EMBED_LISTS
#include "lfllist.h"
#else // !EMBED_LISTS
#include <list>
#include <vector>
#endif

#include "wf_frames.h"
#include "lock.h"
#include "padding.h"
#include "alc_allocator.h"
#include "alc_mmappol.h"
#include "alc_flpol.h"

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
// link_metadata: task graph metadata per stored frame
// ----------------------------------------------------------------------

// Each task can be part of one "deps" list per argument (only in/inout).
// Thus, we can implement this list using prev/next links in the
// per-argument tags. Question then is how to recompute the task from
// the tags address. Need to replicate it inside each of the tags.
// Note that this is not worse than the std::vector<> solution, which
// basically does the same thing.
class link_metadata {
#if EMBED_LISTS
    link_metadata * prev, * next;
    friend class dl_list_traits<link_metadata>;
#endif
};

//----------------------------------------------------------------------
// Traits for accessing elements stored in a dl_list<>, in global
// namespace.
//----------------------------------------------------------------------

} // end namespace obj because the traits class must be in global namespace

#if EMBED_LISTS
template<>
struct dl_list_traits<obj::link_metadata> {
    typedef obj::link_metadata T;
    typedef obj::link_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    static void set_prev( T * elm, T * prev ) { elm->prev = prev; }
    static T * get_prev( T const * elm ) { return elm->prev; }
    static void set_next( T * elm, T * next ) { elm->next = next; }
    static T * get_next( T const * elm ) { return elm->next; }

    static ValueType * get_value( T const * elm ) {
	return const_cast<ValueType *>( elm ); // cheat
    }
};
#endif

namespace obj { // reopen
// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
class pending_metadata;

class taskgraph {
    typedef cas_mutex mutex_t;

private:
#if EMBED_LISTS
    dl_head_list<link_metadata> ready_list;
#else // !EMBED_LISTS
    std::list<pending_metadata *> ready_list;
#endif
    mutable mutex_t mutex;

public:
    ~taskgraph() {
	assert( ready_list.empty() && "Pending tasks at destruction time" );
    }

    inline pending_metadata * get_ready_task();
    inline void add_ready_task( link_metadata * fr );

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

// ----------------------------------------------------------------------
// gen_tags: tag storage for all dependency usage types
// ----------------------------------------------------------------------
class task_metadata;
class generation;

// gen_tags stores the accessed generation and links the task on the task list.
// Required for all types expect for queues
class gen_tags {
#if EMBED_LISTS
    gen_tags * it_prev, * it_next;
    task_metadata * st_task;
#endif
    generation * gen;

    friend class serial_dep_traits;
    template<typename MetaData, typename Task,
	     template<typename U> class DepTy>
    friend class dep_traits;
    friend class generation;
#if EMBED_LISTS
    friend class dl_list_traits<gen_tags>;
#endif
};

} // end namespace obj because the traits class must be in global namespace

#if EMBED_LISTS
template<>
struct dl_list_traits<obj::gen_tags> {
    typedef obj::gen_tags T;
    typedef obj::task_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    static void set_prev( T * elm, T * prev ) { elm->it_prev = prev; }
    static T * get_prev( T const * elm ) { return elm->it_prev; }
    static void set_next( T * elm, T * next ) { elm->it_next = next; }
    static T * get_next( T const * elm ) { return elm->it_next; }

    static ValueType * get_value( T const * elm ) { return elm->st_task; }
};
#endif

namespace obj { // reopen

// ----------------------------------------------------------------------
// queue_tags: tag storage for queue dependency usage types
// ----------------------------------------------------------------------
class task_metadata;
class generation;

// queue_tags stores the accessed generation and links the task on the task list.
// Required for queue types 
class queue_tags {
    task_metadata * st_task;
    queue_tags * qt_next;

    friend class queue_metadata;
};


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
    typedef cas_mutex_v<uint8_t> mutex_t;

private:
    generation * next;
#if EMBED_LISTS
    dl_head_list<gen_tags> tasks; // set of readers after the writer
#else // !EMBED_LISTS
    std::vector<task_metadata *> tasks; // set of readers after the writer
#endif
    size_t num_tasks;
    group_t group;
    bool may_free;
    mutex_t mutex;

    pad_multiple<64, sizeof(void *)
#if EMBED_LISTS
		 + sizeof(dl_head_list<gen_tags>)
#else // !EMBED_LISTS
		 + sizeof(std::vector<task_metadata *>)
#endif
		 + sizeof(size_t) + sizeof(group_t) + sizeof(bool)
		 + sizeof(mutex_t)> padding;

protected:
    friend class ecgtg_metadata;

    generation() : next( 0 ), num_tasks( 0 ), group( g_NUM ), may_free( false ) {
	static_assert( sizeof( generation ) % 64 == 0, "Alignment failed" );
	// errs() << "create initial generation " << this << "\n";
    }
    generation( group_t g )
	: next( 0 ), num_tasks( 0 ), group( g ), may_free( false ) {
	// errs() << "create generation " << this << "\n";
    }
    ~generation() {
	// errs() << "delete generation " << this << "\n";
	assert( next && "ecgtg_metadata must not link to us" );
	assert( !has_tasks()
		&& "Must have zero tasks when destructing ecgtg_metadata" );
    }

    inline void * operator new ( size_t size );
    inline void operator delete( void * p );

public:
    // Deallocation
    void del_ref() {
	lock();
	if( may_free )
	    delete this;
	else {
	    may_free = true;
	    unlock();
	}
    }
    bool consider_delete() {
	if( !has_tasks() ) {
	    if( may_free ) {
		delete this;
		return true;
	    } else if( next )
		may_free = true;
	}
	return false;
    }

    // Access to generation
    generation * get_next_generation() { return next; }
    void set_next_generation( generation * g ) { next = g; }

    // Dependency queries on readers
#if EMBED_LISTS
    typedef dl_head_list<gen_tags>::const_iterator task_iterator;
#else // !EMBED_LISTS
    typedef std::vector<task_metadata *>::const_iterator task_iterator;
#endif
    task_iterator task_begin() const { return tasks.begin(); }
    task_iterator task_end() const { return tasks.end(); }

    // Register users of this object.
    void add_task( task_metadata * t, gen_tags * tags, bool insert ) {
	// errs() << "gen " << this << " add task " << t << "\n";
#if EMBED_LISTS
	if( insert ) {
	    tags->st_task = t;
	    tasks.push_back( tags );
	}
#else // !EMBED_LISTS
	if( insert )
	    tasks.push_back( t );
#endif
	++num_tasks;
    }

    // Erase links if we are about to destroy a task
    void del_task( task_metadata * fr, taskgraph * graph,
		   gen_tags * tags ) {
	// Never delete a task: we blow away the list of tasks in a generation
	// as soon as it becomes the head of the (argument's) task graph
	--num_tasks;
	// errs() << "gen " << this << " del task " << fr << " #" << num_tasks << " left\n";
#if EDGE_CENTRIC
	if( next )
	    next->wakeup( graph, num_tasks == 0 );
#else // !EDGE_CENTRIC
	if( next && num_tasks == 0 )
	    next->wakeup( graph, true );
#endif
    }

    // Dependency queries on readers and writer
    bool has_tasks() const { return get_num_tasks() != 0; }
    size_t get_num_tasks() const { return num_tasks; }

    // Link to readers
    inline void link_tasks( task_metadata * fr );

    // Waking up dependents - only for a stack_frame
    inline void wakeup( taskgraph * graph, bool clear );

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

extern __thread generation_allocator * tls_ecgtg_allocator;

void * generation::operator new ( size_t size ) {
    if( !tls_ecgtg_allocator )
	tls_ecgtg_allocator = new generation_allocator();
    return tls_ecgtg_allocator->allocate( 1 );
}
void generation::operator delete( void * p ) {
    if( !tls_ecgtg_allocator ) // we may deallocate blocks allocated elsewhere
	tls_ecgtg_allocator = new generation_allocator();
    return tls_ecgtg_allocator->deallocate( (generation *)p, 1 );
}

// ----------------------------------------------------------------------
// ecgtg_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class ecgtg_metadata {
    typedef cas_mutex_v<uint8_t> mutex_t;

private:
    // Every task can be on one readers list for every in/inout
    // dependency that it has. Embed this list in the tags with next/prev
    // links and a pointer to the task.
    generation * prev, * gen;
    // mutex_t mutex;
    mutex_t cmutex;

public:
    ecgtg_metadata() : prev( 0 ), gen( new generation() ) {
	// errs() << "ecgtg_metadata create: " << this << "\n";
    }
    ~ecgtg_metadata() {
	// errs() << "ecgtg_metadata delete: " << this << "\n";
	if( prev ) {
	    prev->lock();
	    prev->set_next_generation( (generation *)1 );
	    if( !prev->consider_delete() )
		prev->unlock();
	}
	gen->lock();
	gen->set_next_generation( (generation *)1 );
	if( !gen->consider_delete() )
	    gen->unlock();
    }

public:
    // External inferface
    bool rename_is_active() const { return gen->has_tasks(); }
    bool rename_has_readers() const { return gen->has_tasks(); }
    bool rename_has_writers() const { return gen->has_tasks(); }

    generation * get_prev_generation() { return prev; }
    generation * get_generation() { return gen; }

    // Register users of this object.
    bool new_group( group_t g ) const {
	return g == g_write || gen->group != g;
    }
    void open_group( group_t g ) {
	if( new_group( g ) && gen->has_tasks() ) {
	    if( prev ) prev->del_ref();
	    prev = gen;
	    gen = new generation( g );
	    prev->set_next_generation( gen );
	} else // if( gen->group != g )
	    gen->group = g;
    }
    void force_group() { // only intended for outdep, so g == g_write
	assert( !gen->has_tasks() );
	gen->group = g_write;
    }
    bool match_group( group_t g ) const {
	// An empty gen implies always an empty prev
	// Thus, applying distribution allows us to simplify
	// the last term
	// return ( !new_group( g ) || !gen->has_tasks() )
	    // && ( !prev || !prev->has_tasks() );
	return ( !new_group( g ) && ( !prev || !prev->has_tasks() ) ) || !gen->has_tasks();
    }

    // Dependency queries on current generation
    bool has_readers() const { return gen->has_tasks(); }
    bool has_writers() const { return gen->has_tasks(); }

#if OBJECT_COMMUTATIVITY
    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return cmutex.try_lock(); }
    void commutative_release() { cmutex.unlock(); }
#endif

    // Locking
    // void lock() { mutex.lock(); }
    // void unlock() { mutex.unlock(); }
};

// Some debugging support. Const-ness of printed argument is a C++ library
// requirement, but we want to keep the lock as non-const.
inline std::ostream &
operator << ( std::ostream & os, const ecgtg_metadata & md_const ) {
    ecgtg_metadata & md = *const_cast<ecgtg_metadata *>( &md_const );
    generation * gen = md.get_generation();
    gen->lock();
    os << "taskgraph_md={gen=" << gen << ", dep_tasks=";
    for( generation::task_iterator
	     I=gen->task_begin(), E=gen->task_end(); I != E; ++I )
	os << *I << ", ";
    gen->unlock();
    os << '}';
    return os;
}

// ----------------------------------------------------------------------
// queue_metadata: dependency-tracking metadata for queues
// ----------------------------------------------------------------------
class queue_metadata {
    typedef cas_mutex_v<uint8_t> mutex_t;

    queue_tags * youngest;
    mutex_t mutex;

private:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

public:
    queue_metadata() : youngest( 0 ) { }
    ~queue_metadata() {
	assert( !youngest && "Must have finished all tasks when "
		"destructing queue_metadata" );
    }

    // Register users of this queue.
    inline void add_task( task_metadata * t, queue_tags * tags );

    // Erase links if we are about to destroy a task
    inline void del_task( task_metadata * fr, taskgraph * graph,
			  queue_tags * tags );

    bool has_tasks() const { return youngest != 0; }
};

// ----------------------------------------------------------------------
// task_metadata: dependency-tracking metadata for tasks (pending and stack)
//     We require the list of objects that are held by this task. This is
//     obtained through the actual argument list of the task.
// ----------------------------------------------------------------------
class full_metadata;

class task_metadata : public task_data_t {
    typedef cas_mutex mutex_t;
#if !STORED_ANNOTATIONS
    typedef bool (*acquire_fn_t)( task_data_t & );
#endif

private:
    taskgraph * graph;
#if !STORED_ANNOTATIONS
    acquire_fn_t acquire_fn;
#endif
    size_t incoming_count;
    mutex_t mutex;

protected:
    // Default constructor
    task_metadata() : graph( 0 ),
#if !STORED_ANNOTATIONS
		      acquire_fn( 0 ),
#endif
		      incoming_count( 0 ) {
	// errs() << "task_metadata create: " << this << '\n';
    }
    ~task_metadata() {
	// errs() << "task_metadata delete: " << this << '\n';
    }
    // Constructor for creating stack frame from pending frame
    inline void create_from_pending( task_metadata * from, full_metadata * ff );
    // Constructor for converting stack to full frame.
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

    // Acquire mutual exclusion for commutativity
    bool acquire() {
#if STORED_ANNOTATIONS
	return arg_acquire_fn<ecgtg_metadata>( get_task_data() );
#else
	assert( acquire_fn && "Don't have an acquire frame function" );
	return (*acquire_fn)( get_task_data() );
#endif
    }

    // Ready counter
    void add_incoming() {
	__sync_fetch_and_add( &incoming_count, 1 );
	// errs() << "add incoming " << this << ": " << incoming_count << "\n";
    }
    void add_incoming( size_t n ) {
	__sync_fetch_and_add( &incoming_count, n );
	// errs() << "add incoming N " << this << ": " << incoming_count << "\n";
    }
    bool del_incoming() {
	// errs() << "del incoming " << this << ": " << incoming_count << "\n";
	return __sync_fetch_and_add( &incoming_count, -1 ) == 1;
    }
    // size_t get_incoming() const volatile { return incoming_count; }

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
generation::link_tasks( task_metadata * fr ) {
    // errs() << "link task " << fr << " " << get_num_tasks() << " inc\n";
#if EDGE_CENTRIC
    fr->add_incoming( get_num_tasks() );
#else // !EDGE_CENTRIC
    if( has_tasks() )
	fr->add_incoming( 1 );
#endif
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
// Member function implementations with cyclic dependencies on class
// definitions.
// ----------------------------------------------------------------------
void
task_metadata::create_from_pending( task_metadata * from, full_metadata * ff ) {
    graph = ff->get_graph();
    // We don't need to change the task pointer in the generation's task list
    // because we throw the list away as soon as it wakes up.
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
    acquire_fn = &arg_acquire_fn<ecgtg_metadata,Tn...>;
    // errs() << "set graph in " << this << " to " << graph << "\n";
#endif
}

pending_metadata *
taskgraph::get_ready_task() {
    lock();
    pending_metadata * task = 0;
    if( !ready_list.empty() ) {
	for( auto I=ready_list.begin(), E=ready_list.end(); I != E; ++I ) {
#if EMBED_LISTS
	    pending_metadata * t = pending_metadata::get_from_link( *I );
#else // !EMBED_LISTS
	    pending_metadata * t = *I;
#endif
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
taskgraph::add_ready_task( link_metadata * fr ) {
    // Translate from task_metadata to link_metadata
    // Can only be successfully done in case of queued_frame!
    // errs() << "add_ready_task to TG " << this << ": " << fr << "\n";
    lock();
#if EMBED_LISTS
    ready_list.push_back( fr );
#else // !EMBED_LISTS
    ready_list.push_back( pending_metadata::get_from_link( fr ) );
#endif
    unlock();
}

void
generation::wakeup( taskgraph * graph, bool clear ) {
    // Lock the generation because we may be waking up tasks in the same
    // generation as the one we are adding to.
    lock();
    for( task_iterator I=task_begin(), E=task_end(); I != E; ++I ) {
	task_metadata * t = *I;
	// TODO: probably don't need to lock here because del_incoming is atomic
	// t->lock();
	// errs() << "task " << t << " dec_incoming " << t->get_incoming() << '\n';
	if( t->del_incoming() ) {
	    graph->add_ready_task( pending_metadata::get_from_task( t ) );
	    // errs() << "task " << this << " wakes up " << t << '\n';
	}
	// t->unlock();
    }
    if( clear )
	tasks.clear();
    unlock();
}

void
task_metadata::add_to_graph() {
    graph->add_ready_task( pending_metadata::get_from_task( this ) );
}

void queue_metadata::add_task( task_metadata * t, queue_tags * tags ) {
    lock();
    tags->st_task = t;
    tags->qt_next = 0;
    if( youngest ) {
	t->add_incoming( 1 );
	youngest->qt_next = tags;
    }
    youngest = tags;
    unlock();
}

void queue_metadata::del_task( task_metadata * fr, taskgraph * graph,
			       queue_tags * tags ) {
    lock();
    if( youngest == tags )
	youngest = 0;
    unlock();
    if( queue_tags * nt = tags->qt_next ) {
	// By constructing, nt->st_task is a pending frame. When selecting
	// the task, erase it from the tags because it will changed from
	// a pending frame to a stack frame, which invalidates the pointer.
	task_metadata * t = nt->st_task;
	if( t->del_incoming() )
	    graph->add_ready_task( pending_metadata::get_from_task(t) );
	nt->st_task = 0;
    }
}

// ----------------------------------------------------------------------
// Dependency handling traits
// ----------------------------------------------------------------------
// Whole-function dependency tags
class function_tags : public function_tags_base { };

// A fully serialized version
class serial_dep_tags { };

struct serial_dep_traits {
    static
    void arg_issue( task_metadata * fr,
		    obj_instance<ecgtg_metadata> & obj,
		    gen_tags * sa, group_t g ) {
	ecgtg_metadata * md = obj.get_version()->get_metadata();
	md->open_group( g );
	// We need to be carefull to avoid locking the previous
	// generation: we don't want to add a task that will
	// never get woken up. Adding a task has two steps:
	// atomic increment (link_tasks) and insert in list (add_task)
	// Wakeup is one step: traverse gen list and atomic dec.
	// Both code sequences are encapsulated inside the gen->lock()
	// Precision:
	// issue = read prev->#tasks and atomic inc #incoming (link_tasks)
	//       + insert in gen->list (add_task)
	// release = decrement prev->#tasks
	//         + walk gen->list
	// issue = [R - A - I]g
	// release = [D - [W]g]p
	// Erroneous behavior would be:
	// * R - A - I - D - W: ok, no interleaving
	// * R - A - D - I - W: ok, I and D commute
	// * R - D - A - I - W: ok, A and D commute, I and D commute
	// * D - R - A - I - W: wakeup too early (#incoming is one too small) !!
	// * R - A - D - W - I: impossible by shared lock on gen
	// * R - D - A - W - I: impossible by shared lock on gen
	// * D - R - A - W - I: impossible by shared lock on gen
	// * R - D - W - A - I: impossible by shared lock on gen
	// * D - R - W - A - I: impossible by shared lock on gen
	// * D - W - R - A - I: ok, no interleaving
	// Note: D is not atomic, so R may also interleave inside D, which means
	// that these cases are problematic:
	// * R - D - A - I - W
	// * D - R - A - I - W
	// If we make the decrement in D atomic, then we are left with only:
	// * D - R - A - I - W: wakeup too early (#incoming is one too small) !!
	// So we can change release:
	// issue = [R - A - I]g
	// release = [[W]g - D]p, assuming D is atomic
	// * R - A - I - W - D: ok, no interleaving
	// * R - A - W - I - D: impossible by shared lock on gen
	// * R - W - A - I - D: impossible by shared lock on gen
	// * W - R - A - I - D: #incoming is one too large
	// * R - A - W - D - I: impossible by shared lock on gen
	// * R - W - A - D - I: impossible by shared lock on gen
	// * W - R - A - D - I: #incoming is one too large
	// * R - W - D - A - I: impossible by shared lock on gen
	// * W - R - D - A - I: #incoming is one too large
	// * W - D - R - A - I: ok, no interleaving
	// I don't see any option but to lock both prev and gen during issue.
	// Note that non-overlapping exclusive regions is not good enough:
	// issue = [R - A]p - [I]g
	// release = [D - [W]g]p
	// Because of:
	// * R - A - D - W - I: #incoming is appropriate but task not found
	// during wakeup (W before I); or #incoming is one too large
	// Check this:
	// issue = [p[g R - A]p - I]g (or [I]g if there is no previous task)
	// release = [D - [W]g]p
	// Ok - guarded by two locks
	generation * gen = md->get_generation();
	if( generation * prv = md->get_prev_generation() ) {
	    prv->lock(); // lock order: prev before gen
	    gen->lock();
	    prv->link_tasks( fr );
	    gen->add_task( fr, sa, prv->has_tasks() );
	    gen->unlock();
	    prv->unlock();
	} else {
	    gen->lock();
	    gen->add_task( fr, sa, false );
	    gen->unlock();
	}
	sa->gen = gen;
    }
    static
    bool arg_ini_ready( const obj_instance<ecgtg_metadata> & obj, group_t g ) {
	const ecgtg_metadata * md = obj.get_version()->get_metadata();
	return md->match_group( g );
    }
    static
    void arg_release( task_metadata * fr, obj_instance<ecgtg_metadata> & obj,
		      gen_tags * tags, group_t g ) {
	tags->gen->lock();
	tags->gen->del_task( fr, fr->get_graph(), tags );
	if( !tags->gen->consider_delete() )
	    tags->gen->unlock();
    }
};

// We could simplify queue dependence traits to a linked list of popdeps.
// Everything else could be sequential. We don't need to use the same
// metadata, but can specialize it. We can do this even once in a way
// that applies to all taskgraph schemes.
struct queue_dep_traits {
    static
    void arg_issue( task_metadata * fr, queue_metadata * md,
		    queue_tags * tags ) {
	md->add_task( fr, tags );
    }
    static
    bool arg_ini_ready( const queue_metadata * md ) {
	return !md->has_tasks();
    }
    static
    void arg_release( task_metadata * fr, queue_metadata * md,
		      queue_tags * tags ) {
	md->del_task( fr, fr->get_graph(), tags );
    }
};

// Input dependency tags
class indep_tags : public indep_tags_base, public gen_tags,
		   public serial_dep_tags { };

// Output dependency tags require fully serialized tags in the worst case
class outdep_tags : public outdep_tags_base, public gen_tags,
		    public serial_dep_tags { };

// Input/output dependency tags require fully serialized tags
class inoutdep_tags : public inoutdep_tags_base, public gen_tags,
		      public serial_dep_tags { };

#if OBJECT_COMMUTATIVITY
// Commutative input/output dependency tags
class cinoutdep_tags : public cinoutdep_tags_base, public gen_tags,
		       public serial_dep_tags { };
#endif

#if OBJECT_REDUCTION
// Reduction dependency tags
class reduction_tags : public reduction_tags_base<ecgtg_metadata>,
		       public gen_tags, public serial_dep_tags { };
#endif

// Popdep (input) dependency tags - fully serialized with other pop and pushpop
class popdep_tags : public popdep_tags_base<queue_metadata>,
		    public queue_tags,
		    public serial_dep_tags {
public:
    popdep_tags( queue_version<queue_metadata> * parent )
	: popdep_tags_base( parent ) { }
};

// Pushpopdep (input/output) dependency tags - fully serialized with other
// pop and pushpop
class pushpopdep_tags : public pushpopdep_tags_base<queue_metadata>,
			public queue_tags,
			public serial_dep_tags {
public:
    pushpopdep_tags( queue_version<queue_metadata> * parent )
	: pushpopdep_tags_base( parent ) { }
};

// Pushdep (output) dependency tags
class pushdep_tags : public pushdep_tags_base<queue_metadata>,
		     public serial_dep_tags {
public:
    pushdep_tags( queue_version<queue_metadata> * parent )
	: pushdep_tags_base( parent ) { }
};

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
struct dep_traits<ecgtg_metadata, task_metadata, indep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, indep<T> & obj,
			  typename indep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_read );
    }
    template<typename T>
    static bool arg_ini_ready( const indep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj, g_read );
    }
    template<typename T>
    static void arg_release( task_metadata * fr, indep<T> & obj,
			     typename indep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_read );
    }
};

// output dependency traits
template<>
struct dep_traits<ecgtg_metadata, task_metadata, outdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, outdep<T> & obj,
			   typename outdep<T>::dep_tags * sa ) {
	// serial_dep_traits::arg_issue( fr, obj, sa, g_write );
	assert( obj.get_version()->is_versionable() ); // enforced by applicator
	ecgtg_metadata * md = obj.get_version()->get_metadata();
	md->force_group(); // assume task lists are empty
	generation * gen = md->get_generation();
	// gen->lock(); -- no lock required, we are the first and only so far
	gen->add_task( fr, sa, false );
	// gen->unlock();
	sa->gen = gen;
    }
    template<typename T>
    static bool arg_ini_ready( const outdep<T> & obj ) {
	assert( obj.get_version()->is_versionable() ); // enforced by applicator
	return true;
    }
    template<typename T>
    static void arg_release( task_metadata * fr, outdep<T> & obj,
			     typename outdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_write );
    }
};

// inout dependency traits
template<>
struct dep_traits<ecgtg_metadata, task_metadata, inoutdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, inoutdep<T> & obj,
			  typename inoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_write );
    }
    template<typename T>
    static bool arg_ini_ready( const inoutdep<T> & obj ) {
	return serial_dep_traits::arg_ini_ready( obj, g_write );
    }
    template<typename T>
    static void arg_release( task_metadata * fr, inoutdep<T> & obj,
			     typename inoutdep<T>::dep_tags & sa  ) {
	serial_dep_traits::arg_release( fr, obj, &sa, g_write );
    }
};

#if OBJECT_COMMUTATIVITY
template<>
struct dep_traits<ecgtg_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, cinoutdep<T> & obj,
	       typename cinoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_commut );
    }
    template<typename T>
    static bool
    arg_ini_ready( cinoutdep<T> & obj ) {
	ecgtg_metadata * md = obj.get_version()->get_metadata();
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
	obj.get_version()->get_metadata()->commutative_release();
	serial_dep_traits::arg_release( fr, obj, &sa, g_commut );
    }
};
#endif

#if OBJECT_REDUCTION
template<>
struct dep_traits<ecgtg_metadata, task_metadata, reduction> {
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

// queue pop dependency traits
template<>
struct dep_traits<queue_metadata, task_metadata, popdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, popdep<T> & obj,
	       typename popdep<T>::dep_tags * tags ) {
	queue_metadata * md = obj.get_version()->get_metadata();
	queue_dep_traits::arg_issue( fr, md, tags );
    }
    template<typename T>
    static bool
    arg_ini_ready( const popdep<T> & obj ) {
	const queue_metadata * md = obj.get_version()->get_metadata();
	return queue_dep_traits::arg_ini_ready( md );
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, popdep<T> & obj,
		 typename popdep<T>::dep_tags & tags  ) {
	queue_metadata * md = obj.get_version()->get_metadata();
	queue_dep_traits::arg_release( fr, md, &tags );
    }
};

// queue push dependency traits
template<>
struct dep_traits<queue_metadata, task_metadata, pushdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, pushdep<T> & obj,
			   typename pushdep<T>::dep_tags * sa ) {
    }
    template<typename T>
    static bool arg_ini_ready( const pushdep<T> & obj ) {
	return true;
    }
    template<typename T>
    static void arg_release( task_metadata * fr, pushdep<T> & obj,
			     typename pushdep<T>::dep_tags & sa  ) {
    }
};

typedef ecgtg_metadata obj_metadata;
typedef ecgtg_metadata token_metadata;


} // end of namespace obj

#endif // ECGTASKGRAPH_H

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

/* ecltaskgraph.h
 * This file implements an embedded generational task graph where edges
 * between tasks are explicitly maintained. They are not gathered
 * on a single dependee list per task, but they are kept on a task
 * list per generation. The scheme supports commutativity and reductions.
 */
#ifndef ECLTASKGRAPH_H
#define ECLTASKGRAPH_H

#include <cstdint>
#include <iostream>

#if ( OBJECT_TASKGRAPH != 13 && OBJECT_TASKGRAPH != 14 )
#error This file should only be sourced for OBJECT_TASKGRAPH in {13,14}
#endif

#define EMBED_LISTS  ( OBJECT_TASKGRAPH == 14 )

// Per-object users list through gen_tags is currently only supported
// as an embedded list.
#define EMBED_USER_LIST 1

#if EMBED_LISTS || EMBED_USER_LIST
#include "lfllist.h"
#include <list>
#else // !EMBED_LISTS
#include <list>
#include <vector>
#endif

#include "swan/wf_frames.h"
#include "swan/lock.h"
#include "swan/padding.h"
#include "swan/alc_allocator.h"
#include "swan/alc_mmappol.h"
#include "swan/alc_flpol.h"
#include "swan/queue/taskgraph.h"
#include "swan/taskgraph/ready_list_tg.h"
#include "swan/functor/acquire.h"

namespace obj {

// ----------------------------------------------------------------------
// Auxiliary type
// ----------------------------------------------------------------------
enum group_t {
    g_empty = 0,
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
    case g_empty: return os << "empty";
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

#if EMBED_LISTS
typedef ::taskgraph<pending_metadata, link_metadata,
		    dl_head_list<link_metadata> > taskgraph;
#else
typedef ::taskgraph<pending_metadata, pending_metadata,
		    std::list<obj::pending_metadata *> > taskgraph;
#endif

// ----------------------------------------------------------------------
// gentags: tag storage for all dependency usage types
// ----------------------------------------------------------------------
class task_metadata;
class ecltg_metadata;

// gen_tags stores the accessed generation and links the task on the task list.
// Required for all types
class gen_tags {
#if EMBED_USER_LIST
    gen_tags * it_next;
    task_metadata * st_task;
#endif
    size_t last_in_gen; // OPT: bool, size_t for alignment

    void clr_last_in_generation() { last_in_gen = false; }
    void set_last_in_generation() { last_in_gen = true; }
    bool is_last_in_generation() const { return last_in_gen; }

    friend class serial_dep_traits;
    template<typename MetaData, typename Task, template<typename U> class DepTy>
    friend class dep_traits;
    friend class ecltg_metadata;
#if EMBED_USER_LIST
    friend class sl_list_traits<gen_tags>;
#endif
};

} // end namespace obj because the traits class must be in global namespace

#if EMBED_USER_LIST
template<>
struct sl_list_traits<obj::gen_tags> {
    typedef obj::gen_tags T;
    typedef obj::task_metadata ValueType;

    // not implemented -- static size_t get_depth( T * elm );
    // not implemented -- static bool is_ready( T * elm );

    // static void set_prev( T * elm, T * prev ) { elm->it_prev = prev; }
    // static T * get_prev( T const * elm ) { return elm->it_prev; }
    static void set_next( T * elm, T * next ) { elm->it_next = next; }
    static T * get_next( T const * elm ) { return elm->it_next; }

    static ValueType * get_value( T const * elm ) { return elm->st_task; }
};
#endif

namespace obj { // reopen

// ----------------------------------------------------------------------
// ecltg_metadata: dependency-tracking metadata (not versioning)
// ----------------------------------------------------------------------
class ecltg_metadata {
    typedef cas_mutex_v<uint8_t> mutex_t;

private:
    struct Oldest {
	size_t num_tasks;
	mutex_t mutex;

	Oldest() : num_tasks( 0 ) { }

	bool has_tasks() const { return num_tasks > 0; }

	void lock() { mutex.lock(); }
	void unlock() { mutex.unlock(); }
    };
    struct Youngest {
	group_t g;
	// bool some_tasks;
	mutex_t mutex;

	// Youngest() : g( g_NUM ), some_tasks( 0 ) { }
	Youngest() : g( g_empty ) { }

	// bool new_group( group_t grp ) const {
	    // TODO: Optimize by representing group_t as bit_mask?
	    // return ( grp == g_write || g != grp ) && g != g_NUM;
	// }
	bool match_group( group_t grp ) const {
	    // This condition is markedly simpler than in other task graphs
	    // because we reset the group type to empty (g_empty) whenever
	    // the youngest generation runs empty.
	    return ( grp != g_write && g == grp ) || g == g_empty;
	    // return ( g != g_write ) & ( (g == grp) | (g == g_empty) );
	    // return !new_group( grp ) || !has_tasks();
	}
	void open_group( group_t grp ) { g = grp; }

	void add_task() { } // some_tasks = true; }
	void clr_tasks() { g = g_empty; } // some_tasks = false; }
	bool has_tasks() const { return g != g_empty; } // return some_tasks; }

	void lock() { mutex.lock(); }
	void unlock() { mutex.unlock(); }
    };

    Oldest oldest;
    // pad_multiple<CACHE_ALIGNMENT, sizeof(Oldest)> padding;
    Youngest youngest;
    size_t num_gens;

#if EMBED_USER_LIST
    typedef sl_list<gen_tags> task_list_type;
#else // !EMBED_USER_LIST
    typedef std::vector<task_metadata *> task_list_type;
#endif
    task_list_type tasks; // set of readers after the writer

    // To implement commutativity
    mutex_t cmutex;

    friend std::ostream &
    operator << ( std::ostream & os, const ecltg_metadata & md_const );

public:
    ecltg_metadata() : num_gens( 0 ) {
	// errs() << "ecltg_metadata create: " << this << "\n";
    }
    ~ecltg_metadata() {
	// errs() << "ecltg_metadata delete: " << this << "\n";
	assert( !has_tasks()
		&& "delete ecltg_metadata while tasks are still pending" );
    }

public:
    // External inferface
    bool rename_is_active() const { return has_tasks(); }
    bool rename_has_readers() const { return has_tasks(); }
    bool rename_has_writers() const { return has_tasks(); }
    bool has_tasks() const {
	// return oldest.has_tasks() || youngest.has_tasks();
	return num_gens > 0;
    }

    // Dependency queries on current generation
    bool has_readers() const { return num_gens > 0; } // youngest.has_tasks(); }
    bool has_writers() const { return num_gens > 0; } // youngest.has_tasks(); }

    // This is really a ready check: can we launch with the previous gang?
    bool match_group( group_t grp ) const {
	return ( num_gens == 1 && youngest.match_group( grp ) )
	    || num_gens == 0;
    }

    // Dependency queries on readers
#if EMBED_USER_LIST
    typedef sl_list<gen_tags>::const_iterator task_iterator;
#else // !EMBED_USER_LIST
    typedef std::vector<task_metadata *>::const_iterator task_iterator;
#endif
    task_iterator task_begin() const { return tasks.begin(); }
    task_iterator task_end() const { return tasks.end(); }

    inline void wakeup( taskgraph * graph );
    inline void add_task( task_metadata * t, gen_tags * tags, group_t g );

#if OBJECT_COMMUTATIVITY
    // There is no lock operation - because there is no reason to wait...
    bool commutative_try_acquire() { return cmutex.try_lock(); }
    void commutative_release() { cmutex.unlock(); }
#endif

private:
    bool may_interfere() const volatile { return num_gens <= 2; }
    size_t pop_generation() volatile {
	assert( num_gens > 0 && "pop when no generations exist" );
	return __sync_fetch_and_add( &num_gens, -1 );
    }
    void push_generation() volatile { __sync_fetch_and_add( &num_gens, 1 ); }

    // Locking
    // void lock() { mutex.lock(); }
    // void unlock() { mutex.unlock(); }
};

// Some debugging support. Const-ness of printed argument is a C++ library
// requirement, but we want to keep the lock as non-const.
inline std::ostream &
operator << ( std::ostream & os, const ecltg_metadata & md_const ) {
    ecltg_metadata & md = *const_cast<ecltg_metadata *>( &md_const );
    os << "taskgraph_md={this=" << &md_const
       << " o.num_tasks=" << md.oldest.num_tasks
       << " y.g=" << md.youngest.g
       << " y.has=" << md.youngest.has_tasks()
       << " num_gens=" << md.num_gens
       << " front=" << md.tasks.front()
       << " back=" << md.tasks.back()
       << '}';
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
	return arg_acquire_fn<ecltg_metadata>( get_task_data() );
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
    void stop_deregistration( full_metadata * parent ) { }
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
    static inline
    pending_metadata * get_from_link( link_metadata * link ) {
	return static_cast<pending_metadata *>( link );
    }
};

// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
} // end namespace obj

#if EMBED_LISTS
template<>
struct taskgraph_traits<obj::pending_metadata, obj::link_metadata, dl_head_list<obj::link_metadata> > {
    typedef obj::pending_metadata task_type;
    typedef obj::link_metadata stored_task_type;

    static obj::pending_metadata * get_task( obj::link_metadata * lnk ) {
	return obj::pending_metadata::get_from_link( lnk );
    }

    static bool acquire( obj::pending_metadata * pnd ) {
	return pnd->acquire();
    }
};
#else
template<>
struct taskgraph_traits<obj::pending_metadata, obj::pending_metadata,
			std::list<obj::pending_metadata *> > {
    typedef obj::pending_metadata task_type;
    typedef obj::link_metadata stored_task_type;

    static obj::pending_metadata * get_task( obj::pending_metadata * lnk ) {
	return lnk;
    }

    static bool acquire( obj::pending_metadata * pnd ) {
	return pnd->acquire();
    }
};
#endif

namespace obj { // reopen

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
    acquire_fn = &arg_acquire_fn<ecltg_metadata,Tn...>;
    // errs() << "set graph in " << this << " to " << graph << "\n";
#endif
}

void
ecltg_metadata::wakeup( taskgraph * graph ) {
    // Are we holding a lock on youngest?
    bool has_youngest = false;

    oldest.lock();

    // Decrement the number of tasks in the oldest generation.
    // NOTE: performance optimization: atomic-- is more expensive than atomic++
    // while only one of the two directions must be a hardware atomic. Hence,
    // change the counter to count negative number of tasks.
    if( --oldest.num_tasks > 0 ) {
	oldest.unlock();
	return;
    }

    // Specialized case. Race on num_gens as we don't have a lock
    // on youngest yet, but num_gens can go up only.
    if( num_gens == 1 ) {
	has_youngest = true;
	youngest.lock();
	if( num_gens == 1 ) {
	    assert( !tasks.front() && "tasks should be empty if 1 generation" );
	    pop_generation();
	    youngest.clr_tasks();

	    youngest.unlock();
	    oldest.unlock();
	    return;
	}
    }

    // Lock only the wakeup end of the list if free of interference, or
    // lock both ends of the list if there may be interference between
    // the issue thread and wakeup threads. There is a race on the num_gens
    // variable, but as we are holding the oldest lock, the num_gens
    // can only go up between the call to may_interfere() and taking the
    // lock, so we are safe, perhaps taking the lock when not striclty
    // necessary.
    else if( may_interfere() ) {
	has_youngest = true;
	youngest.lock();
    }

    gen_tags * head = tasks.front();
    assert( (head || has_youngest) && "youngest not locked but list empty" );
    gen_tags * i_next = 0;
    unsigned new_tasks = 0;
    for( gen_tags * i=head; i; i=i_next ) {
	task_metadata * t = i->st_task;
	++new_tasks;
	if( t->del_incoming() )
	    graph->add_ready_task( pending_metadata::get_from_task( t ) );
	i_next = i->it_next;
	if( i->is_last_in_generation() )
	    break;
    }

    // A store is sufficient because all ready tasks have finished execution
    // (checked by the atomic decrement). Therefore, there cannot be races
    // on num_tasks.
    // assert( oldest.num_tasks == 0 && "num_tasks changed during wakeup" );
    // oldest.num_tasks = new_tasks;
    // WRONG: A store is not sufficient because when num_generations==1,
    // then a task issue may occur between the decrement of oldest.num_tasks
    // to 0 and obtaining both young and old locks. When this happens, we will
    // not see that oldest.num_tasks is still zero.
    oldest.num_tasks += new_tasks;

    // --num_gens; protected by lock on both sides?
    // errs() << "flush..." << std::endl;
    bool empty = pop_generation() == 1; // Down to 0 from 1.
    assert( empty == !num_gens && "empty iff num_gens==0" );

    if( i_next ) {
	assert( !empty && "Zero generations but still tasks in youngest" );
	tasks.fastforward( i_next );
    } else {
	// Two possibilities:
	// - empty, or num_gens==0: no more tasks in system, num_tasks==0
	//                          and youngest.some_tasks should be false
	// - !empty, or num_gens!=0: one generation left, youngest has no
	//           more tasks in it (i_next == 0), but because !empty,
	//           there is 1 generation and youngest.some_tasks should
	//           remain set.
	assert( has_youngest
		&& "Youngest generation not locked and list becomes empty" );
	assert( num_gens <= 1
		&& "Few generations present when depleting youngest" );
	tasks.clear();
	if( empty )
	    youngest.clr_tasks();
    }

    assert( (youngest.has_tasks() == (num_gens > 0)) && "tasks require gens" );

    oldest.unlock();
    if( has_youngest )
	youngest.unlock();
}

void
ecltg_metadata::add_task( task_metadata * t, gen_tags * tags, group_t g ) {
    bool has_oldest = false;

    // Set pointer to task in argument's tags storage
    tags->st_task = t;

    // Note: potential race: (1) may_interfere() says no, (2) any number
    // of wakeups and pop_generation's to make may_interfere() true,
    // (3) error due to lack of synchronization.
    // may_interfere() returns true for num_gens == 2, so if this were
    // to happen, the last (detrimental) wakeup() would block on the
    // lock on youngest that it is required to take. Put differently,
    // num_gens <= 1 is the pure condition for interference, except we
    // take num_gens <= 2 to block out wakeup()'s and avoid races.
    if( may_interfere() ) {
	has_oldest = true;
	oldest.lock();
    }
    youngest.lock();

    if( num_gens == 0 ) { // Empty, tasks fly straight through
	// NOTE: There cannot be a concurrent wakeup, and there cannot be
	// concurrent add_task()'s, so no need to have locks, except we
	// need the lock on oldest to wait until the latest wakeup() has
	// finished.
	assert( has_oldest && "Must have lock on oldest when num_gens==0" );

	// Update oldest
	oldest.num_tasks++;

	// Update youngest
	youngest.open_group( g );
	youngest.add_task();

	// Count generations
	push_generation();
    } else if( num_gens == 1 ) {
	// NOTE: Taking a lock on oldest suffices to enforce the
	// synchronization between add_task() (issue, serially) and wakeup()
	// (concurrent), provided we have num_gens right.
	assert( has_oldest && "Must have lock on oldest when num_gens==1" );

	if( !youngest.match_group( g ) ) {	// Update youngest
	    assert( may_interfere() && "condition inlined and specialized" );

	    assert( youngest.has_tasks()
		    && "youngest not empty if 1 generation" );

	    youngest.open_group( g );
	    youngest.add_task();
	    push_generation();
	    t->add_incoming();
	    tasks.push_back( tags );

	    assert( (oldest.has_tasks() || num_gens != 1)
		    && "tasks require gens" );
	    assert( (youngest.has_tasks() == (num_gens > 0))
		    && "tasks require gens" );
	} else {
	    // One generation, tasks fly straight through.
	    // Update only oldest.
	    oldest.num_tasks++;
	}
    } else { // Two or more generations (at start)
	if( !youngest.match_group( g ) ) { // Going to at least two gens -- NO? OR argue that it is always true given that num_gens > 0
	    youngest.open_group( g );
	    youngest.add_task();
	    if( tasks.back() ) // should use iterators?
		tasks.back()->set_last_in_generation();
	    push_generation();
	    // In this case, we are assured that a wakeup will happen on the
	    // new task.
	    t->add_incoming();
	    tasks.push_back( tags );
	} else {
	    // In this case, we are assured that a wakeup will happen on the
	    // new task.
	    t->add_incoming();
	    tasks.push_back( tags );
	}

	// assert( (oldest.has_tasks() == youngest.has_tasks() || num_gens != 1)
	// && "Either no generations or oldest and youngest diff has_tasks" );
	assert( (has_oldest || !may_interfere()) && "interference invariant" );
	assert( (oldest.has_tasks() || num_gens != 1) && "tasks require gens" );
	assert( (youngest.has_tasks() == (num_gens > 0)) && "tasks require gens" );
    }

    if( has_oldest )
	oldest.unlock();
    youngest.unlock();
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
class serial_dep_tags { };

struct serial_dep_traits {
    static
    void arg_issue( task_metadata * fr,
		    obj_instance<ecltg_metadata> & obj,
		    gen_tags * sa, group_t g ) {
	ecltg_metadata * md = obj.get_version()->get_metadata();
	// errs() << "0 issue serial " << *md << " g=" << g << " task=" << fr << "\n";
	md->add_task( fr, sa, g );
	// errs() << "1 issue serial " << *md << "\n";
    }
    static
    bool arg_ini_ready( const obj_instance<ecltg_metadata> & obj, group_t g ) {
	const ecltg_metadata * md = obj.get_version()->get_metadata();
	// errs() << "0 ini_ready serial: " << *md << " g=" << g << "\n";
	bool x = md->match_group( g );
	// errs() << "1 ini_ready serial: " << *md << " x=" << x << "\n";
	return x;
    }
    static
    void arg_release( task_metadata * fr, obj_instance<ecltg_metadata> & obj,
		      gen_tags * tags, group_t g ) {
	ecltg_metadata * md = obj.get_version()->get_metadata();
	// errs() << "0 wakeup serial " << *md << " task=" << fr << "\n";
	md->wakeup( fr->get_graph() );
	// errs() << "1 wakeup serial " << *md << " task=" << fr << "\n";
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
class reduction_tags : public reduction_tags_base<ecltg_metadata>,
		       public gen_tags, public serial_dep_tags { };
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
struct dep_traits<ecltg_metadata, task_metadata, indep> {
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
struct dep_traits<ecltg_metadata, task_metadata, outdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, outdep<T> & obj,
			   typename outdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_write );
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
struct dep_traits<ecltg_metadata, task_metadata, inoutdep> {
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
struct dep_traits<ecltg_metadata, task_metadata, cinoutdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, cinoutdep<T> & obj,
	       typename cinoutdep<T>::dep_tags * sa ) {
	serial_dep_traits::arg_issue( fr, obj, sa, g_commut );
    }
    template<typename T>
    static bool
    arg_ini_ready( cinoutdep<T> & obj ) {
	ecltg_metadata * md = obj.get_version()->get_metadata();
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
struct dep_traits<ecltg_metadata, task_metadata, reduction> {
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

typedef ecltg_metadata obj_metadata;
typedef ecltg_metadata token_metadata;


} // end of namespace obj

#endif // ECLTASKGRAPH_H

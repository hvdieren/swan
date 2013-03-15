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
#ifndef WF_INTERFACE_H
#define WF_INTERFACE_H

#include "swan_config.h"

#include <type_traits>
#include <tr1/type_traits>

#include "wf_spawn_deque.h"
#include "wf_stack_frame.h"
#include "wf_setup_stack.h"
#include "wf_worker.h"
#include "logger.h"
#include "object.h"

// Definition of the programmer's interface.
// It seems that gcc does not make use of the returns_twice attribute.
// We specify it all the same. A side-effect should be to clobber all
// registers. Because it is not effective, we clobber ourselves.

// Return value of spawn(). This class casts to a type T.
// Only valid to extract value after the appropriate sync().
// Generates run-time error if value is extracted before it is posted.
template<typename T>
class chandle;

// Interface description:
// run(): call a parallel function from a sequential C/C++ function. Assumes
//        the spawn deque is empty. Similar to cilk_run.
template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
run( TR (*func)( Tn... ), Tn... args ) __attribute__((always_inline));

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value, TR>::type
run( TR (*func)( Tn... ), Tn... args ) __attribute__((always_inline));

// Interface description:
// spawn(): call function, potentially in parallel with parent.
template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args )
    __attribute__((always_inline, returns_twice));

template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), Tn... args )
    __attribute__((always_inline, returns_twice));

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args )
    __attribute__((always_inline, returns_twice));

// Interface description:
// call(): call function and have parent wait for the call to finish.
template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args )
    __attribute__((always_inline, returns_twice));

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args )
    __attribute__((always_inline, returns_twice));

// Interface description:
// ssync(): wait for all outstanding spawns in this stack frame to finish
inline void ssync() __attribute__((always_inline, returns_twice));

// ssync(): wait for all outstanding spawns in this stack frame that
// write to the object/dependency obj to finish. This may wait for
// internal tasks also, depending on what task is the last writer.
inline void ssync( obj::obj_instance<obj::obj_metadata> obj ) __attribute__((always_inline, returns_twice));

// Interface description:
// leaf_call(): call a C/C++ function without the possibility to return to
//              parallel mode.

// What follows is implementation

// A handle to the future that stores the return value.
// Ideally, the return value should be stack-allocated and the
// handle would be a pointer to the stack-allocated value.
// Just a little bit problematic if spawn() in a loop...
// T should not be a reference, it must be a scalar type!
//
// Extended the class to have the possibility to copy handles (danger
// for multiple free!) and to assign constants to the same thing.
// Thus: chandle<T> c = spawn(); ssync(); if( ... ) c = T(); is possible
template<typename T>
class chandle {
    future c;

public:
    chandle() { }
    // chandle( typename std::tr1::add_const<typename std::tr1::add_reference<T>::type>::type t ) {
    chandle( const T & t ) {
	c.set_result( (intptr_t)t );
    }
    
    operator T () const { return get_result(); }

private:
    T get_result() const {
	assert( c.is_finished()
		&& "Requesting value from non-finished future" );
	return c.get_result<T>();
    }

public:
    future & get_future() { return c; }
};

// Special case for void return values
template<>
class chandle<void> {
    future c;

public:
    chandle() { }
    
private:
    void get_result() const {
	assert( c.is_finished()
		&& "Requesting value from non-finished future" );
    }

public:
    future & get_future() { return c; }
};

// Check whether dependencies of arguments are satisfied
#if STORED_ANNOTATIONS
template<typename... Tn>
inline bool wf_arg_ready(full_frame * ff, task_data_t & task_data) {
    bool ready = true;
    if( !the_task_graph_traits::arg_ready( task_data ) )
	ready = false;
#if 0
    assert( ff );
    return ff->slack_invoke( ready );
#endif
    return ready;
}
#else
template<typename... Tn>
inline bool wf_arg_ready(full_frame * ff, Tn... an) {
    bool ready = true;
    if( !the_task_graph_traits::arg_ready( an... ) )
	ready = false;
#if 0
    assert( ff );
    return ff->slack_invoke( ready );
#endif
    return ready;
}
#endif

// Grab argument dependencies
template<typename Frame, typename... Tn>
inline void wf_arg_issue(Frame * fr, stack_frame * parent, Tn & ... an) {
    the_task_graph_traits::arg_issue( fr, parent, an... );
}

// Release argument dependencies
template<typename... Tn>
inline void wf_arg_release(Tn... an) {
    the_task_graph_traits::arg_release( an... );
}

#if TRACING
template<typename Frame>
static void wf_trace( Frame * fr, stack_frame * parent,
		      void * fn, bool sp, bool ready ) {
    errs() << ( sp ? "spawn function " : "call function " )
	   << fn << " at " << fr
	   << " parent " << parent
	   << ( ready ? " now\n" : " pending\n" );
}
#else
template<typename Frame>
static void wf_trace( Frame * fr, stack_frame * parent,
		      void * fn, bool sp, bool ready ) { }
#endif

// Define in this header file because we need definitions of stack_frame,
// spawn_deque and worker_state.
// Locking policy: delay locks as long as possible. The current frame or
// parent frame cur_frame is in state fs_executing and is therefore protected
// against stealing from its spawn_deque. The child is created here, so no-one
// has a pointer to it so it does not require a lock either. The spawn_deque
// is manipulated only when pushing the child. Therefore, push the child only
// when it is completely finished and we don't need the parent any more, i.e.,
// we have transfered to the child stack frame.
// Thus the spawn_deque is locked only in stack_frame::split_ctrl();
template<typename TR, typename... Tn>
void stack_frame::invoke( future * cresult,
#if STORED_ANNOTATIONS
			  task_data_t & task_data_p,
#endif
			  bool _is_call, TR (*func)( Tn... ), Tn... args ) {
    // Create new stack frame, put arguments on frame and move
    // stack pointer to it. Then call function on new stack.
    // When function returns normally (not because parent was stolen),
    // then free the child stack.
    stack_frame * cur_frame = stack_frame::my_stack_frame();
    assert( cur_frame->get_state() == fs_executing );
    assert( cur_frame->get_owner() == worker_state::tls()->get_deque() );

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_invoke++;
#endif

#if STORED_ANNOTATIONS
    stack_frame * frame
	= new stack_frame( cresult, task_data_p, false, _is_call );
#else
    // Create child frame and push it on deque
    size_t args_size = arg_size( args... ); // statically computed
    size_t tags_size = the_task_graph_traits::arg_stored_size<Tn...>();
    size_t fn_tags_size = the_task_graph_traits::fn_stored_size();
    size_t num_args = arg_num<Tn...>(); // statically computed

    stack_frame * frame
	= new stack_frame( args_size, tags_size, fn_tags_size, num_args,
			   cur_frame, cresult, false, _is_call );

    // Copy the arguments to our stack frame
    frame->push_args( args... );
#endif

    LOG( id_invoke, frame );
    wf_trace( frame, cur_frame, (void*)func, !_is_call, true );

    // Grab the arguments, potentially modifying runtime state arguments
    wf_arg_issue( frame, cur_frame, args... );

    // Save the PIC register, which is ebx on the supported target
    // Can't do this from within the stub, because gcc changes ebx before
    // we can copy.
#ifdef __PIC__
    cur_frame->saved_ebx = get_pr();
#endif

    // The functions that the stub needs to call
    void (*void_func)(void) = reinterpret_cast<void (*)(void)>(func);

    // The parent is no longer executing
    cur_frame->set_state( fs_waiting );

    // Call the stub to control stack unwinding and pass it the spawned
    // function. Spawned function arguments are already on the stack.
    bool stolen
	= stack_frame::prevent_inlining_dir<&stack_frame::
	template split_stub<fc_executing, TR, Tn...> >( frame, void_func );

    if( likely( !stolen ) ) { // identical to try_pop() successful
#if IMPROVED_STUBS
	CLOBBER_CALLEE_SAVED_BUT1(); // only when prevent_inlining() is inlined?
#endif
	// Parent has now been sinked into current call stack. Moved
	// update of parent->state and spawn_deque->popped to try_pop().
	assert( frame->get_state() == fs_executing );
	assert( !frame->is_full() );
	assert( frame->get_owner() == worker_state::tls()->get_deque() );
	stack_frame * parent = stack_frame::my_stack_frame(); // frame->get_parent(); // or my_stack_frame()
	parent->verify();
	// parent->check_continuation(); // may not be valid: returned over stack!
	parent->clear_continuation();
	// We don't need to lock the parent when freeing because the child
	// is not full!
	assert( !frame->is_full() && "Frame may not be full here" );
	the_task_graph_traits::release_task( frame );
	delete frame;
    } else {
	// We come here if the frame has been stolen.
	// resume() sets the state to executing
	CLOBBER_CALLEE_SAVED_BUT1(); // because of resume!
	assert( cur_frame->get_owner() == worker_state::tls()->get_deque() );
	assert( cur_frame->get_state() == fs_executing );
    }
}

template<typename TR, typename... Tn>
void stack_frame::create_waiting( full_frame * cur_full, future * cresult,
#if STORED_ANNOTATIONS
				  task_data_t & task_data_p,
#endif
				  TR (*func)( Tn... ), Tn... args ) {
    stack_frame * cur_frame = cur_full->get_frame();
    assert( cur_frame->get_owner() == worker_state::tls()->get_deque() );

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_waiting++;
#endif

#if STORED_ANNOTATIONS
    stack_frame * frame
	= new stack_frame( cresult, task_data_p, true, false );
#else
    // Create child frame
    size_t args_size = arg_size( args... ); // statically computed
    size_t tags_size = the_task_graph_traits::arg_stored_size<Tn...>();
    size_t fn_tags_size = the_task_graph_traits::fn_stored_size();
    size_t num_args = arg_num<Tn...>(); // statically computed

    stack_frame * frame
	= new stack_frame( args_size, tags_size, fn_tags_size, num_args,
			   cur_frame, cresult, true, false );

    // Copy the arguments to our stack frame
    frame->push_args( args... );
#endif

    LOG( id_create, frame );

    // Save the PIC register, which is ebx on the supported target
    // Can't do this from within the stub, because gcc changes ebx before
    // we can copy.
#ifdef __PIC__
    cur_frame->saved_ebx = get_pr();
#endif

    // The functions that the stub needs to call
    void (*void_func)(void) = reinterpret_cast<void (*)(void)>(func);

    // Call the stub to control stack unwinding and pass it the spawned
    // function. Spawned function arguments are already on the stack.
    stack_frame::prevent_inlining_dir<&stack_frame::
    split_stub<fc_waiting, TR, Tn...> >( frame, void_func );
    CLOBBER_CALLEE_SAVED_BUT1();

    assert( frame->get_state() == fs_waiting );
    frame->verify();
}

template<typename TR, typename... Tn>
pending_frame *
stack_frame::create_pending( TR (*func)( Tn... ),
			     stack_frame * cur, future * fut,
#if STORED_ANNOTATIONS
			     task_data_t & task_data_p,
#endif
			     Tn... args ) {
#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_pending++;
#endif

#if STORED_ANNOTATIONS
    pending_frame * pnd
	= new pending_frame( task_data_p, fut,
			     reinterpret_cast<void (*)(void)>(func),
			     &stack_frame::split_stub<fc_waiting, TR, Tn...> );
#else
    size_t args_size = arg_size( args... ); // statically computed
    size_t tags_size = the_task_graph_traits::arg_stored_size<Tn...>();
    size_t fn_tags_size = the_task_graph_traits::fn_stored_size();
    size_t num_args = arg_num<Tn...>(); // statically computed

    pending_frame * pnd
	= new pending_frame( args_size, tags_size, fn_tags_size, num_args, fut,
			     reinterpret_cast<void (*)(void)>(func),
			     &stack_frame::split_stub<fc_waiting, TR, Tn...> );

    // Push arguments (potentially modified for renaming) on the stack
    pnd->push_args( args... );
#endif

    wf_trace( pnd, cur, (void*)func, true, false );

    // Grab the arguments, potentially modifying runtime state arguments
    wf_arg_issue( pnd, cur, args... );

    // Notify parent that there is another child
    cur->get_full()->add_child();

    the_task_graph_traits::push_pending( cur->get_full(), pnd );
    return pnd;
}

// From wf_task.h
template<typename DstTy, typename SrcTy> // eg: obj::outdep and obj::indep
void create_copy_task( DstTy dst, SrcTy src ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    assert( fr->is_full()
	    && "Should create pending copy task only from full frame" );
    stack_frame::create_pending( &delayed_copy_task<DstTy, SrcTy>,
				 fr, (future *)0, dst, src );
}

template<typename Monad, typename AccumTy, typename ReductionTy>
void parallel_reduction_task( AccumTy accum, ReductionTy * reduc ) {
    typedef typename Monad::value_type T;
    obj::object_t<T, obj::obj_recast> obj( accum );

    // errs() << "execute reduction in parallel (spawned task)...\n";

    size_t nthreads = ::nthreads;
    obj::obj_version<typename ReductionTy::metadata_t> * data[nthreads+1];
    int n = reduc->build_reduction_array( accum.get_version(), data );
    data[0] = obj.get_version();

    // n == 0 signifies nothing to do
    if( !n )
	return;

    // TODO: opportunity for leading-zero assembly instruction
    int levels = 0;
    while( (1<<levels) < n )
	++levels;

    // typedef obj::inoutdep<T> AccumTy;
    typedef obj::indep<T> InTy;

    // assert( stack_frame::my_stack_frame()->is_full()
    // && "Should create parallel reduction task only from full frame" );

    for( int lvl=0; lvl < levels; ++lvl ) {
	// errs() << "reduce: level " << lvl << "\n";

	// First iteration specialized: don't pass the same argument twice
	assert( (1<<lvl) < n && "we computed levels that way" );
	spawn( &reduce_pair_task<Monad, AccumTy, InTy>,
	       AccumTy::create( data[0] ), InTy::create( data[(1<<lvl)] ) );

	for( int i=1<<(lvl+1); i+(1<<lvl) < n; i += 1<<(lvl+1) ) {
	    // errs() << "reduce: pairs " << i << ", " << (i+(1<<lvl)) << "\n";
	    spawn( &reduce_pair_task<Monad, AccumTy, InTy>,
		   AccumTy::create( data[i] ), InTy::create( data[i+(1<<lvl)] ) );
	}
    }

    ssync();
}

template<typename Monad, typename AccumTy, typename ReductionTy>
void create_parallel_reduction_task( AccumTy accum, ReductionTy * reduc ) {
    spawn( &parallel_reduction_task<Monad, AccumTy, ReductionTy>, accum, reduc );
}


#if STORED_ANNOTATIONS
template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), fr );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), td ) ) {
	stack_frame::invoke( &ch.get_future(), td, false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, &ch.get_future(), td, args... );
    }
}

template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), fr );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), td ) ) {
	stack_frame::invoke( (future*)0, td, false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, (future*)0, td, args... );
    }
}

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), fr );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), td ) ) {
	stack_frame::invoke( &ch.get_future(), td, false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, &ch.get_future(), td, args... );
    }
}

template<typename TR, typename... Tn>
typename std::enable_if<std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args ) {
    future cresult;
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), stack_frame::my_stack_frame() );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    stack_frame::invoke( &cresult, td, true, func, args... );
    return;
}

template<typename TR, typename... Tn>
typename std::enable_if<!std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args ) {
    future cresult;
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), stack_frame::my_stack_frame() );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    stack_frame::invoke( &cresult, td, true, func, args... );
    return cresult.get_result<TR>();
}

template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
run( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    full_frame * cur_frame = ws->get_dummy();
    cur_frame->get_frame()->set_owner( (spawn_deque*)ws->get_deque() );
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), cur_frame->get_frame() );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    stack_frame::create_waiting( cur_frame, ws->get_future(), td, func, args... );
    cur_frame->get_frame()->set_owner( 0 );
    ws->worker_fn();
}

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value, TR>::type
run( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    full_frame * cur_frame = ws->get_dummy();
    cur_frame->get_frame()->set_owner( (spawn_deque*)ws->get_deque() );
    task_data_t td( arg_size( args... ),
		    the_task_graph_traits::arg_stored_size<Tn...>(),
		    arg_num<Tn...>(), cur_frame->get_frame() );
    // Copy the arguments to our stack frame
    td.push_args( args... );
    the_task_graph_traits::arg_stored_initialize<Tn...>( td );
    stack_frame::create_waiting( cur_frame, ws->get_future(), td, func, args... );
    cur_frame->get_frame()->set_owner( 0 );
    ws->worker_fn();
    return ws->get_future()->get_result<TR>();
}

#else
template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), args... ) ) {
	stack_frame::invoke( &ch.get_future(), false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, &ch.get_future(), args... );
    }
}

template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), args... ) ) {
	stack_frame::invoke( (future*)0, false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, (future*)0, args... );
    }
}

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value>::type
spawn( TR (*func)( Tn... ), chandle<TR> & ch, Tn... args ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    if( /*!fr->is_full() ||*/ wf_arg_ready( fr->get_full(), args... ) ) {
	stack_frame::invoke( &ch.get_future(), false, func, args... );
    } else {
	stack_frame::create_pending( func, fr, &ch.get_future(), args... );
    }
}

template<typename TR, typename... Tn>
typename std::enable_if<std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args ) {
    future cresult;
    stack_frame::invoke( &cresult, true, func, args... );
    return;
}

template<typename TR, typename... Tn>
typename std::enable_if<!std::is_void<TR>::value, TR>::type
call( TR (*func)( Tn... ), Tn... args ) {
    future cresult;
    stack_frame::invoke( &cresult, true, func, args... );
    return cresult.get_result<TR>();
}

template<typename TR, typename... Tn>
inline typename std::enable_if<std::is_void<TR>::value>::type
run( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    full_frame * cur_frame = ws->get_dummy();
    cur_frame->get_frame()->set_owner( (spawn_deque*)ws->get_deque() );
    stack_frame::create_waiting( cur_frame, ws->get_future(), func, args... );
    cur_frame->get_frame()->set_owner( 0 );
    ws->worker_fn();
}

template<typename TR, typename... Tn>
inline typename std::enable_if<!std::is_void<TR>::value, TR>::type
run( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    full_frame * cur_frame = ws->get_dummy();
    cur_frame->get_frame()->set_owner( (spawn_deque*)ws->get_deque() );
    stack_frame::create_waiting( cur_frame, ws->get_future(), func, args... );
    cur_frame->get_frame()->set_owner( 0 );
    ws->worker_fn();
    return ws->get_future()->get_result<TR>();
}

#endif

void
stack_frame::sync() {
    LOG( id_sync_ctr, 0 /*join_counter*/ );
#ifdef __PIC__
    saved_ebx = get_pr();
#endif
    stack_frame::sync_stub( this );
    CLOBBER_CALLEE_SAVED_BUT1();
}

// On a sync, we (1) check if all children are done (traverse the list
// of futures in the current stack frame).
// If all children are done, we return. Else, we put the stack frame
// aside, allowing to resume it later and even have it stolen.
void ssync() {
    stack_frame * fr = stack_frame::my_stack_frame();
    assert( fr->get_state() == fs_executing );
    LOG( id_sync_enter, fr );

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_ssync++;
#endif

    // If the frame is full and has outstanding children, jump back to scheduler
    // until they have executed. Otherwise, we're done.
    if( full_frame * ff = fr->get_full() ) {
	the_task_graph_traits::run_finalizers( fr, true ); // reductions
	if( !ff->all_children_done() ) {
	    do {
		fr->sync();
		CLOBBER_CALLEE_SAVED_BUT1();
		// all following code will be executed when resuming the sync
		assert( fr == stack_frame::my_stack_frame()
			&& "Sanity check on variables" );
	    } while( !ff->all_children_done() );
	}
	the_task_graph_traits::run_finalizers( fr, false ); // reductions
    }
    assert( fr->get_state() == fs_executing );
}

// On a conditional sync, we (1) check if the object's current version
// has no pending writers.
// If all writers are done, we return. Else, we put the stack frame
// aside, allowing to resume it later and even have it stolen.
template<typename T, template<typename U> class DepTy>
void ssync( DepTy<T> obj ) {
    stack_frame * fr = stack_frame::my_stack_frame();
    assert( fr->get_state() == fs_executing );
    LOG( id_dsync_enter, fr );

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_ssync++;
#endif

    // If the frame is full and has outstanding children, jump back to scheduler
    // until they have executed. Otherwise, we're done.
    if( fr->is_full() ) {
	if( !DepTy<T>::dep_traits::arg_ini_ready( obj ) ) {
	    do {
		fr->sync();
		CLOBBER_CALLEE_SAVED_BUT1();
		// all following code will be executed when resuming the sync
		assert( fr == stack_frame::my_stack_frame()
			&& "Sanity check on variables" );
	    } while( !DepTy<T>::dep_traits::arg_ini_ready( obj ) );
	}
	obj.get_version()->finalize(); // reductions
    }
    assert( fr->get_state() == fs_executing );
}

// TODO: figure out how to write a template for foreach that allows both
//   foreach( 0, 10, f );
// where f( int );
// and
//   foreach( c.begin(), c.end(), f );
// where f( c's type::value_type ) instead of f( c's type::iterator )
template<typename InputIterator, typename ValueType, typename... Tn>
void foreachg( InputIterator start, InputIterator end,
	      size_t granularity,
	      void (*func)( ValueType, Tn... ), Tn... an ) {
    // errs() << "foreachg " << start << " to " << end << "\n";
    if( size_t(end - start) <= granularity ) {
	for( ; start != end; ++start )
	    call( func, *start, an... );
    } else {
	InputIterator half = start + (end+1 - start) / 2;
	spawn( &foreachg<InputIterator,ValueType,Tn...>, start, half, granularity, func, an... );
	call( &foreachg<InputIterator,ValueType,Tn...>, half, end, granularity, func, an... );
	ssync();
    }
    // errs() << "foreachg " << start << " to " << end << " done\n";
}

template<typename InputIterator, typename ValueType, typename... Tn>
void foreach( InputIterator start, InputIterator end,
	      void (*func)( ValueType, Tn... ), Tn... an ) {
    foreachg<InputIterator,ValueType,Tn...>( start, end, 1, func, an...  );
}

template<typename InputIterator, typename... Tn>
void foreachig( InputIterator start, InputIterator end,
	       size_t granularity,
	       void (*func)( InputIterator, Tn... ), Tn... an ) {
    // errs() << "foreachg " << start << " to " << end << "\n";
    if( size_t(end - start) <= granularity ) {
	for( ; start != end; ++start )
	    call( func, start, an... );
    } else {
	InputIterator half = start + (end+1 - start) / 2;
	spawn( &foreachig<InputIterator,Tn...>, start, half, granularity, func, an... );
	call( &foreachig<InputIterator,Tn...>, half, end, granularity, func, an... );
	ssync();
    }
    // errs() << "foreachg " << start << " to " << end << " done\n";
}

template<typename InputIterator, typename... Tn>
void foreachi( InputIterator start, InputIterator end,
	      void (*func)( InputIterator, Tn... ), Tn... an ) {
    foreachig<InputIterator,Tn...>( start, end, 1, func, an...  );
}

template<typename InputIterator, typename... Tn>
void foreachirg( InputIterator start, InputIterator end, size_t granularity,
		 void (*func)( InputIterator, InputIterator, Tn... ),
		 Tn... an ) {
    // errs() << "foreachg " << start << " to " << end << "\n";
    if( size_t(end - start) <= granularity ) {
	call( func, start, end, an... );
    } else {
	InputIterator half = start + (end+1 - start) / 2;
	spawn( &foreachirg<InputIterator,Tn...>, start, half, granularity, func, an... );
	call( &foreachirg<InputIterator,Tn...>, half, end, granularity, func, an... );
	ssync();
    }
    // errs() << "foreachg " << start << " to " << end << " done\n";
}

template<typename InputIterator, typename... Tn>
void foreachir( InputIterator start, InputIterator end,
		void (*func)( InputIterator, InputIterator, Tn... ),
		Tn... an ) {
    foreachirg<InputIterator,Tn...>( start, end, 1, func, an...  );
}

// The leaf_call<>() functions execute leaf calls. Their property is that
// they can not be stolen (because they are leafs, the processor never lets
// go of them). Therefore, we can execute them on the original program stack
// giving us unbounded stack space. This is handy e.g. when calling C library
// functions. Otherwise, there is no important distinction with call().

// The leaf_call<>() functions assume that the compiler always references
// local variables relative to the base pointer, so it is valid to change
// the stack pointer to the original thread stack and leave the base pointer
// on the malloc'ed cactus stack frame.

template<typename TR, typename... Tn>
static inline
typename std::enable_if<std::is_void<TR>::value, TR>::type
leaf_call( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    intptr_t sp;

    assert( !the_task_graph_traits::arg_introduces_deps<Tn...>() );
    // Grab the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_issue( (stack_frame *)0, stack_frame::my_stack_frame(), args... );

    save_sp( sp );
    restore_sp( ws->get_main_sp() );
    (*func)( args... );
    restore_sp( sp );

    // Release the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_release( args... );
}

#if 0
// This works for ellipsis functions but does not properly check types...
template<typename TR, typename... Fn, typename... Tn>
static inline
typename std::enable_if<std::is_void<TR>::value, TR>::type
leaf_call( TR (*func)( Fn..., ... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    intptr_t sp;

    assert( !the_task_graph_traits::arg_introduces_deps<Tn...>() );
    // Grab the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_issue( (stack_frame *)0, stack_frame::my_stack_frame(), args... );

    save_sp( sp );
    restore_sp( ws->get_main_sp() );
    (*func)( args... );
    restore_sp( sp );

    // Release the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_release( args... );
}

template<typename TR, typename... Fn, typename... Tn>
static inline
typename std::enable_if<!std::is_void<TR>::value, TR>::type
leaf_call( TR (*func)( Fn..., ... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    intptr_t sp;
    TR tr;

    assert( !the_task_graph_traits::arg_introduces_deps<Tn...>() );
    // Grab the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_issue( (stack_frame *)0, stack_frame::my_stack_frame(), args... );

    save_sp( sp );
    restore_sp( ws->get_main_sp() );
    tr = (*func)( args... );
    restore_sp( sp );

    // Release the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_release( args... );

    return tr;
}
#endif

template<typename TR, typename... Tn>
static inline
typename std::enable_if<!std::is_void<TR>::value, TR>::type
leaf_call( TR (*func)( Tn... ), Tn... args ) {
    worker_state * ws = worker_state::tls();
    intptr_t sp;
    TR tr;

    assert( !the_task_graph_traits::arg_introduces_deps<Tn...>() );
    // Grab the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_issue( (stack_frame *)0, stack_frame::my_stack_frame(), args... );

    save_sp( sp );
    restore_sp( ws->get_main_sp() );
    tr = (*func)( args... );
    restore_sp( sp );

    // Release the arguments, potentially modifying runtime state arguments
    // if( cur_frame->is_full() )
    // wf_arg_release( args... );

    return tr;
}

// Compliance with Cilk's SYNCHED
inline bool SYNCHED() {
    stack_frame * fr = stack_frame::my_stack_frame();
    return !fr->is_full();
}

#endif // WF_INTERFACE_H

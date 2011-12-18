#include "config.h"

#include <csetjmp>

#include "wf_stack_frame.h"
#include "wf_spawn_deque.h"
#include "wf_interface.h"
#include "logger.h"

// Split off part of the work because we want to be able to work on a
// nice stack that automatically restores callee-saved registers and that
// is also not limited by the scarce room on the stack below the spawned
// function's arguments.
// That part is not guaranteed right now but perhaps we are ok with
// the combination of returns_twice and noreturn attributes.
bool
stack_frame::split_return( stack_frame * child ) {
#if !defined(NDEBUG)
    extern __thread worker_state * tls_worker_state;
#endif

    LOG( id_split_return, child );

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_split_ret++;
#endif

    // child is implicitly locked because it is executing!
    assert( my_stack_frame() == child );
    assert( child->get_state() == fs_executing );
    assert( child->get_owner() == tls_worker_state->get_deque() );
    // child->verify();

    // Flag computation is finished.
    child->flag_result();

    // Make sure that all user tasks have finished. The user *must*
    // write a ssync(); before a return, if spawns may be pending.
    assert( ( !child->is_full() || child->get_full()->all_children_done() )
	    && "Missing sync in procedure" );

    // Check whether parent is stolen and see if we can return
    // errs() << "stub_return " << child << " at pop\n";
    if( !child->get_owner()->try_pop() ) {
	// Cleanup and jump to the worker routine. Prefer to use
	// setjmp/longjmp over direct assembly jmp because it is safer
	// in the worker code. It also minimizes inline assembly.
	worker_state::longjmp( int( child->is_call() ? edc_call : edc_spawn ) );
    }
    return false; // not stolen
}

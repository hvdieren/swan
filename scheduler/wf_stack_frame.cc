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

#include "swan_config.h"

#include <csetjmp>

#include "wf_stack_frame.h"
#include "wf_spawn_deque.h"
#include "wf_setup_stack.h"
#include "wf_worker.h"
#include "wf_interface.h"
#include "mangled.h"
#include "logger.h"

#if 0
const size_t slack_weights::success[sm_N] = { 0, 0, -1, -1 };
const size_t slack_weights::fail[sm_N]    = { 2, 0,  1,  0 };
#endif

std::ostream & operator << ( std::ostream & os, empty_deque_condition_t edc ) {
    switch( edc ) {
    case edc_bootstrap: return os << "edc_bootstrap";
    case edc_call: return os << "edc_call";
    case edc_spawn: return os << "edc_spawn";
    case edc_sync: return os << "edc_sync";
    }
    return os;
}

void
stack_frame::resume() {
#if !defined(NDEBUG)
    extern __thread worker_state * tls_worker_state;
#endif

    LOG( id_resume, this );

    // only callable from main worker's stack!
#ifndef __x86_64__
    assert( ((long)my_stack_frame() < 0) );
#endif

    assert( get_owner() == tls_worker_state->get_deque() );
    assert( get_state() == fs_waiting || get_state() == fs_suspended );
    assert( stack_frame_of( saved_ebp ) == this );
    verify();
    check_continuation();
    clear_continuation();
    set_state( fs_executing );

#if TRACING
    errs() << "resume " << this << '\n';
#endif

    LEAVE_RETURN_ONE( saved_ebp, saved_ebx );
}

void
stack_frame::sync_stub( stack_frame * fr ) {
    LOG( id_sync_stub, fr );
    assert( fr->get_state() == fs_executing );
    fr->saved_ebp = get_bp();
    fr->save_continuation();
    worker_state::longjmp( (int)edc_sync );
}

// The split_stub function is declared with attribute regparm(2) to force
// initial allocation of arguments to registers. Hopefully gcc will keep them
// there because it is impossible to retrieve them from the stack after
// we have switched stacks. (It is possible to reference them when copying
// stack). Note that we have to be carefull that this function does not use
// too much stack space. Only N_stub bytes are available. If gcc lowers esp
// too much and starts storing stuff on the stack relative to esp (instead of
// ebp) then these items will overwrite the spawnee's arguments and anything
// can go wrong. We have avoided this by writing a lot of assembler code,
// removing the need for gcc to compute much temporaries (eg in reading
// the fields of the stack_frame) and by making the split_return() function
// a regparm(2) function.
#if !IMPROVED_STUBS
bool
stack_frame::prevent_inlining( stack_frame * child, void (*fn_ptr)(void),
			       bool (*ss_ptr)(stack_frame *, void (*)(void)) ) {
    return (*ss_ptr)( child, fn_ptr );
}
#endif

stack_frame *
stack_frame::create_frame( stack_frame * parent, spawn_deque * owner,
			   pending_frame * pnd ) {
    // No reason to have lock: join_counter > 0 due to existance of
    // pending_frame.
    // assert( parent->get_full()->test_lock()
    // && "Must have lock on parent when creating frame" );

    stack_frame * fr = new stack_frame( parent, owner, pnd, true, false );

#ifdef __PIC__
    fr->set_saved_pr( get_pr() ); // redundant - will return via longjmp()
#endif

    wf_trace( fr, parent, (void *)pnd->func, true, true );

    prevent_inlining( fr, pnd->func, pnd->stub );
    CLOBBER_CALLEE_SAVED_BUT1();

    assert( fr->get_state() == fs_waiting );
    fr->verify();

    LOG( id_create_frame, pnd );
    LOG( id_create_frame, fr );

    delete pnd;

    return fr;
}

#if DBG_VERIFY
void
stack_frame::verify() const {
    if( parent == 0 )
	return;

    intptr_t ebp = *(intptr_t*)saved_ebp;
    intptr_t esp = saved_ebp+2*sizeof(intptr_t);
    stack_frame * fr_ebp = stack_frame_of(ebp);
    stack_frame * fr_esp = stack_frame_of(esp);
    // stack_frame * fr_stk = stack_frame_of(intptr_t(stack_ptr));

    assert( fr_ebp == fr_esp );
    // assert( fr_ebp == fr_stk );
}

void
stack_frame::verify( intptr_t ebp ) {
    intptr_t ret_ebp = *(intptr_t*)ebp;
    intptr_t ret_esp = ebp+2*sizeof(intptr_t);
    void * fr_ebp = (void*)( ret_ebp & ~(Align-1) );
    void * fr_esp = (void*)( ret_esp & ~(Align-1) );
    if( fr_ebp != fr_esp && (long)ebp >= 0 ) {
	std::cerr << "verify(" << (void *)ebp
		  << "): ret_ebp=" << (void *)ret_ebp
		  << ", fr_ebp=" << (void *)fr_ebp
		  << ", ret_esp=" << (void *)ret_esp
		  << ", fr_esp=" << (void *)fr_esp
		  << "\n";
    }
    assert( fr_ebp == fr_esp || (long)ebp < 0 );
}

void
stack_frame::verify_rec() const {
    const stack_frame * ptr = this;
    while( ptr && ptr->get_owner() == get_owner() ) {
	// Note: join_counter is uninitialized in stack frames, but it
	// is zero on first allocation and every stack_frame resets it to zero.
	if( full_frame * ff = ptr->get_full() ) {
	    if( !ff->all_children_done() && ptr->saved_ebp )
		verify( ptr->saved_ebp );
	}
	ptr = ptr->parent;
    }
}
#endif // DBG_VERIFY

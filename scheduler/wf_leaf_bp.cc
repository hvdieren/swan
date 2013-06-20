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

#include "wf_stack_frame.h"
#include "wf_spawn_deque.h"
#include "wf_setup_stack.h"
#include "wf_worker.h"
#include "logger.h"

// All functions in this file *MUST* use a base pointer in their
// stack frame setup.


// Release all locks. Child remains unaccessible as it is in state
// fs_executing, unless in bootstrap case.
void
stack_frame::split_ctrl_waiting( stack_frame * child ) {
    // Update state
    child->set_state( fs_waiting );
    // Push the frame on the spawn_deque now, making the parent
    // stealable.  Deque must be empty so we don't need a lock
    spawn_deque * owner = child->get_owner();
    assert( owner->empty() && "Deque must be empty" );
    owner->push( child->get_full() );

    // Return and save continuation
    intptr_t leave_ebp = child->saved_ebp;
#ifdef __PIC__
    intptr_t leave_ebx = child->saved_ebx;
#endif
    child->saved_ebp = get_bp();
    child->save_continuation();
    LEAVE( leave_ebp, (intptr_t)leave_ebx );
}

void
stack_frame::split_ctrl( stack_frame * child, frame_create_t fcreate ) {
    LOG( id_split_ctrl, child );

    if( fcreate == fc_executing )
	split_ctrl_executing( child );
    else
	split_ctrl_waiting( child );
}


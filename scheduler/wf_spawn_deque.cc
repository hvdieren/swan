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

#include "wf_spawn_deque.h"
#include "wf_setup_stack.h"
#include "logger.h"

// TODO: optimize by either allowing multiple full frame/spawn deque
// (i.e. not removing them from the deque) OR by playing with ooo_frames.
void
spawn_deque::convert_and_pop_all() {
    // Exclude any work-stealing activity on our current stack
    deque.lock_self();

    // Safety case: require head is not full in detch. Cannot change this
    // condition once deque is locked.
    if( current.head->is_full() ) {
	deque.unlock_self();
	return;
    }

    // Make all frames on current stack and spawn deque full.
    // detach() iterates until it hits a full frame.
    bool need_unlock = deque.detach( current.head, current.head->get_parent(), this );
    full_frame * sf = current.head->convert_to_full();
    if( need_unlock )
	sf->get_parent()->unlock( this );

    // Empty the deque
    deque.flush();

    // Restrict current call_stack to one full_frame
    current.tail = current.head;

    // Done
    deque.unlock_self();
}

pending_frame *
spawn_deque::steal_rchild( full_frame * fr, steal_method_t sm ) {
#if TIME_STEALING
    pp_time_start( &time_rchild_steal );
    pp_time_start( &time_rchild_steal_fail );
#endif

    SD_PROFILE( rsib_steals );
    if( pending_frame * rchild = the_task_graph_traits::get_ready_task( fr ) ) {
	LOG( id_steal_right_child, rchild );
	SD_PROFILE( rsib_steals_success );
#if !PACT11_VERSION && 0
	fr->rchild_steal_attempt( true, sm );
#endif
#if TIME_STEALING
	pp_time_end( &time_rchild_steal );
#endif
	return rchild;
    }

#if !PACT11_VERSION && 0
    fr->rchild_steal_attempt( false, sm );
#endif
#if TIME_STEALING
    pp_time_end( &time_rchild_steal_fail );
#endif
    return 0;
}

void
spawn_deque::wakeup_steal( full_frame * parent, pending_frame * fr ) {
    LOG( id_wakeup_steal, fr );
    SD_PROFILE( cvt_pending );

    assert( empty() && "Can only wakeup-steal when extended deque is empty" );

    // Also cleans up fr
    // It may be the case that we create a frame here with a different parent!
    // This is due to the delegation of dependencies and the corresponding
    // top-down traversal of the task graph in case a sub-tree was abandond.
    stack_frame::create_frame( parent->get_frame(), this, fr );
}

bool
spawn_deque_store::detach( stack_frame * s, stack_frame * t,
			   spawn_deque * tgt ) {
    if( t->is_full() ) {
	t->get_full()->lock( tgt );
	assert( t->get_owner() == s->get_owner() );
	assert( t->get_parent()->get_owner() != s->get_owner() );
	t->set_state( fs_suspended );
	t->set_owner( 0 );
	return true;
    }

    bool need_unlock
	= detach( s, t->get_parent(), tgt );

    assert( t->get_owner() == s->get_owner() );
    t->convert_to_full(); // ->lock( tgt ); // TODO: create full_frame in locked state!
    t->set_state( fs_suspended );
    t->set_owner( 0 );

    if( need_unlock )
	t->get_parent()->get_full()->unlock( tgt );

    return false;
}

full_frame *
spawn_deque_store::pop_front( spawn_deque * tgt,
			      stack_frame *** new_top ) { // steal
    // Try to steal the oldest call stack, taking potential races between
    // victim and thief into account.
    long my_head = head++;
    __vasm__( "mfence" : : : "memory" );
    if( head > tail ) {
	head--;
	*new_top = &deque[head].tail;
	LOG( id_pop_front, 0 );
	return 0;
    }
    // Youngest frame on the call stack is transferred to tgt.
    // I suspect there is a potential race here, when we eat the last
    // frame from head, tail finishes, worker longjmps() and then
    // resets deque and restarts executing work after stealing ; only then
    // do we resume current thread and try to read stack frame pointer...
    // Problem solved by putting an extra synchronization in try_pop().
    // That synchronization grabs the deque lock so it waits until head
    // has been read and it also makes sure that we do not have a race when
    // converting the new top of this extended spawn deque to a full frame.
    stack_frame * s = deque[my_head].head;
    *new_top = &deque[my_head+1].tail;
    full_frame * sf;
    if( !s->is_full() ) {
	bool need_unlock = detach( s, s->get_parent(), tgt );
	sf = s->convert_to_full();
	sf->lock( tgt );
	if( need_unlock )
	    sf->get_parent()->unlock( tgt );
    } else {
	assert( s->get_parent()->get_owner() != s->get_owner() );
	sf = s->get_full();
	sf->lock( tgt );
    }

    assert( s->get_state() == fs_waiting );
    s->set_owner( tgt );

    // We avoid races between the victim finishing and deleting the
    // stack frame that will become top and the thief who converts it to
    // a full frame by locking s and rechecking for emptiness of the
    // victim's deque. Rationale: it is necessary
    // to take a lock on the parent (s) before deleting a frame. Thus,
    // either the thief locks s first and new-top exists, or the victim
    // locks first and destroys new-top.

    LOG( id_pop_front, sf );
    return sf;
}

spawn_deque::spawn_deque() :
#if FF_MCS_MUTEX
    ff_tag_cur( 0 ),
#endif
    current(), top_parent( 0 ), popped( 0 ), top_parent_maybe_suspended( false )
#if PROFILE_SPAWN_DEQUE
#define INIT(x) , num_##x(0)
    INIT(pop_call_nfull)
    INIT(pop_success)
    INIT(pop_success_lock)
    INIT(pop_fail)
    INIT(steal_top)
    INIT(steal_top_sibling)
    INIT(steal_fail)
    INIT(steal_fail_sibling)
    INIT(rsib_steals)
    INIT(rsib_steals_success)
    INIT(cvt_pending)
#undef INIT
#endif
{
#if TIME_STEALING
    memset( &steal_acquire_time, 0, sizeof(pp_time_t) );
    memset( &steal_time_pop, 0, sizeof(pp_time_t) );
    memset( &steal_time_deque, 0, sizeof(pp_time_t) );
    memset( &steal_time_sibling, 0, sizeof(pp_time_t) );
    memset( &try_pop_lock_time, 0, sizeof(pp_time_t) );
    memset( &time_rchild_steal, 0, sizeof(time_rchild_steal) );
    memset( &time_rchild_steal_fail, 0, sizeof(time_rchild_steal) );
#endif
}

spawn_deque::~spawn_deque() {
    assert( deque.empty() && current.empty() && "Destructing non-empty deque" );
#if PROFILE_SPAWN_DEQUE
    dump_profile();
#endif
#if TIME_STEALING
#define SHOW(x) pp_time_print( &x, (char *)#x )
    SHOW( steal_acquire_time );
    SHOW( steal_time_pop );
    SHOW( steal_time_deque );
    SHOW( steal_time_sibling );
    SHOW( try_pop_lock_time );
    SHOW( time_rchild_steal );
    SHOW( time_rchild_steal_fail );
#undef SHOW
#endif
}

#if PROFILE_SPAWN_DEQUE
void
spawn_deque::dump_profile() const {
    std::cerr << "Spawn Deque " << this;
#define DUMP(x) std::cerr << "\n num_" << #x << '=' << num_##x
    DUMP(pop_call_nfull);
    DUMP(pop_success);
    DUMP(pop_success_lock);
    DUMP(pop_fail);
    DUMP(steal_top);
    DUMP(steal_top_sibling);
    DUMP(steal_fail);
    DUMP(steal_fail_sibling);
    DUMP(rsib_steals);
    DUMP(rsib_steals_success);
    DUMP(cvt_pending);
#undef DUMP
    std::cerr << '\n';
}
#endif

/*
 * @brief Steal the oldest call stack on this spawn deque.
 * The call stack consists of a series of zero or more stack frames
 * created by a call followed by a frame created by a spawn. The youngest
 * frame on the stack is transfered to the @a tgt spawn deque, the others
 * are not owned by any spawn deque (suspended). All stack frames are
 * converted to full frames.
 */
full_frame *
spawn_deque::steal_stack( spawn_deque * tgt, steal_method_t sm ) {
#if TIME_STEALING
    pp_time_start( &tgt->steal_acquire_time ); // Measures locked time
#endif
    if( !deque.try_lock( &tgt->steal_node ) )
	return 0;
#if TIME_STEALING
    pp_time_end( &tgt->steal_acquire_time ); // Measures locked time
    pp_time_start( &tgt->steal_time_pop ); // Measures locked time
#endif

    // Victim may repeatedly push and pop but the tail cannot change!
    // Before tail can change, he will block in the lock() from try_pop()
    // on an empty deque.
    stack_frame ** volatile new_top_loc = 0;

    // Take the oldest call stack, if any. Return it locked.
    full_frame * ff = deque.pop_front( tgt, (stack_frame ***)&new_top_loc );

#if TIME_STEALING
	pp_time_end( &tgt->steal_time_pop ); // Measures locked time
#endif
    if( ff ) {
#if TIME_STEALING
	pp_time_start( &tgt->steal_time_deque ); // Measures locked time
#endif
	// Determine new top of spawn deque
	// current.tail equals new_top if current was first pushed and
	// later popped, but it is a different value if current has not yet
	// been pushed. Really dirty things happen for random interleavings
	// of push_back() and pop_back() on the victim side and
	// deque.empty() and reading current.tail or *new_top on the thief side.
	// This portion is really not well synchronized, so we try until we
	// get it right :-(
	// The only guarantee we have is that current.tail and *new_top are
	// allocated stack_frames and that the victim will block in lock()
	// as soon as he tries to pop from an empty deque. When that happens,
	// current.tail holds the stack we are looking for...
	stack_frame * new_top;
	while( true ) {
	    new_top = deque.empty() ? current.tail : *new_top_loc;
	    if( new_top->get_parent() == ff->get_frame() )
		break;
	}
	LOG( id_new_top_sd, new_top );
	assert( new_top && "New top of spawn deque must be non-null" );
	new_top->convert_to_full();
	top_parent = ff;
	top_parent_maybe_suspended
	    = top_parent->get_frame()->get_state() == fs_suspended;
	SD_PROFILE_ON(tgt, steal_top);
#if TIME_STEALING
	pp_time_end( &tgt->steal_time_deque ); // Measures locked time
#endif

	// Next:
	// Youngest frame on the call stack is transferred to tgt
	// Other frames are disowned and suspended and converted to full frames
	// Move to target deque, obsoleting lock (push in current locks it)
	tgt->insert_stack( ff );
	ff->unlock( tgt );

	// Unlock deque
	deque.unlock( &tgt->steal_node );
    } else if( top_parent ) {
#if TIME_STEALING
	pp_time_start( &tgt->steal_time_sibling ); // Measures locked time
#endif
	// If we cannot steal the oldest call stack, then try to steal
	// a right sibling of the current stack.
	LOG( id_old_top_sd, top_parent );

	top_parent_maybe_suspended
	    = top_parent->get_frame()->get_state() == fs_suspended;
	if( top_parent->get_frame()->get_state() == fs_suspended ) { // in sync
	    // Release lock on deque and replace by lock on top_parent.
	    // We only need to prevent top_parent from being de-allocated
	    // while we are accessing its task graph. We have no further
	    // business with the victim spawn deque.
	    full_frame * tg_victim = top_parent;
	    tg_victim->lock( tgt );
	    deque.unlock( &tgt->steal_node );

	    if( pending_frame * qf = tgt->steal_rchild( tg_victim, sm ) ) {
		tgt->wakeup_steal( tg_victim, qf );
		SD_PROFILE_ON(tgt, steal_top_sibling);
	    } else {
		SD_PROFILE_ON(tgt, steal_fail_sibling);
	    }

	    tg_victim->unlock( tgt );
	} else
	    deque.unlock( &tgt->steal_node );
#if TIME_STEALING
	pp_time_end( &tgt->steal_time_sibling ); // Measures locked time
#endif
    } else
	deque.unlock( &tgt->steal_node );

    if( !ff ) {
	SD_PROFILE_ON(tgt, steal_fail);
	return 0;
    }

    LOG( id_steal_stack, ff );

    return ff;
}

void
spawn_deque::dump() const {
    //iolock();
    // std::cerr << *this << "\n";
    //iounlock();
}

/*
std::ostream & operator << ( std::ostream & os, const spawn_deque & sd ) {
    os << "Spawn Deque: " << &sd
       << ( sd.test_lock() ? " locked\n" : " not locked\n" );
    for( std::deque<stack_frame*>::const_reverse_iterator
	     I=sd.deque.rbegin(), E=sd.deque.rend(); I != E; ++I ) {
	os << "\tcall stack beginning with "
	   << *I << " parent: " << (*I)->get_parent() << "\n";
    }
    os << "\tcurrent: " << sd.current << " parent: "
       << (sd.current ? sd.current->get_parent() : 0) << "\n"; 
    return os;
}
*/

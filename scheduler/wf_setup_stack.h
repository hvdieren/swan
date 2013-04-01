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
#ifndef SETUP_STACK_H
#define SETUP_STACK_H

#include "swan_config.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "platform.h"
#include "wf_stack_frame.h"
#include "wf_spawn_deque.h"
#include "wf_worker.h"
#include "object.h"

#ifndef SUPPORT_GDB
#define SUPPORT_GDB 0
#endif // SUPPORT_GDB

#include "mangled.h"

void *
pending_frame::operator new ( size_t size ) {
    return worker_state::get_pf_allocator().allocate(1);
}

void
pending_frame::operator delete( void * p ) {
    worker_state::get_pf_allocator().deallocate( (pending_frame*)p, 1 );
}

void *
stack_frame::operator new ( size_t size ) {
    return worker_state::get_sf_allocator().allocate(1);
}

void
stack_frame::operator delete( void * p ) {
    // errs() << "dealloc stack " << p << "\n";
    assert( my_stack_frame() != (stack_frame *)p
	    && "Cannot free the stack we are executing on!" );
    worker_state::get_sf_allocator().deallocate( (stack_frame*)p, 1 );
}

#if IMPROVED_STUBS
template<bool (*ss_ptr)(stack_frame *, void (*)(void))>
bool
stack_frame::prevent_inlining_dir( stack_frame * child,
				   void (*fn_ptr)(void) ) {
#if  !defined( __APPLE__ )
    bool ret;
    __vasm__( "call %c3 \n\t"
	      : "=a"(ret)
	      : "D"(child), "S"(fn_ptr), "i"(ss_ptr)
	      : "memory" );
    return ret;
#else
    return prevent_inlining( child, fn_ptr, ss_ptr );
#endif
}

bool
stack_frame::prevent_inlining( stack_frame * child,
			       void (*fn_ptr)(void),
			       bool (*ss_ptr)(stack_frame *, void (*)(void)) ) {
    bool ret;
    __vasm__( "call *%%rdx \n\t"
	      : "=a"(ret)
	      : "D"(child), "S"(fn_ptr), "d"(ss_ptr)
	      : "memory" );
    return ret;
}

#else
template<bool (*ss_ptr)(stack_frame *, void (*)(void))>
bool
stack_frame::prevent_inlining_dir( stack_frame * child,
				   void (*fn_ptr)(void) ) {
    return prevent_inlining( child, fn_ptr, ss_ptr );
}
#endif

// Release all locks. Child remains unaccessible as it is in state
// fs_executing. This code may be inlined in split_stub_body().
void
stack_frame::split_ctrl_executing( stack_frame * child ) {
    // Update state
    child->set_state( fs_executing );

    // Push on deque - requires lock on deque
    spawn_deque * owner = child->get_owner();
    owner->push( child );
}

template<frame_create_t fcreate, typename TR, typename... Tn>
bool
stack_frame::split_stub( stack_frame * child,
			 void (*fn_ptr)(void) ) {
    intptr_t old_rsp;
    bool ret;
    bool (* const ss_ptr)(stack_frame *, void (*)(void))
	= &stack_frame::split_stub_body<fcreate,TR,Tn...>;

    // Save continuation frame in case we return by a steal
    // This must be done before moving to a new stack frame
    if( fcreate == fc_executing ) {
	child->get_parent()->saved_ebp = get_bp();
	child->get_parent()->save_continuation();
    } else
	child->saved_ebp = get_bp();

    // Stack pointer must be aligned
    assert( (intptr_t(child->get_stack_ptr(fcreate)) & 15) == 0 );

    // Make a construction that makes this function a non-leaf function
    // so that we can specify -momit-leaf-frame-pointer and have a frame
    // pointer here where we need it. Do this by returning from assembly
    // and calling fprintf() to signal error and make it non-leaf.
#if  !defined( __APPLE__ )
    __vasm__( "movq %%rbp,%0\n\t"
	      "movq %5,%%rsp\n\t"
	      "call %c2\n\t"
	      "movq %0,%%rsp\n\t"
	      "leaveq\n\t"
	      "ret\n\t"
	      : "=m"(old_rsp), "=&a"(ret)
	      : "i"(ss_ptr), "D"(child), "S"(fn_ptr), "g"(child->get_stack_ptr(fcreate))
	      : "memory" );
#else
    // Cached value *must* be stored on stack
    __vasm__( "movq %%rbp,%0\n\t"
	      "movq %5,%%rsp\n\t"
	      "call *%%rdx\n\t"
	      "movq %0,%%rsp\n\t"
	      "leaveq\n\t"
	      "ret\n\t"
	      : "=m"(old_rsp), "=&a"(ret)
	      : "d"(ss_ptr), "D"(child), "S"(fn_ptr), "g"(child->get_stack_ptr())
	      : "memory" );
#endif

    fprintf( stderr, "We should not get here! file=%s line=%u\n", __FILE__, __LINE__ );

    return ret;
}

// A bug is exposed with RSP_RELATIVE_ADDRESSING set to 0 for passing arguments
// to a function with signature <void *, prefixdep, pushdep>. Bug does not 
// happen with -O0 but it does with -O4.
#define RSP_RELATIVE_ADDRESSING 1

#if RSP_RELATIVE_ADDRESSING
fun_constexpr size_t region_offset_rsp( size_t idx, size_t size ) {
    return idx * sizeof(intptr_t) + ( (size + 15) & ~size_t(15) );
}
#endif

template<frame_create_t fcreate, typename TR, typename... Tn>
bool
stack_frame::split_stub_body( stack_frame * child,
			      void (*fn_ptr)(void) ) {

#if PROFILE_WORKER
    worker_state::tls()->get_profile_worker().num_split_body++;
#endif

#ifdef __x86_64__
    // Make room on stack for our own local control variables
    // as well as for the function arguments passed through memory.
    // Hope that gcc does not introduce code with rsp-relative offsets
    // (there isn't any reason why it should do that here).
    // Note no asm alternatives: the presence of a variant where
    // one of the constants is placed in a register allows correctness
    // in case of unoptmized code and speed in case of optimized code.
    // However, for even more speed, we will avoid using %rbx in case we
    // have constexpr support.
    var_constexpr size_t mem_size = platform_x86_64::count_mem_words<Tn...>();
    var_constexpr size_t disp = 4*sizeof(intptr_t); // multiple of 16 bytes
    var_constexpr size_t off = (mem_size+15) & ~size_t(15);
    intptr_t * region;

    // Make room on the stack and remember region as "rsp+off"
    if( off == 0 ) {
	__vasm__( "subq %1,%%rsp # make room for arguments\n"
		  "movq %%rsp,%0 # copy rsp to 'region'\n"
		  : "=r,r"(region)
		  : "i,?r"(disp) : );
    } else {
	__vasm__( "movq %%rsp,%0 # copy rsp to 'region'\n"
		  "subq %2,%%rsp # make room for arguments\n"
		  "subq %1,%0 # adjust 'region'\n"
		  : "=&r,&r"(region)
		  : "i,?r"(disp), "i,?r"(off+disp) : );
    }
   
    // Initialize local control variables
    region[0] = intptr_t(fn_ptr);
    region[1] = intptr_t(&child->cresult->result);
    region[2] = intptr_t(child->get_args_ptr());
    region[3] = intptr_t(child);

    if( fcreate == fc_executing ) {
	split_ctrl_executing( child ); // book-keeping, may be inlined
    } else {
	// book-keeping, must not be inlined
	split_ctrl_waiting( child );

	// Here all registers *may* be invalidated except for
	// rbp, rsp and the PIC register. We don't want to do the
	// clobber:
	// CLOBBER_CALLEE_SAVED_BUT1(); // Reload all registers
	// because that makes gcc save and restore all callee-saved
	// registers. Therefore, we designed the code such that all
	// local variables that we will need are saved in region[]
	// and we will reload them ourselves using a "free" register,
	// either rsp or rbx, depending on whether we have constexpr
	// functionality (rsp) or not (rbx)
    }

    // We may resume the current stack frame on a different thread than
    // where it started *AND* gcc may have voluntarily allocated region
    // to a callee-saved register, in which case it will be corrupted
    // by calls to  split_ctrl_XXX().
    // Hence, recalculate value for region.
#if RSP_RELATIVE_ADDRESSING
    // Do this because it is beneficial for performance !?
    // Move to %rdi is not as good as move to %rbx.
    // __vasm__( "movq %%rsp,%0\n\t" : "=b"(region) : : );
#else
    if( off > 0 ) {
	__vasm__( "movq %%rsp,%0\n\t"
		  "addq %1,%0\n\t"
		  : "=&b,&b"(region) : "i,r?"(off) : );
    } else {
	__vasm__( "movq %%rsp,%0\n\t" : "=b"(region) : : );
    }
#endif

    // Note: we require the region variable in a callee-saved register
    // because we don't want to place it in an argument-passing register,
    // and there are hardly any others. r12, r13, etc. would do equally,
    // but we cannot easily specify them using a constraint.

    intptr_t tmp1, tmp2;
    if( mem_size > 0 ) {
	// Reserve space on stack for arguments, but really we
	// need to modify rsp!!!
	// __vasm__( "movq 16(%%rbx),%%rdi # rdi is source of mem args\n"
#if RSP_RELATIVE_ADDRESSING
	var_constexpr size_t off_ = region_offset_rsp( 2, mem_size );
	__vasm__( "movq %c2(%%rsp),%%rdi # rdi is source of mem args\n\t"
		  "movq %%rsp,%%rsi # rsi is destination of mem args\n\t"
		  : "=D"(tmp1), "=S"(tmp2) : "i"(off_) : );
#else
	var_constexpr size_t off_ = 2 * sizeof(intptr_t);
	__vasm__( "movq %c3(%%rbx),%%rdi # rdi is source of mem args\n\t"
		  "movq %%rsp,%%rsi # rsi is destination of mem args\n\t"
		  : "=D"(tmp1), "=S"(tmp2) : "b"(region), "i"(off_) : );
#endif
	// hidden arguments are source and destination of buffers
	load_mem_args<Tn...>( (void*)tmp1, (void*)tmp2 );
    }

#if RSP_RELATIVE_ADDRESSING
    // Load the argument values in registers - inlined code
    var_constexpr size_t off2 = region_offset_rsp( 2, mem_size );
    __vasm__( "movq %c1(%%rsp),%%rdi # rdi is source of mem args\n\t"
	      : "=D"(tmp1) : "i"(off2) : );
	      // "=D"(tmp1) : : : ); // gcc 4.6.0 -O0 overwrites %rbx
#else
    // Load the argument values in registers - inlined code
    __vasm__( "movq 16(%%rbx),%%rdi # rdi is source of mem args\n\t"
	      : : "b"(region) : "%rdi" ); // gcc 4.6.0 -O0 overwrites %rbx, %rdx
	      // "=D"(tmp1) : : : );
#endif
    load_reg_args<Tn...>(); // hidden argument is start of argument list

#if RSP_RELATIVE_ADDRESSING
    var_constexpr size_t off0 = region_offset_rsp( 0, mem_size );
    __vasm__( "call *%c0(%%rsp) # spawn function \n\t"
	      : : "i"(off0): "%rax", "memory" );
#else
    // Do NOT reload rbx(region) here because load_reg_args<>()
    // won't touch it but gcc may touch the argument registers!
    __vasm__( "call *0(%%rbx) # spawn function \n\t"
	      : : : "%rax", "memory" );
#endif

    if( !std::is_void<TR>::value ) {
#if RSP_RELATIVE_ADDRESSING
	var_constexpr size_t off_ = region_offset_rsp( 1, mem_size );
	__vasm__( "movq %c0(%%rsp), %%rdi # pointer to result buffer\n\t"
		  : : "i"(off_) : "%rax", "%rdi", "memory" );
#else
	__vasm__( "movq 8(%%rbx), %%rdi # pointer to result buffer\n\t"
		  : : : "%rax", "%rdi", "memory" );
#endif

	// Store the return value in memory
	get_value_from_regs<TR>(); // hidden argument is memory address to store to
    }

    // reload child into register for call to split_return
    if( fcreate != fc_executing ) {
#if RSP_RELATIVE_ADDRESSING
	var_constexpr size_t off_ = region_offset_rsp( 3, mem_size );
	__vasm__( "movq %c1(%%rsp),%0\n\t" : "=r"( child ) : "i"(off_) : );
#else
	__vasm__( "movq 24(%%rbx),%0\n\t" : "=r"( child ) : : );
#endif
    }

    // We can skip this assuming there is a leaveq statement at the
    // end of the procedure AND we don't use any callee-save registers.
    __vasm__( "addq %0,%%rsp # remove room for arguments\n"
	      : : "i,?r"(off+disp) : );

    // Handle the return condition (stolen?)
    // This is a big piece of work, no point in inlining
    return split_return( child );

#else // i386
    // Controlled access to some local variables
    intptr_t new_bp[5];
    new_bp[0] = intptr_t(fn_ptr);
    new_bp[1] = intptr_t(&child->cresult->result);
    new_bp[2] = intptr_t(child->get_args_ptr());
    new_bp[3] = intptr_t(child);
    new_bp[4] = get_bp();


FAILURE: i386 version not supported any more;
FAILURE: we need (a.o.) to copy arguments to our stack;
    __vasm__( "movl %0, %%ebp # redirect stack \n\t"
	      "movl %1, %%esp # child->stack_ptr \n\t"
	      "movl %%esi, %%eax # argument: child \n\t"
	      "movl %3, %%edx # bootstrap? flag \n\t"
	      "call " STACK_FRAME_SPLIT_CTRL_MANGLED_NAME " # release locks\n\t"
	      "movl 0(%%ebp), %%edx\n\t"
	      "call *%%edx # spawn function \n\t"
	      "movl 4(%%ebp), %%edi\n\t"
	      : 
	      : "c"(new_bp), "S"(child), "g"(child->get_stack_ptr()), "i"(fcreate)
	      : /* "%ebp", */ "%edx", "%esp", "memory" );

    get_value_from_regs<TR>();
    
    // We have to manage the stack ourselves because GCC doesn't know
    // how little space there is between base pointer and stack pointer.
    // With -O4 we can call split_return( 0, child) after the assembly
    // statement but not with -O0, hence the call in assembly. Don't want
    // inline it anyway because we don't want GCC to allocate anything on the
    // current stack frame (not managed by GCC).
    __vasm__( "movl 12(%%ebp), %%eax\n\t"
	      "call " STACK_FRAME_SPLIT_RETURN_MANGLED_NAME " \n\t"
	      "movq 16(%%rbp),%%rbp # reset rbp (untested) \n\t"
	      : : : );
#endif
    return false; // not stolen
}

#endif // SETUP_STACK_H

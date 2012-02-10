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

#ifndef PLATFORM_X86_64_H
#define PLATFORM_X86_64_H

#include "config.h"

#include <cstdint>
#include <cstring>
#include <cassert>
#include <type_traits>
#include <algorithm>

#define __vasm__ __asm__ __volatile__

// Gcc below 4.6.0 doesn't recognize the constexpr keyword in all positions
// we use it.
#if __GNUC__ < 4 || ( __GNUC__ == 4 && __GNUC_MINOR__ < 6 )
#define WITH_CONSTEXPR 0
#define fun_constexpr static inline
#define var_constexpr const
#else
#define WITH_CONSTEXPR 1
#define fun_constexpr constexpr
#define var_constexpr constexpr
// #define WITH_CONSTEXPR 0
// #define fun_constexpr static inline
// #define var_constexpr const
#endif

// See comments in platform_i386.h

// #define GET_BP(var)  __vasm__( "movq %%rbp,%0\n\t" : "=g"(var) : : )
// #define SET_BP(var)  __vasm__( "movq %0,%%rbp\n\t" : : "g"(var) : )
// #define GET_SP(var)  __vasm__( "movq %%rsp,%0\n\t" : "=g"(var) : : )
// #define SET_SP(var)  __vasm__( "movq %0,%%rsp\n\t" : : "g"(var) : )
// #ifdef __PIC__
// #define GET_PR(var)  __vasm__( "movq %%rbx,%0\n\t" : "=g"(var) : : )
// #define SET_PR(var)  __vasm__( "movq %0,%%rbx\n\t" : : "g"(var) : )
// #else
// #define GET_PR(var)
// #define SET_PR(var)
// #endif

static inline intptr_t get_bp() __attribute__((always_inline));
static inline intptr_t get_sp() __attribute__((always_inline));
static inline intptr_t get_pr() __attribute__((always_inline));

//static inline void set_sp(intptr_t _sp) __attribute__((always_inline));

intptr_t get_bp() {
    register intptr_t _bp __asm__("%rbp");
    return _bp;
}

intptr_t get_sp() {
    register intptr_t _sp __asm__("%rsp");
    return _sp;
}

intptr_t get_pr() {
#ifdef __PIC__
    register intptr_t _pr __asm__("%rbx");
    return _pr;
#else
    return 0;
#endif
}

#define restore_sp(var)  do { __vasm__( "movq %0,%%rsp\n\t" : : "g"(var) : ); } while( 0 )
#define save_sp(var)  do { __vasm__( "movq %%rsp,%0\n\t" : "=g"(var) : : ); } while( 0 )


// This is really not fully supported by GCC.
// GCC does not correctly recognize r8-r15 in the clobber list, turning r14
// into st(6) for some weird reason. On one particular gcc (gcc-4.3.3-5ubuntu4)
// r14 corresponds to the internal number 43, which we can specify manually.
#if 1
#define CLOBBER_CALLEE_SAVED_BUT1() \
    __asm__ __volatile__( "" : : : "%rbx", "%rdi", "%rsi", "%rdx", "%rcx", "%r8", "%r9", "%r10", "%r11", "%r12", "%r13", "%14", "%r15", "43", "42" )
#define CLOBBER_CALLEE_SAVED_BUT2() \
    __asm__ __volatile__( "" : : : "%rbx", "%rdx", "%rcx", "%r8", "%r9", "%r12", "%r13", "%14", "%r15", "43" )
#define CLOBBER_CALLEE_SAVED_BUT3() \
    __asm__ __volatile__( "" : : : "%rbx", "%rcx", "%r8", "%r9", "%r12", "%r13", "%14", "%r15", "43" )
#else
// This attempt is modeled on bug reports on the GCC website. It should work
// but it does not.
#define CLOBBER_CALLEE_SAVED_BUT1() \
    register unsigned long _r8 asm("r8"); \
    register unsigned long _r9 asm("r9"); \
    register unsigned long _r12 asm("r12"); \
    register unsigned long _r13 asm("r13"); \
    register unsigned long _r14 asm("r14"); \
    __asm__ __volatile__( "" : "+r"(_r8), "+r"(_r9), "+r"(_r12), "+r"(_r13), "+r"(_r14): : "%rbx", "%rsi", "%rdx", "%rcx" )
#define CLOBBER_CALLEE_SAVED_BUT2() \
    register unsigned long _r8 asm("r8"); \
    register unsigned long _r9 asm("r9"); \
    register unsigned long _r12 asm("r12"); \
    register unsigned long _r13 asm("r13"); \
    register unsigned long _r14 asm("r14"); \
    __asm__ __volatile__( "" : "+r"(_r8), "+r"(_r9), "+r"(_r12), "+r"(_r13), "+r"(_r14): : "%rbx", "%rdx", "%rcx" )
#define CLOBBER_CALLEE_SAVED_BUT3() \
    register unsigned long _r8 asm("r8"); \
    register unsigned long _r9 asm("r9"); \
    register unsigned long _r12 asm("r12"); \
    register unsigned long _r13 asm("r13"); \
    register unsigned long _r14 asm("r14"); \
    __asm__ __volatile__( "" : "+r"(_r8), "+r"(_r9), "+r"(_r12), "+r"(_r13), "+r"(_r14): : "%rbx", "%rcx" )
#endif

#ifdef __PIC__
// Performance will be slightly better if we pass a stack_frame pointer
// and do movq offset(%0), %%rsp etc. Downside is that struct layout must be
// fixed.
#define LEAVE(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "movq %1, %%rbx\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"(bp), "r"(pr) : ); } while( 0 )

#define LEAVE_RETURN_ZERO(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "movq %1, %%rbx\n\t"		       \
	    "xorq %%rax, %%rax\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"(bp), "r"(pr) : ); } while( 0 )

#define LEAVE_RETURN_ONE(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "movq %1, %%rbx\n\t"		       \
	    "movq $1, %%rax\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"((bp)), "r"((pr)) : ); } while( 0 )
#else // !PIC
#define LEAVE(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"(bp) : ); } while( 0 )

#define LEAVE_RETURN_ZERO(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "xorq %%rax, %%rax\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"(bp) : ); } while( 0 )

#define LEAVE_RETURN_ONE(bp,pr) do { __asm__ __volatile__(	\
	    "movq %0, %%rsp\n\t"		       \
	    "movq $1, %%rax\n\t"		       \
	    "popq %%rbp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"((bp)) : ); } while( 0 )
#endif // PIC

// Use GCC builtins rather than inline assembly to improve portability somewhat
inline uint32_t
v_atomic_incr_long( volatile uint32_t * address ) {
    return __sync_fetch_and_add(address, 1);

}
inline uint32_t
v_atomic_decr_long( volatile uint32_t * address ) {
    return __sync_fetch_and_add(address, -1);

}

// Representing return values.
// Cases (documentation):
// * Integers (of any size up to 32 bits) and pointers are returned in the
//   %eax register.
// * Floating point values are returned in the 387 top-of-stack register, st(0).
// * Return values of type long long int are returned in %edx:%eax (the most
//   significant word in %edx and the least significant in %eax).
// * Returning a structure is complicated and rarely useful; try to avoid it.
//   (Note that this is different from returning a pointer to a structure.)
//   So we don't support return structures yet.
struct return_value {
    intptr_t data[2];

    inline return_value() { }

    template<typename T>
    inline const return_value & operator = ( const T & t ) {
	*reinterpret_cast<T *>( &data[0] ) = t;
	return *this;
    }

    template<typename T>
    inline const T & get_value() const {
	return *reinterpret_cast<const T *>( &data[0] );
    }
};


template<typename T>
static inline
typename std::enable_if<std::is_void<T>::value>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
static inline
typename std::enable_if<std::is_integral<T>::value
			|| std::is_pointer<T>::value
			|| std::is_enum<T>::value>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
static inline
typename std::enable_if<std::is_floating_point<T>::value>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
static inline
typename std::enable_if<(std::is_class<T>::value
			 && std::has_trivial_default_constructor<T>::value
			 && std::has_trivial_destructor<T>::value)>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
typename std::enable_if<std::is_void<T>::value>::type
get_value_from_regs() {
    // Void requires no actions
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value
			|| std::is_pointer<T>::value
			|| std::is_enum<T>::value>::type
get_value_from_regs() {
    if( sizeof(T) <= sizeof(intptr_t) ) {
	__vasm__( "movq %%rax, (%%rdi) \n\t" : : : "memory" );
    } else {
	__vasm__( "movq %%rax, (%%rdi) \n\t"
		  "movq %%rdx, 8(%%rdi) \n\t" : : : "memory" );
    }
}

template<typename T>
static inline
typename std::enable_if<std::is_floating_point<T>::value>::type
get_value_from_regs() {
    // Only implement for the space we provide. Should always be ok on x86-64:
    // intptr_t is 8 bytes, long double is 16 bytes.
    static_assert( sizeof(T) <= 2*sizeof(intptr_t),
		   "Structures of at most 2 intptr_t allowed in return values");
    if( std::is_same<T, float>::value )
	__vasm__( "movss %%xmm0, (%%rdi) \n\t" : : : "memory" );
    else if( std::is_same<T, double>::value )
	__vasm__( "movdqu %%xmm0, (%%rdi) \n\t" : : : "memory" );
    else if( std::is_same<T, long double>::value )
	// AMD x86-64 ABI and gcc map long double to 80-bit x87 format.
	__vasm__( "stp (%%rdi) \n\t" : : : "memory" );
    else
	abort();
}

template<typename T>
static
typename std::enable_if<(std::is_class<T>::value
			 && std::has_trivial_default_constructor<T>::value
			 && std::has_trivial_destructor<T>::value)>::type
get_value_from_regs() {
    // only implement for the space we provide
    static_assert( sizeof(T) <= 2*sizeof(intptr_t),
		   "Structures of at most 2 intptr_t allowed in return values");
    if( sizeof(T) <= sizeof(intptr_t) ) {
	__vasm__( "movq %%rax, (%%rdi) \n\t" : : : "memory" );
    } else {
	__vasm__( "movq %%rax, (%%rdi) \n\t"
		  "movq %%rdx, 8(%%rdi) \n\t" : : : "memory" );
    }
}

template<typename T>
fun_constexpr
typename std::enable_if<!std::is_floating_point<T>::value, size_t>::type
abi_arg_size();

template<typename T>
fun_constexpr
typename std::enable_if<std::is_floating_point<T>::value, size_t>::type
abi_arg_size();

// Round natural size up to a multiple of 8
template<typename T>
fun_constexpr
typename std::enable_if<!std::is_floating_point<T>::value, size_t>::type
abi_arg_size() {
    return (sizeof(T)+size_t(7)) & ~size_t(7);
}

// Floating-point values go into SSE registers and we allocate by
// convention 16 bytes in our argument buffer for them.
template<typename T>
fun_constexpr
typename std::enable_if<std::is_floating_point<T>::value, size_t>::type
abi_arg_size() {
    return 16; // SSE class
}

// Get the size of the memory needed to store the arguments in bytes.
inline size_t arg_size() { return 0; }

template<typename T, typename... Tn>
inline size_t arg_size(T a0, Tn... an) {
    return abi_arg_size<T>() + arg_size( an... );
}

template<typename... Tn>
struct arg_num_counter;

template<>
struct arg_num_counter<> {
    static const size_t value = 0;
};

template<typename T, typename... Tn>
struct arg_num_counter<T, Tn...> {
    static const size_t value = 1 + arg_num_counter<Tn...>::value;
};

template<typename... Tn>
inline size_t arg_num() {
    return arg_num_counter<Tn...>::value;
};

namespace platform_x86_64 {

// The INTEGER class of operands.
template<typename T>
struct is_integer_class
    : std::integral_constant<bool, std::is_integral<T>::value
			     || std::is_pointer<T>::value> {
};

// Default case, pass through memory. Memory arguments have already been
// placed in the proper location in the argument buffer by copy_args(),
// so the load() function is empty.
// We do not exactly support passing structures as arguments, because the
// ABI makes a distinction between trivial structs passed in registers and
// non-trivial or large trivial structs passed by implicit reference. The
// latter case is tricky to implement (maybe with rvalue references &&?).
// For specific types in the runtime we have an interface to declare the
// members of the struct, such that we can check for trivial structs and
// what registers they fit in. Currently an interface for one-member structs
// is implemented.
template<size_t ireg, size_t freg, size_t loff, typename T,
	 typename Enable = void>
struct arg_passing {
    static const bool in_reg = false;
    static const size_t ibump = 0;
    static const size_t fbump = 0;
    static const size_t lbump = 0;

    static inline void load() __attribute__((always_inline)) { }

    // In general, passing structs directly is not allowed!
    static_assert( !std::is_class<T>::value,
		   "x86-64 calling convention: Should not pass structs "
		   "directly as function arguments!" );
};

// Other cases: template specializations on the integer argument position
// number. To save typing, generate the template instantiations with a macro
// We could specialize to smaller sizes of values by movb, movl, etc.
// but that would not change correctness because all elements are 8-byte
// aligned anyway.
#define X86_64_ARG_INT_CASE(nth,name)					\
    template<size_t freg, size_t loff, typename T>			\
    struct arg_passing<nth, freg, loff, T,				\
		       typename std::enable_if<is_integer_class<T>	\
					       ::value>::type > {	\
	static const bool in_reg = true;				\
	static const size_t ibump = 1;					\
	static const size_t fbump = 0;					\
	static const size_t lbump = 8;					\
									\
	static inline void load() __attribute__((always_inline)) {	\
	    __vasm__ ( "mov %c0(%%rax), %" name " \n\t" : : "i"(loff) : ); \
	}								\
    }

X86_64_ARG_INT_CASE(0,"%rdi");
X86_64_ARG_INT_CASE(1,"%rsi");
X86_64_ARG_INT_CASE(2,"%rdx");
X86_64_ARG_INT_CASE(3,"%rcx");
X86_64_ARG_INT_CASE(4,"%r8" );
X86_64_ARG_INT_CASE(5,"%r9" );

// Other cases: template specializations on the SSE argument position
// number. To save typing, generate the template instantiations with a macro
#define X86_64_ARG_SSE_CASE(nth,name)					\
    template<size_t ireg, size_t loff, typename T>			\
    struct arg_passing<ireg, nth, loff, T,				\
		       typename std::enable_if<std::is_floating_point<T>\
					       ::value>::type > {	\
	static const bool in_reg = true;				\
	static const size_t ibump = 0;					\
	static const size_t fbump = 1;					\
	static const size_t lbump = 16;					\
									\
	static inline void load() __attribute__((always_inline)) {	\
	    __vasm__ ( "movsd %c0(%%rax), %" name " \n\t" : : "i"(loff) : ); \
	}								\
    }

// only good for float and double!
X86_64_ARG_SSE_CASE(0,"%xmm0");
X86_64_ARG_SSE_CASE(1,"%xmm1");
X86_64_ARG_SSE_CASE(2,"%xmm2");
X86_64_ARG_SSE_CASE(3,"%xmm3");
X86_64_ARG_SSE_CASE(4,"%xmm4");
X86_64_ARG_SSE_CASE(5,"%xmm5");
X86_64_ARG_SSE_CASE(6,"%xmm6");
X86_64_ARG_SSE_CASE(7,"%xmm7");

// Some special cases of structs (incomplete - allows only one field
// and assumes that this field is in the integer class)
// These trivial structs are passed in register(s).

// Default case: struct with 1 member, passed by implicit reference.
// Trigger a compile-time assertion failure. We inherit from arg_passing<>
// with a scalar to limit compiler errors on non-existing template members.
// Do not use arg_passing_struct1 on non-structure things!
template<size_t ireg, size_t freg, size_t loff, typename T, typename M,
	 typename Enable = void>
struct arg_passing_struct1 : arg_passing<100,100,0,long> {
    // In general, passing structs (with 1 member) directly is not allowed!
    static_assert( !std::is_class<T>::value,
		   "x86-64 calling convention: Should not pass 1-member "
		   "structs directly as function arguments!" );
};

// Trivial struct
template<size_t ireg, size_t freg, size_t loff, typename T, typename M>
struct arg_passing_struct1<
    ireg, freg, loff, T, M,
    typename std::enable_if<std::is_class<T>::value
			    && std::has_trivial_default_constructor<T>::value
			    && std::has_trivial_destructor<T>::value
			    && sizeof(T) == sizeof(M)
			    >::type >
    : public arg_passing<ireg, freg, loff, M> {
};

// Default case: struct with 2 member, passed by implicit reference.
// Trigger a compile-time assertion failure. We inherit from arg_passing<>
// with a scalar to limit compiler errors on non-existing template members.
// Do not use arg_passing_struct2 on non-structure things!
template<size_t ireg, size_t freg, size_t loff, typename T, typename M1,
	 typename M2, typename Enable = void>
struct arg_passing_struct2 : arg_passing<100,100,0,long> {
    // In general, passing structs (with 1 member) directly is not allowed!
    static_assert( !std::is_class<T>::value,
		   "x86-64 calling convention: Should not pass 2-member "
		   "structs directly as function arguments!" );
};

// Trivial struct - 2 non-static data members. Does not implement packing
// small values in single 8-word (e.g. struct { short; short; }).
template<size_t ireg, size_t freg, size_t loff, typename T,
	 typename M1, typename M2>
struct arg_passing_struct2<
    ireg, freg, loff, T, M1, M2,
    typename std::enable_if<std::is_class<T>::value
			    && std::has_trivial_default_constructor<T>::value
			    && std::has_trivial_destructor<T>::value
			    // What about alignment (e.g. char + long)
			    && sizeof(T) == sizeof(M1) + sizeof(M2)
			    >::type > {
    typedef arg_passing<ireg, freg, loff, M1> arg_pass1;
    typedef arg_passing<ireg+arg_pass1::ibump, freg+arg_pass1::fbump,
			loff+arg_pass1::lbump, M2> arg_pass2;

    static const bool in_reg = arg_pass1::in_reg && arg_pass2::in_reg;
    static const size_t ibump =
	in_reg ? arg_pass1::ibump + arg_pass2::ibump : 0;
    static const size_t fbump =
	in_reg ? arg_pass1::fbump + arg_pass2::fbump : 0;
    static const size_t lbump =
	in_reg ? arg_pass1::lbump + arg_pass2::lbump : 0;

    static inline void load() __attribute__((always_inline)) {
	// Only load in registers if both are in registers.
	if( in_reg ) {
	    arg_pass1::load();
	    arg_pass2::load();
	}
    }
};

// Default case: struct with 3 members, passed by implicit reference.
// Trigger a compile-time assertion failure. We inherit from arg_passing<>
// with a scalar to limit compiler errors on non-existing template members.
// Do not use arg_passing_struct2 on non-structure things!
template<size_t ireg, size_t freg, size_t loff, typename T, typename M1,
	 typename M2, typename M3, typename Enable = void>
struct arg_passing_struct3 : arg_passing<100,100,0,long> {
    // In general, passing structs (with 1 member) directly is not allowed!
    static_assert( !std::is_class<T>::value,
		   "x86-64 calling convention: Should not pass 3-member "
		   "structs directly as function arguments!" );
};

// Trivial struct - 3 non-static data members. Does not implement packing
// small values in single 8-word (e.g. struct { short; short; }).
template<size_t ireg, size_t freg, size_t loff, typename T,
	 typename M1, typename M2, typename M3>
struct arg_passing_struct3<
    ireg, freg, loff, T, M1, M2, M3,
    typename std::enable_if<std::is_class<T>::value
			    && std::has_trivial_default_constructor<T>::value
			    && std::has_trivial_destructor<T>::value
			    // What about alignment (e.g. char + long)
			    && sizeof(T) == sizeof(M1) + sizeof(M2) + sizeof(M3)
			    >::type > {
    typedef arg_passing<ireg, freg, loff, M1> arg_pass1;
    typedef arg_passing<ireg+arg_pass1::ibump, freg+arg_pass1::fbump,
			loff+arg_pass1::lbump, M2> arg_pass2;
    typedef arg_passing<ireg+arg_pass2::ibump, freg+arg_pass2::fbump,
			loff+arg_pass2::lbump, M3> arg_pass3;

    static const bool in_reg = arg_pass1::in_reg && arg_pass2::in_reg
    && arg_pass3::in_reg;
    static const size_t ibump =
	in_reg ? arg_pass1::ibump + arg_pass2::ibump
	+ arg_pass3::ibump : 0;
    static const size_t fbump =
	in_reg ? arg_pass1::fbump + arg_pass2::fbump
	+ arg_pass3::fbump : 0;
    static const size_t lbump =
	in_reg ? arg_pass1::lbump + arg_pass2::lbump
	+ arg_pass3::lbump : 0;

    static inline void load() __attribute__((always_inline)) {
	// Only load in registers if both are in registers.
	if( in_reg ) {
	    arg_pass1::load();
	    arg_pass2::load();
	    arg_pass3::load();
	}
    }
};

// Default case: struct with 4 members, passed by implicit reference.
// Trigger a compile-time assertion failure. We inherit from arg_passing<>
// with a scalar to limit compiler errors on non-existing template members.
// Do not use arg_passing_struct2 on non-structure things!
template<size_t ireg, size_t freg, size_t loff, typename T, typename M1,
	 typename M2, typename M3, typename M4, typename Enable = void>
struct arg_passing_struct4 : arg_passing<100,100,0,long> {
    // In general, passing structs (with 1 member) directly is not allowed!
    static_assert( !std::is_class<T>::value,
		   "x86-64 calling convention: Should not pass 4-member "
		   "structs directly as function arguments!" );
};

// Trivial struct - 4 non-static data members. Does not implement packing
// small values in single 8-word (e.g. struct { short; short; }).
template<size_t ireg, size_t freg, size_t loff, typename T,
	 typename M1, typename M2, typename M3, typename M4>
struct arg_passing_struct4<
    ireg, freg, loff, T, M1, M2, M3, M4,
    typename std::enable_if<std::is_class<T>::value
			    && std::has_trivial_default_constructor<T>::value
			    && std::has_trivial_destructor<T>::value
			    // What about alignment (e.g. char + long)
			    && sizeof(T) == sizeof(M1) + sizeof(M2) + sizeof(M3) + sizeof(M4)
			    >::type > {
    typedef arg_passing<ireg, freg, loff, M1> arg_pass1;
    typedef arg_passing<ireg+arg_pass1::ibump, freg+arg_pass1::fbump,
			loff+arg_pass1::lbump, M2> arg_pass2;
    typedef arg_passing<ireg+arg_pass2::ibump, freg+arg_pass2::fbump,
			loff+arg_pass2::lbump, M3> arg_pass3;
    typedef arg_passing<ireg+arg_pass3::ibump, freg+arg_pass3::fbump,
			loff+arg_pass3::lbump, M4> arg_pass4;

    static const bool in_reg = arg_pass1::in_reg && arg_pass2::in_reg
	&& arg_pass3::in_reg && arg_pass4::in_reg;
    static const size_t ibump =
	in_reg ? arg_pass1::ibump + arg_pass2::ibump
	+ arg_pass3::ibump + arg_pass4::ibump : 0;
    static const size_t fbump =
	in_reg ? arg_pass1::fbump + arg_pass2::fbump
	+ arg_pass3::fbump + arg_pass4::fbump : 0;
    static const size_t lbump =
	in_reg ? arg_pass1::lbump + arg_pass2::lbump
	+ arg_pass3::lbump + arg_pass4::lbump : 0;

    static inline void load() __attribute__((always_inline)) {
	// Only load in registers if both are in registers.
	if( in_reg ) {
	    arg_pass1::load();
	    arg_pass2::load();
	    arg_pass3::load();
	    arg_pass4::load();
	}
    }
};

// Count how many 8-words of memory arguments there are
// Always inline because called from load_reg_args() function.
template<size_t ireg, size_t freg>
fun_constexpr size_t count_mem_words() __attribute__((pure));
template<size_t ireg, size_t freg, typename T0, typename... Tn>
fun_constexpr size_t count_mem_words() __attribute__((pure));
template<typename... Tn>
fun_constexpr size_t count_mem_words() __attribute__((pure));

template<size_t ireg, size_t freg>
fun_constexpr size_t count_mem_words() { return 0; }

template<size_t ireg, size_t freg, typename T0, typename... Tn>
fun_constexpr size_t count_mem_words() {
    return ( arg_passing<ireg, freg, 0, T0>::in_reg ? 0 : abi_arg_size<T0>() )
	+ count_mem_words<ireg+arg_passing<ireg, freg, 0, T0>::ibump, freg+arg_passing<ireg, freg, 0, T0>::fbump, Tn...>();
    // typedef arg_passing<ireg, freg, 0, T0> arg_pass;
    // return ( arg_pass::in_reg ? 0 : abi_arg_size<T0>() )
	// + count_mem_words<ireg+arg_pass::ibump, freg+arg_pass::fbump, Tn...>();
}

template<typename... Tn>
fun_constexpr size_t count_mem_words() {
    return count_mem_words<0, 0, Tn... >();
}


// Copy the arguments to a memory buffer.
// Note: little-endian assumptions
template<typename T>
inline void copy( char *& tgt, T t ) {
    size_t size = abi_arg_size<T>();
    *reinterpret_cast<T *>(tgt) = t;
    tgt += size;
}

template<size_t ireg, size_t freg>
static inline void copy_args2( char *& reg, char *& mem ) { }

template<size_t ireg, size_t freg, typename T0, typename... Tn>
static inline void copy_args2( char *& reg, char *& mem, T0 a0, Tn... an ) {
    typedef arg_passing<ireg, freg, 0, T0> arg_pass;
    if( arg_pass::in_reg )
	copy( reg, a0 );
    else
	copy( mem, a0 );
    copy_args2<ireg+arg_pass::ibump, freg+arg_pass::fbump, Tn...>
	( reg, mem, an... );
}

template<typename... Tn>
static inline void copy_args( char *& tgt, Tn... an ) {
    size_t mem_words = count_mem_words<Tn...>();
    char * reg_tgt = tgt+mem_words;
    copy_args2<0,0,Tn...>( reg_tgt, tgt, an... );
}




template<size_t ireg, size_t freg, size_t loff>
static inline void load_regs() __attribute__((always_inline));

template<size_t ireg, size_t freg, size_t loff>
void load_regs() { }

template<size_t ireg, size_t freg, size_t loff, typename T0, typename... Tn>
static inline void load_regs() __attribute__((always_inline));

template<size_t ireg, size_t freg, size_t loff, typename T0, typename... Tn>
void load_regs() {
    typedef arg_passing<ireg, freg, loff, T0> arg_pass;
    arg_pass::load(); // mute if pass in memory class
    load_regs<ireg+arg_pass::ibump, freg+arg_pass::fbump,
	loff+arg_pass::lbump, Tn...>();
}

template<typename... Tn>
static inline void load_reg_args() __attribute__((always_inline));

// It is assumed that rsp points to the argument buffer when calling this
// function and that the function epilog pushes 16 bytes on the stack:
// 8 for the return address and 8 for the saved rbp.
// This function and all the load_regs<>() callee's must absolutely be
// inlined for correctness.
// When optimizing, count_mem_words<>() evaluates to a constant, so a slightly
// faster approach would be "add %c0, %%rbp" with an "i" constraint and the
// macros above would use rbp instead of rax. This saves a constant-to-reg move.
// Or even better, with C++0x constexpr support we could add the
// count_mem_words<>() value to the loff value in the load_regs<>() template
// argument.
template<typename... Tn>
void load_reg_args() {
#if WITH_CONSTEXPR
    __vasm__( "movq %%rdi, %%rax\n\t" : : : ); // use different reg?
    var_constexpr size_t mem_words = count_mem_words<Tn...>();
    load_regs<0, 0, 0+mem_words, Tn... >();
#else
    if( size_t mem_words = count_mem_words<Tn...>() )
	__vasm__( "add %%rdi, %0\n\t" : : "a"(mem_words) : );
    else
	__vasm__( "movq %%rdi, %%rax\n\t" : : : ); // use different reg?
    load_regs<0, 0, 0, Tn... >();
#endif
}

// %rdi points to buffer source
// %rsi points to buffer target
template<typename... Tn>
void load_mem_args() {
    void * src; // __asm__("%rdi");
    void * dst; // __asm__("%rsi");
    __vasm__( "movq %%rdi, %0\n\t"
	      "movq %%rsi, %1\n\t"
	      : "=g"(dst), "=g"(src) : : );

    // Indicate to compiler that memmove clobbers %rax
    size_t num_words = count_mem_words<Tn...>();
    memcpy( src, dst, num_words );
    /*
    while( num_words > 0 ) {
	// With constexpr facilities, we should be able to use %c0(%%rdi)
	// etc, potentially with a recursive template call to avoid any
	// explicit (register-based) address calculations.
	__vasm__( "movntdqa (%%rdi),%%xmm0\n\t"
		  "addq %%rdi,16\n\t"
		  "movdqa %%xmm0,(%%rsi)\n\t"
		  "addq %%rsi,16\n\t"
		  : : : "%rdi", "%rsi", "%xmm0", "memory" );
	num_words -= 16;
    }
    */
}

// Auxiliary function to compute the offset of an argument in the
// argument block.
// This should be merged with copy_args2 and also with arg_pass, e.g.
// by defining a struct ablk_offset { size_t reg_off, mem_off; } and
// passing this along instead of sometimes pointers, sometimes offsets.
// Or extend the templates to 4 arguments: iref, freg, regoffinmem, memoffinmem
template<size_t ireg, size_t freg>
static inline size_t offset_of2( size_t & reg_off, size_t & mem_off,
				 size_t argnum ) {
    assert( 0 && "argnum out of range" );
    return size_t(~0)>>1; // likely bogus value
}

template<size_t ireg, size_t freg, typename T0, typename... Tn>
static inline size_t offset_of2( size_t & reg_off, size_t & mem_off,
				 size_t argnum ) {
    typedef arg_passing<ireg, freg, 0, T0> arg_pass;
    if( argnum == 0 )
	return arg_pass::in_reg ? reg_off : mem_off;
    size_t size = abi_arg_size<T0>();
    if( arg_pass::in_reg )
	reg_off += size;
    else
	mem_off += size;
    return offset_of2<ireg+arg_pass::ibump, freg+arg_pass::fbump, Tn...>
	( reg_off, mem_off, argnum-1 );
}

template<typename... Tn>
static inline size_t offset_of( size_t argnum ) {
    size_t reg_off = count_mem_words<Tn...>();
    size_t mem_off = 0;
    return offset_of2<0, 0, Tn...>( reg_off, mem_off, argnum );
}


} // namespace x86_64

//
// Public interfaces
//

template<typename... Tn>
inline void copy_args( char *& tgt, Tn... an ) {
    platform_x86_64::copy_args( tgt, an... );
}

template<typename... Tn>
void load_reg_args() {
    platform_x86_64::load_reg_args<Tn...>();
}

template<typename... Tn>
void load_mem_args() {
    platform_x86_64::load_mem_args<Tn...>();
}

template<typename... Tn>
static inline size_t offset_of( size_t argnum ) {
    return platform_x86_64::offset_of<Tn...>( argnum );
}

template<size_t ireg, size_t freg>
struct arg_locator {
    size_t reg_off, mem_off;

    // TODO: create argument-less public constructor and
    // with-argument private/friend constructor
    arg_locator( size_t reg_off_ = 0, size_t mem_off_ = 0 )
	: reg_off( reg_off_ ), mem_off( mem_off_ ) { }

    template<typename T>
    struct arg_locator_next {
	typedef platform_x86_64::arg_passing<ireg, freg, 0, T> arg_pass;
	typedef arg_locator<ireg+arg_pass::ibump, freg+arg_pass::fbump> type;
    };

    template<typename T>
    typename arg_locator_next<T>::type
    step() const {
	var_constexpr size_t size = abi_arg_size<T>();
	return arg_locator_next<T>::arg_pass::in_reg
	    ? typename arg_locator_next<T>::type( reg_off+size, mem_off )
	    : typename arg_locator_next<T>::type( reg_off, mem_off+size );
    }

    template<typename T>
    size_t get() const {
	typedef platform_x86_64::arg_passing<ireg, freg, 0, T> arg_pass;
	size_t off = arg_pass::in_reg ? reg_off : mem_off;
	return off;
    }
};

template<typename... Tn>
static inline arg_locator<0,0> create_arg_locator() {
    var_constexpr size_t mem_words = platform_x86_64::count_mem_words<Tn...>();
    return arg_locator<0,0>( mem_words, 0 );
}


#endif // PLATFORM_X86_64_H

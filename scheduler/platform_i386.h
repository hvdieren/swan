// -*- c++ -*-
#ifndef PLATFORM_I386_H
#define PLATFORM_I386_H

#include <cstdint>
#include <type_traits>

#define __vasm__ __asm__ __volatile__

// Some experience with C++ has led us to define functions that determine
// and potentially set the frame pointer, stack pointer and PIC register.
// These functions are always inlined (by GCC) because of the always_inline
// attribute, also at -O0 optimization level. Furthermore, they are slightly
// faster than the macros below because with the macros below it always takes
// two instructions to store one of the registers to memory (first copy to
// other register, then store to memory) even at -O4, while the trick
// with variables declared as register allows a single assembly instruction.
// Macros like
// #define GET_SP() ({ register intptr_t _sp __asm__("%esp"); _sp; })
// would also do the trick but are also GCC specific.

// Alternative macros.
// #define GET_BP(var)  __vasm__( "movl %%ebp,%0\n\t" : "=g"(var) : : )
// #define GET_SP(var)  __vasm__( "movl %%esp,%0\n\t" : "=g"(var) : : )
// #define GET_PR(var)  __vasm__( "movl %%ebx,%0\n\t" : "=g"(var) : : )
// #define SET_SP(var)  __vasm__( "movl %0,%%esp\n\t" : "=g"(var) : : )

static inline intptr_t get_bp() __attribute__((always_inline));
static inline intptr_t get_sp() __attribute__((always_inline));
static inline intptr_t get_pr() __attribute__((always_inline));

intptr_t get_bp() {
    register intptr_t _bp __asm__("%ebp");
    return _bp;
}

intptr_t get_sp() {
    register intptr_t _sp __asm__("%esp");
    return _sp;
}

intptr_t get_pr() {
#ifdef __PIC__
    register intptr_t _pr __asm__("%ebx");
    return _pr;
#else
    return 0;
#endif
}

#define restore_sp(var)  do { __vasm__( "movl %0,%%esp\n\t" : : "g"(var) : ); } while( 0 )
#define save_sp(var)  do { __vasm__( "movl %%esp,%0\n\t" : "=g"(var) : : ); } while( 0 )


static inline intptr_t get_st0() __attribute__((always_inline));
static inline intptr_t get_st1() __attribute__((always_inline));

intptr_t get_st0() {
    register intptr_t _st0 __asm__("st(0)");
    return _st0;
}

intptr_t get_st1() {
    register intptr_t _st1 __asm__("st(1)");
    return _st1;
}


#define CLOBBER_CALLEE_SAVED_BUT1() \
    __asm__ __volatile__( "" : : : "%edx", "%ecx", "%esi", "%edi" )
#define CLOBBER_CALLEE_SAVED_BUT2() \
    __asm__ __volatile__( "" : : : "%ecx", "%esi", "%edi" )
#define CLOBBER_CALLEE_SAVED_BUT3() \
    __asm__ __volatile__( "" : : : "%esi", "%edi" )

// Performance will be slightly better if we pass a stack_frame pointer
// and do movl offset(%0), %%esp etc. Downside is that the offsets must be
// fixed.
#define LEAVE(bp,pr) do { __asm__ __volatile__(	       \
	    "movl %0, %%esp\n\t"		       \
	    "movl %1, %%ebx\n\t"		       \
	    "popl %%ebp\n\t"			       \
	    "ret\n\t"				       \
	    : : "r"(bp), "r"(pr) : ); } while( 0 )

#define LEAVE_RETURN_ZERO(bp,pr) do { __asm__ __volatile__(	\
	    "movl %0, %%esp\n\t"				\
	    "movl %1, %%ebx\n\t"				\
	    "xorl %%eax, %%eax\n\t"				\
	    "popl %%ebp\n\t"					\
	    "ret\n\t"						\
	    : : "r"(bp), "r"(pr) : ); } while( 0 )

#define LEAVE_RETURN_ONE(bp,pr) do { __asm__ __volatile__(	\
	    "movl %0, %%esp\n\t"				\
	    "movl %1, %%ebx\n\t"				\
	    "movl $1, %%eax\n\t"				\
	    "popl %%ebp\n\t"					\
	    "ret\n\t"						\
	    : : "r"((bp)), "r"((pr)) : ); } while( 0 )

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
			|| std::is_pointer<T>::value>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
static inline
typename std::enable_if<std::is_floating_point<T>::value>::type
get_value_from_regs() __attribute__((always_inline));

template<typename T>
typename std::enable_if<std::is_void<T>::value>::type
get_value_from_regs() {
    // void return type requires no actions
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value
			|| std::is_pointer<T>::value>::type
get_value_from_regs() {
    if( sizeof(T) <= sizeof(intptr_t) ) {
	__vasm__( "movl %%eax, (%%edi) \n\t" : : : "memory" );
    } else {
	__vasm__( "movl %%eax, (%%edi) \n\t"
		  "movl %%edx, 4(%%edi) \n\t" : : : "memory" );
    }
}

// Get the size of the memory needed to store the arguments in bytes.
inline size_t arg_size() { return 0; }

template<typename T, typename... Tn>
inline size_t arg_size(T a0, Tn... an) {
    return sizeof(T) + arg_size( an... );
}

// Copy the arguments to a memory buffer.
// Note: little-endian assumptions
template<typename T>
inline void copy( char *& tgt, T t ) {
    size_t size = sizeof(T);
    *reinterpret_cast<T *>(tgt) = t;
    tgt += size;
}

inline void copy_args( char *& tgt ) { }

template<typename T, typename... Tn>
inline void copy_args( char *& tgt, T a0, Tn... an ) {
    copy( tgt, a0 );
    copy_args( tgt, an... );
}

template<typename... Tn>
inline void load_reg_args( void ) {
}

#endif // PLATFORM_I386_H

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

#ifndef STACK_FRAME_H
#define STACK_FRAME_H

#include "config.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <type_traits>

#include "platform.h"
#include "alc_objtraits.h"
#include "lock.h"
#include "wf_task.h"
#include "wf_frames.h"
#include "object.h"

class spawn_deque;
class worker_state;

enum empty_deque_condition_t {
    edc_bootstrap = 0,
    edc_spawn, // GCC considers case 1 first in a switch and spawn is hottest...
    edc_call,
    edc_sync
};

enum frame_state_t {
    fs_dummy,      // only the dummy root frame is in state fs_dummy
    fs_waiting,    // sitting deeper on a spawn deque
    fs_executing,  // currently executing, youngest in spawn deque
    fs_suspended,  // not inside any spawn deque (owner must be null)
    fs_pending     // pending frame, waiting to become ready
};

enum frame_create_t {
    fc_executing,  // create frame and start executing
    fc_waiting     // create frame and place it in waiting state on deque
};

enum steal_method_t {
    sm_release = 0,
    sm_sync,
    sm_focussed,
    sm_random,
    sm_N
};

//----------------------------------------------------------------------
// This struct helps to compute the size of a base class, assuming that it may
// be empty, in which case sizeof(T) >= 1 but T's contribution in sizeof(Derived:T)
// is zero (empty base class optimization).
//----------------------------------------------------------------------
template<typename T, bool is_empty_ = false>
struct inherited_size_helper {
    static const size_t value = sizeof(T);
};

template<typename T>
struct inherited_size_helper<T, true> {
    static const size_t value = 0;
};

template<typename T>
struct inherited_size
    : public inherited_size_helper<T, std::is_empty<T>::value> {
};

//----------------------------------------------------------------------
// slack_creator: a mechanism to create more slack (more ready tasks) in
// a task graph.
//----------------------------------------------------------------------
#if 0
struct slack_weights {
    // release, sync, focussed, random
    static const size_t success[sm_N]; // typically <= 0
    static const size_t fail[sm_N];    // typically >= 0
    static const size_t min = 0;
    static const size_t max = 256;
};

template<typename Weights>
class slack_creator {
    typedef Weights weights;

    size_t counter;
    size_t togo;

public:
    slack_creator() : counter( 0 ), togo( 0 ) { }

    void rchild_steal_attempt( bool success, steal_method_t sm ) {
#if 0
	if( success ) {
	    counter += weights::success[int(sm)];
	    if( counter < weights::min )
		counter = weights::min;
	    // --togo;
	} else {
	    counter += weights::fail[int(sm)];
	    if( counter > weights::max )
		counter = weights::max;
	    togo = counter;
	}
#endif
    }

    // Invoke if togo has dropped to zero (or below)
    bool invoke( bool ready ) {
#if 0
	if( ready && togo <= 0 ) {
	    togo = counter;
	    return true;
	} else {
	    --togo;
	    return false;
	}
#else
	// return ready;
	if( ready && togo <= 0 ) {
	    togo += 30;
	    return true;
	} else {
	    --togo;
	    return false;
	}
#endif
    }
};
#endif

//----------------------------------------------------------------------
// pending_frame: a function pointer + arguments + return value location
// and what we need to store the pending_frame in a list
//----------------------------------------------------------------------
class pending_frame : private obj::pending_frame_base_obj
{
    template<typename T>
    friend struct stack_frame_traits;

public:
    typedef obj::pending_frame_base_obj metadata_ty;

private:
    future * cresult;
    void (*func)(void);
    bool (*stub)( stack_frame *, void (*)(void) );

    friend class stack_frame_base;
    friend class stack_frame;

    pad_multiple<CACHE_ALIGNMENT, 3*sizeof(void*)
		 + inherited_size<obj::pending_frame_base_obj>::value > padding;

public:
    typedef pending_frame stack_frame_ty;

public:
#if STORED_ANNOTATIONS
    pending_frame( task_data_t & task_data_,
		   future * cr_, void (*func_)(void),
		   bool (*stub_)( stack_frame *, void (*)(void) ) )
	: cresult( cr_ ), func( func_ ), stub( stub_ )
	{
	    get_task_data().initialize( task_data_ );
	    static_assert( (sizeof(*this) % CACHE_ALIGNMENT) == 0,
			   "Padding of pending_frame failed" );
	}
#else
    pending_frame( size_t as_, size_t ts_, size_t fts_, size_t nargs_,
		   future * cr_, void (*func_)(void),
		   bool (*stub_)( stack_frame *, void (*)(void) ) )
	: cresult( cr_ ), func( func_ ), stub( stub_ )
	{
	    get_task_data().initialize( as_, ts_, fts_, nargs_ );
	    static_assert( (sizeof(*this) % CACHE_ALIGNMENT) == 0,
			   "Padding of pending_frame failed" );
	}
#endif

    inline void * operator new ( size_t size );
    inline void operator delete( void * p );
    
    // Some pointers (where we push variable-sized data)
    inline char * get_args_ptr()  const { return get_task_data().get_args_ptr();  }
    inline char * get_tags_ptr()  const { return get_task_data().get_tags_ptr();  }
    inline size_t get_args_size() const { return get_task_data().get_args_size(); }

    void (*get_func() const)(void) { return func; }

    void push_args() { }
    template<typename... Tn>
    void push_args( Tn... an ) {
	char * tgt = get_args_ptr(); // limit updates to local copy
	copy_args( tgt, an... );
    }
};

class full_frame : private obj::full_frame_base_obj
{
    template<typename T>
    friend struct stack_frame_traits;

public:
    typedef obj::full_frame_base_obj metadata_ty;

private:
#if FF_MCS_MUTEX
    typedef mcs_mutex base_mutex;
#else
    typedef pref_mutex base_mutex;
#endif

#if FF_MCS_MUTEX
    // This class serves to simplify the use of an mcs_mutex by making it
    // easy to keep track of the node between lock() and unlock().
    class sf_mutex {
	typedef holder_mutex<base_mutex, const spawn_deque, (DBG_SF_LOCKER != 0), 1> mutex_;
	typedef base_mutex::node node_;
	// node_ N_one;
	mutex_ M;
	node_ * N;

    public:
	sf_mutex() : N( 0 ) { }

	void lock( const spawn_deque * D ) {
	    node_ * N_ = magically_get_node( D );
	    M.lock( D, N_ );
	    assert( N == 0 && "N must be null on lock" );
	    N = N_;
	}
	bool try_lock( const spawn_deque * D ) {
	    node_ * N_ = magically_get_node( D );
	    if( M.try_lock( D, N_ ) ) {
		N = N_;
		return true;
	    } else
		return false;
	}
	void unlock( const spawn_deque * D ) {
	    assert( N != 0 && "N must not be null on unlock" );
	    node_ * N_ = N;
	    N = 0;
	    M.unlock( D, N_ );
	}

	void unuse() { N->unuse(); N = 0; }

	const spawn_deque * get_holder() const { return M.get_holder(); }

	bool test_lock( const spawn_deque * D ) const {
	    return M.test_lock( D );
	}
	bool test_lock() const { return M.test_lock(); }

    private:
	inline node_ * magically_get_node( const spawn_deque * D );
    };
public:
    typedef base_mutex::node mutex_tag;
    typedef stack_frame stack_frame_ty;
private:
#else

    typedef holder_mutex<base_mutex, const spawn_deque, (DBG_SF_LOCKER != 0), 1> sf_mutex;

#endif // FF_MCS_MUTEX

private:
    stack_frame * fr;
    full_frame * parent;

    volatile size_t num_children;

    // void * pad0[1];

    // New cache line
    sf_mutex vlock;

#if 0
    slack_creator<slack_weights> slack;
#endif

private:
    // full_frame() { }
    full_frame( stack_frame * fr_, full_frame * parent_ )
	: fr( fr_ ), parent( parent_ ), num_children( 0 ) {
    }
    friend class stack_frame;
    friend class stack_frame_base;

public:
    inline ~full_frame();

    inline stack_frame * get_frame() const; // Optimize this! { return fr; }
    full_frame * get_parent() const { return parent; }

    // Note: Only allowed for out-of-order frames
    inline void add_child() { __sync_fetch_and_add( &num_children, 1 ); }
    inline void remove_child() { __sync_fetch_and_add( &num_children, -1 ); }
    bool all_children_done() const volatile { return num_children == 0; }

#if 0
    void rchild_steal_attempt( bool success, steal_method_t sm ) {
	slack.rchild_steal_attempt( success, sm );
    }
    bool slack_invoke( bool ready ) { return slack.invoke( ready ); }
#endif

    // Mutex
    void lock( const spawn_deque * by ) { vlock.lock( by ); }
    bool try_lock( const spawn_deque * by ) { return vlock.try_lock( by ); }
    void unlock( const spawn_deque * by ) { vlock.unlock( by ); }
    bool test_lock( const spawn_deque * by ) const {
	return vlock.test_lock( by );
    }
    bool test_lock() const { return vlock.test_lock(); }
};

class stack_frame_base : private obj::stack_frame_base_obj {
protected:
    future * cresult;
    frame_state_t state;
    const bool call;

    full_frame * ff;


    // parent is in a stack_frame because it is easiest way to make the
    // relationship with the parent and it is used extensively
    // throughout the code. Same for owner. This one we should not need, but
    // we do use it frequently all the same.
    stack_frame * const parent;
    spawn_deque * owner;

    char * stack_ptr;

    intptr_t saved_ebp;
#ifdef __PIC__
    intptr_t saved_ebx; // for PIC
#endif

    dbg_continuation dbgc;

    pad_multiple<8, 5*sizeof(void*)+sizeof(frame_state_t)+4*sizeof(bool)
		 + sizeof(intptr_t)
#ifdef __PIC__
		 + sizeof(intptr_t)
#endif
		 + inherited_size<obj::stack_frame_base_obj>::value
		 + sizeof(dbg_continuation) > pad0;

public:
    // Dummy frame constructor
    stack_frame_base( char * end_of_stack, size_t nargs_, stack_frame * parent_,
		      spawn_deque * owner_, bool call_ )
	: call( call_ ), ff( 0 ), parent( parent_ ), owner( owner_ ) {
	static_assert( (sizeof(stack_frame_base) & 7) == 0,
		       "stack_frame_base alignment" );
	get_task_data().initialize( 0, 0, 0, end_of_stack, nargs_ );
    }
    // Executing frame constructor
#if STORED_ANNOTATIONS
    stack_frame_base( task_data_t & task_data_,
		      spawn_deque * owner_, bool call_ )
	: call( call_ ), ff( 0 ), owner( owner_ ) {
	get_task_data().initialize( task_data_ );
    }
#else
    stack_frame_base( size_t as_, size_t ts_, size_t fts_, char * end_of_stack,
		      size_t nargs_, stack_frame * parent_,
		      spawn_deque * owner_, bool call_ )
	: call( call_ ), ff( 0 ), parent( parent_ ), owner( owner_ ) {
	get_task_data().initialize( as_, ts_, fts_, end_of_stack, nargs_ );
    }
#endif
    // Conversion from pending_frame
    stack_frame_base( task_data_t & data_, char * end_of_stack,
		      stack_frame * parent_, spawn_deque * owner_, bool call_ )
	: call( call_ ), ff( 0 ), parent( parent_ ), owner( owner_ ) {
	get_task_data().initialize( data_ );
    }
    inline ~stack_frame_base();

    obj::stack_frame_base_obj * get_metadata() {
	return static_cast<obj::stack_frame_base_obj *>( this );
    }

    inline void flag_result();

    full_frame * get_full() const { return ff; }
    bool is_full() const { return ff != 0; }

    bool is_call() const { return call; }

    stack_frame * get_parent() const { return parent; }
    spawn_deque * get_owner() const { return owner; }
    void set_owner( spawn_deque * sd ) { owner = sd; }

    frame_state_t get_state() const { return state; }
    void set_state( frame_state_t state_ ) { state = state_; }

    inline char * get_args_ptr()  const { return get_task_data().get_args_ptr();  }
    inline char * get_tags_ptr()  const { return get_task_data().get_tags_ptr();  }
    inline size_t get_args_size() const { return get_task_data().get_args_size(); }
    using stack_frame_base_obj::get_task_data;

#ifdef __PIC__
    void set_saved_pr( intptr_t saved_pr_ ) { saved_ebx = saved_pr_; }
#endif

    void save_continuation() { dbgc.save_continuation( saved_ebp ); }
    void check_continuation() const { dbgc.check_continuation( saved_ebp ); }
    void clear_continuation() { dbgc.clear_continuation(); }

protected:
    inline full_frame * convert_to_full( void * p );
};

// Implement a character array such that its length automatically fills up
// to a particular size of the overall class stack_frame.
template<size_t DataSize_>
struct stack_frame_data {
    static const size_t DataSize = DataSize_;
    char mem[DataSize_];

    char & operator[] ( size_t idx ) { return mem[idx]; }
    const char & operator[] ( size_t idx ) const { return mem[idx]; }
};

class stack_frame : public stack_frame_base// , private obj::stack_frame_base_obj
{
    template<typename T>
    friend struct stack_frame_traits;

public:
    typedef obj::stack_frame_base_obj metadata_ty;

private:
    static const size_t FrameSize = STACK_FRAME_SIZE;
    static const size_t ParentSize =
	inherited_size<stack_frame_base>::value
	; // + inherited_size<obj::stack_frame_base_obj>::value;
    static const size_t FillSize =
	FrameSize - ParentSize - sizeof(full_frame);
    static const size_t DataSize = FillSize & ~size_t(15);
    static const size_t PadSize = FillSize & size_t(15);
public:
    static const size_t Align = FrameSize;
private:
    typedef stack_frame_data<DataSize> data_t;
    char full_frame_storage[sizeof(full_frame)]; // close to stack_frame_base (locality)
    char padding[PadSize];
    data_t mem;

private:
    void check_alignment() const {
	// errs() << "FrameSize=" << FrameSize
	       // << " ParentSize=" << ParentSize
	       // << " FillSize=" << FillSize
	       // << " DataSize=" << DataSize
	       // << " PadSize=" << PadSize
	       // << " Align=" << Align
	       // << '\n';
	assert( ((intptr_t)this & (intptr_t)(Align-1)) == 0
		&& "Stack_frame not aligned on page boundary" );
	assert( ((intptr_t)&mem[DataSize] & intptr_t(15)) == 0
		&& "Top of data area must be 16-byte aligned" );
	static_assert( (DataSize % 16) == 0, "DataSize must be multiple of 16" );
	static_assert( ParentSize + sizeof(full_frame) + PadSize + DataSize
		       == FrameSize, "Components do not add up to FrameSize" );
    }

public:
    stack_frame()
	: stack_frame_base( &mem[DataSize], 0, 0, 0, false )
    // : parent( 0 ),
    // owner( 0 ), cresult( 0 ),
    // state( fs_suspended ), full( true ) {
	{
	    cresult = 0;

	    stack_ptr = &mem[DataSize];
	    state = fs_dummy;
	    ff = new (full_frame_storage) full_frame( this, 0 );
	    check_alignment();
	}

    // Normal constructor. Postpone initialization to initialize() as much
    // as possible in order to push spawnee arguments on stack before doing
    // anything else.
#if STORED_ANNOTATIONS
    stack_frame( future * cresult_, task_data_t & task_data_,
		 bool full_, bool call_ )
	: stack_frame_base( task_data_,
			    task_data_.get_parent()->get_owner(), call_ )
#else
	  stack_frame( size_t as_, size_t ts_, size_t fts_, size_t nargs_,
		       stack_frame * parent_, future * cresult_,
		       bool full_, bool call_ )
        : stack_frame_base( as_, ts_, fts_, &mem[DataSize],
			    nargs_, parent_, parent_->get_owner(), call_ )
#endif
    // : parent( parent_ ),
    // owner( parent ? parent->owner : 0 ),
    // cresult( cresult_ ),
    // // children ( 0 ),
    // state( fs_waiting ), full( full_ ), call( call_ ) {
	{
	    cresult = cresult_;
	    state = fs_waiting;

#if STORED_ANNOTATIONS
	    stack_ptr = &mem[DataSize];
#else
	    stack_ptr = get_task_data().get_args_ptr();
	    // If we have zero arguments, then stack_ptr may coincide
	    // with the end of range
	    assert( &mem[0] <= stack_ptr );
            assert( stack_ptr < &mem[DataSize] || as_ == 0 );
            assert( stack_ptr <= &mem[DataSize] || as_ != 0 );
#endif

	    check_alignment();

	    ff = full_ ? new (full_frame_storage)
		full_frame( this, get_parent()->get_full() ) : 0;

	    assert( get_parent() && "Use special constructor for root frame" );
	    if( is_full() ) {
		assert( get_parent()->is_full() && "Parent of full frame must be full" );
		get_parent()->get_full()->add_child();
	    }
	    assert( ( !is_full() || get_parent()->is_full() )
		    && "parent of full frame must be full" );
	}

    // Constructor to convert pending_frame to stack_frame.
    stack_frame( stack_frame * parent_, spawn_deque * owner_,
		 pending_frame * pnd, bool full_, bool call_ ) :
	stack_frame_base( pnd->get_task_data(), &mem[DataSize],
			  parent_, owner_, call_ )
	{
	    task_graph_traits<stack_frame, full_frame, pending_frame, pending_frame>::create_from_pending( this, pnd );

	    cresult = pnd->cresult;
	    state = fs_waiting;
	    stack_ptr = reinterpret_cast<char *>(intptr_t(&mem[DataSize])&~15);
	    ff = full_ ? new (full_frame_storage)
		full_frame( this, get_parent()->get_full() ) : 0;

	    check_alignment();

	    assert( get_parent() && "Use special constructor for root frame" );
	    assert( ( !is_full() || get_parent()->is_full() )
		    && "parent of full frame must be full" );
	}

    inline ~stack_frame();

    char * get_stack_ptr( frame_create_t fc = fc_waiting ) const {
	return stack_ptr;
#if 0
	char * args = get_task_data().get_args_ptr();

	// Special case: if fc_executing, then arguments are on
	// stack! (If fc_waiting, then we don't know. If fc_pending
	// then surely not on stack.)
	if( fc == fc_executing )
	    return args;

	// Detailed condition:
	//     &mem[0] <= args && args < (char *)&mem[DataSize]
	// Fastest so far
	// return (&mem[0] <= args) & (args < (char *)&mem[DataSize])
	    // ? args : (char *)&mem[DataSize];

	// Even faster: there are corner cases where this is incorrect
	// in particular when &mem[0] is very high (near the end of the
	// address range) and args is very low. This will never happen
	// in practice.
	uintptr_t diff = uintptr_t(args) - uintptr_t(&mem[0]);
	return diff < uintptr_t(DataSize) ? args : (char *)&mem[DataSize];

	// Alternatives
	// return (intptr_t(this)&intptr_t(args)) == intptr_t(this)
	    // ? args : (char *)&mem[DataSize];
	// return &mem[0] <= args && args < (char *)&mem[DataSize]
	// ? args : (char *)&mem[DataSize];
	// return (intptr_t(this)>>13) == (intptr_t(args)>>13)
	    // ? args : (char *)&mem[DataSize];
/* very slow
	intptr_t hi = intptr_t(args) & ~((intptr_t(1)<<13)-intptr_t(1));
	intptr_t sel = ( (intptr_t(this) ^ intptr_t(args))) == 0;
	intptr_t offa = intptr_t(args);
	intptr_t offf = intptr_t(&mem[DataSize]);

	return (char *)((sel & offf) | (~sel & offa));
*/
/* incorrect
	intptr_t hi = intptr_t(args) & ~((intptr_t(1)<<13)-intptr_t(1));
	intptr_t diff = intptr_t(&mem[DataSize]) - intptr_t(args);
	intptr_t sel = intptr_t(this) == intptr_t(hi);
	return (char *)(intptr_t(args)+(sel & diff));
*/
#endif
    }

    /* @brief my_stack_frame
     * Return the stack_frame we are executing on based on the current esp,
     * assuming that is in range. All stack_frame's are allocated on a
     * page boundary by valloc(), so the constant 4095 here relates to the
     * page boundary and not to N.
     * @return A pointer to a stack_frame, presumably the one we are executing
     * on.
     */
    static inline stack_frame * my_stack_frame();
    static inline stack_frame * stack_frame_of(intptr_t ptr);

    inline void * operator new ( size_t size );
    inline void operator delete( void * p );
    
    inline full_frame * convert_to_full();

    // Top of duplicated stack on the child stack_frame. Size determined
    // by number of arguments to split_stub().
    inline char * top_of_stack() const { return (char *)&mem[DataSize]; }

    void push_args() { }
    template<typename... Tn>
    void push_args( Tn... an ) {
	char * tgt = get_args_ptr(); // limit updates to local copy
	copy_args( tgt, an... );
    }

    template<typename TR, typename... Tn>
    static inline void
    create_waiting( full_frame * cur_frame, future * cresult,
#if STORED_ANNOTATIONS
		    task_data_t & task_data_p,
#endif
		    TR (*func)( Tn... ), Tn... args )
	__attribute__((always_inline, returns_twice));

    inline void sync() __attribute__((always_inline));

    template<typename TR, typename... Tn>
    static inline pending_frame *
    create_pending( TR (*func)( Tn... ), stack_frame * fr, future * fut,
#if STORED_ANNOTATIONS
		    task_data_t & task_data_p,
#endif
		    Tn... args )
	__attribute__((always_inline, returns_twice));

    template<typename TR, typename... Tn>
    static inline void
    invoke( future * c,
#if STORED_ANNOTATIONS
	    task_data_t & task_data_p,
#endif
	    bool is_call, TR (*func)( Tn... ), Tn... args )
	__attribute__((always_inline)); // returns_twice

    static stack_frame *
    create_frame( stack_frame * parent, spawn_deque * owner,
		  pending_frame * pnd );


    static inline void split_ctrl_executing( stack_frame * child );
    static void split_ctrl_waiting( stack_frame * child );
    static void split_ctrl( stack_frame * child, frame_create_t fcreate )
	__attribute__((noinline, regparm(2)));

    template<bool (*ss_ptr)(stack_frame *, void (*)(void))>
    static inline bool
    prevent_inlining_dir( stack_frame * child, void (*fn_ptr)(void) )
	__attribute__((returns_twice));

#if IMPROVED_STUBS
    static inline bool
    prevent_inlining( stack_frame * child, void (*fn_ptr)(void),
		      bool (*ss_ptr)(stack_frame *, void (*)(void)) )
	__attribute__((returns_twice));
#else
    static bool
    prevent_inlining( stack_frame * child, void (*fn_ptr)(void),
		      bool (*ss_ptr)(stack_frame *, void (*)(void)) )
	__attribute__((returns_twice, noinline));
#endif
    template<frame_create_t fcreate, typename TR, typename... Tn>
    static bool split_stub( stack_frame * child,
			    void (*fn_ptr)(void) )
	__attribute__((returns_twice, noinline, regparm(3)));
    template<frame_create_t fcreate, typename TR, typename... Tn>
    static bool split_stub_body( stack_frame * child,
				 void (*fn_ptr)(void) )
	__attribute__((returns_twice, noinline, regparm(3)));
    static void sync_stub( stack_frame * fr )
	__attribute__((noinline, regparm(1)));
    static bool split_return( stack_frame * child )
	__attribute__((noinline, regparm(2)));
    void resume() __attribute__((noinline));


#if DBG_VERIFY
    static void verify( intptr_t ebp );
    void verify_rec() const;
    void verify() const;
#else
    static inline void verify( intptr_t ebp ) { }
    inline void verify_rec() const { }
    inline void verify() const { }
#endif
};

stack_frame *
full_frame::get_frame() const {
    return stack_frame::stack_frame_of( intptr_t(this) );
}

stack_frame_base::~stack_frame_base() {
    assert( ( !is_full() || !get_parent() || get_parent()->is_full() )
	    && "Parent of full frame must be full" );
    assert( ( !is_full() || get_full()->all_children_done() )
	    && "All pending frames must have executed "
	    "before cleaning up parent. Did you ssync() before return?" );
}

full_frame *
stack_frame_base::convert_to_full( void * p ) {
    assert( !is_full() && "Frame already full when converting to full" );
    assert( get_state() != fs_suspended && "Frame suspended when cvt to full" );
    assert( get_parent() && "Frame must have parent when cvt to full" );
    assert( get_parent()->is_full() && "Parent must be full when cvt to full" );
    // Maintain invariant: if full frame, then must be out-of-order too
    // Allocate full frame additional fields
    ff = new (p) full_frame( (stack_frame *)this, get_parent()->get_full() );
    // Our parent now has a full child
    ff->get_parent()->add_child();
    return ff;
}

/* @brief
 * Return the stack_frame we are executing on based on the current esp,
 * assuming that is in range. All stack_frame's are allocated on a
 * page boundary by valloc(), so the constant 4095 here relates to the
 * page boundary and not to N.
 * @return A pointer to a stack_frame, presumably the one we are executing
 * on.
 */
stack_frame *
stack_frame::my_stack_frame() {
    return stack_frame_of( get_sp() );
}

stack_frame *
stack_frame::stack_frame_of( intptr_t ptr ) {
    return reinterpret_cast<stack_frame *>( ptr & ~(intptr_t)(Align-1) );
}


stack_frame::~stack_frame() {
    // We must have a lock on the parent when deleting the child.
    // This can be either an explicit lock or the parent can be sinked
    // in the current call stack of the extended spawn deque, which is
    // as good as a lock.
    // Why the lock? Full state can change when sinked in currect stack???
    // And then there is the problem of the conceptual change to full frame,
    // for the oldest of the current call stack which happens only on pop,
    // but should be flagged sooner!
    // Caution: locks should be taken starting from the parent going
    // down in the stack tree.
    assert( get_state() == fs_executing
	    && "No reason to leave fs_executing before destruction" );
}

typedef task_graph_traits<stack_frame, full_frame, pending_frame, pending_frame> the_task_graph_traits;

full_frame::~full_frame() {
    assert( test_lock() );
    vlock.unuse();
    if( likely( parent != 0 ) )
	parent->remove_child(); // parent may launch now
}

void stack_frame_base::flag_result() {
    if( cresult ) {
	assert( !( get_parent() && get_parent()->get_parent() )
		|| stack_frame::stack_frame_of(intptr_t(cresult)) == get_parent() );
	cresult->flag_result();
    }
}

full_frame * stack_frame::convert_to_full() {
    full_frame * full = stack_frame_base::convert_to_full( full_frame_storage );
    the_task_graph_traits::convert_to_full( this, full );
    return full;
}

template<>
struct stack_frame_traits<full_frame> {
    typedef full_frame frame_ty;
    typedef full_frame::metadata_ty metadata_ty;

    static frame_ty *
    get_frame_of( metadata_ty * pf ) { return static_cast<frame_ty *>( pf ); }

    static metadata_ty *
    get_metadata( frame_ty * pf ) { return static_cast<metadata_ty *>( pf ); }
};

template<>
struct pending_frame_traits<full_frame> {
    typedef stack_frame its_pending_frame_ty;

    static its_pending_frame_ty * get_frame( full_frame * elm ) {
	return elm->get_frame();
    }
};

template<>
struct stack_frame_traits<pending_frame> {
    typedef pending_frame frame_ty;
    typedef pending_frame::metadata_ty metadata_ty;

    static frame_ty *
    get_frame_of( metadata_ty * pf ) { return static_cast<frame_ty *>( pf ); }

    static metadata_ty *
    get_metadata( frame_ty * pf ) { return static_cast<metadata_ty *>( pf ); }
};

template<>
struct pending_frame_traits<pending_frame> {
    typedef pending_frame its_pending_frame_ty;

    static its_pending_frame_ty * get_frame( pending_frame * elm ) {
	return elm;
    }
};

template<>
struct stack_frame_traits<stack_frame> {
    typedef stack_frame frame_ty;
    typedef stack_frame::metadata_ty metadata_ty;

    // static frame_ty *
    // get_frame_of( metadata_ty * pf ) { return static_cast<frame_ty *>( pf ); }

    static metadata_ty *
    get_metadata( frame_ty * pf ) { return pf->get_metadata(); }
};

#endif // STACK_FRAME_H

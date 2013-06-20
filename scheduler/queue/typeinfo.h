// -*- c++ -*-

#ifndef QUEUE_TYPEINFO_H
#define QUEUE_TYPEINFO_H

// ------------------------------------------------------------------------
// Classes to extract type-specific functionality: destructor and copy
// operator, to be used specifically for hyperqueues.
// ------------------------------------------------------------------------
template<typename T, bool is_class=std::is_class<T>::value>
struct q_destructor_get {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)( void*, void* );

    static destructor_fn_ty get_destructor() {
	return &call_destructor;
    }
    static copy_fn_ty get_copy_operator() {
	return &call_copy_operator;
    }
private:
    static void call_destructor( void * ptr ) {
	reinterpret_cast<T *>( ptr )->T::~T();
    }
    static void call_copy_operator( void * to, void * from ) {
	reinterpret_cast<T *>( to )->T::operator = (
	    *reinterpret_cast<T *>( from ) );
    }
};

// Assumed scalar.
template<typename T>
struct q_destructor_get<T, false> {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)( void*, void* );

    static destructor_fn_ty get_destructor() { return 0; }
    static copy_fn_ty get_copy_operator() {
	return &call_copy_operator;
    }
	
private:
    // Copy a the value byte by byte.
    static void call_copy_operator( void * to, void * from ) {
	// Check that T is a scalar type, not an array type, which may have
	// undefined size (int[]) or by an array of classes.
	static_assert( !std::is_array<T>::value, "array type not supported" );

	std::copy( reinterpret_cast<T *>( from ),
		   reinterpret_cast<T *>( from )+sizeof(T),
		   reinterpret_cast<T *>( to ) );
    }
};

// ------------------------------------------------------------------------
// Typeinfo for hyperqueues.
// ------------------------------------------------------------------------
class q_typeinfo {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)( void*, void* );

private:
    destructor_fn_ty dfn;
    copy_fn_ty cp_opr;
    uint32_t size;                // size of the data space in bytes
    uint32_t roundup_size;
	
public:
    // Credit:
    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    static size_t roundup_pow2( size_t v ) {
	v--;
	v |= v>>1;
	v |= v>>2;
	v |= v>>4;
	v |= v>>8;
	v |= v>>16;
	v |= v>>32;
	v++;
	return v;
    }

private:
    q_typeinfo( destructor_fn_ty dfn_ , copy_fn_ty cp_opr_, uint32_t size_ )
	: dfn( dfn_ ), cp_opr( cp_opr_ ),
	  size( size_ ), roundup_size( roundup_pow2(((size+7)&~(size_t)7)) ) {
    }
	
public:
    // creates a new typeinfo
    template<typename T>
    static q_typeinfo create() {
	return q_typeinfo( q_destructor_get<T>::get_destructor(),
			   q_destructor_get<T>::get_copy_operator(), 
			   sizeof(T) );
    }

    // calls a T constructor
    template<typename T>
    static void construct( void * ptr ) {
	new (ptr) T();
    }

    // calls a T destructor, where T is the type argument of create().
    void destruct( void * ptr ) const {
	if( dfn )
	    (*dfn)( ptr );
    }
	
    // calls a T copy operator, where T is the type argument of create().
    void copy( void * to, void * from ) const { (*cp_opr)( to, from ); }
    
    size_t get_size() const volatile { return roundup_size; }
    size_t get_actual_size() const volatile { return size; }
};

#endif // QUEUE_TYPEINFO_H

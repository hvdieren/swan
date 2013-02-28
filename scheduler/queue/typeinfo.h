
#ifndef QUEUE_TYPEINFO_H
#define QUEUE_TYPEINFO_H
// ------------------------------------------------------------------------
// Classes to support versioning of objects
// ------------------------------------------------------------------------
template<typename T, bool is_class=std::is_class<T>::value>
struct q_destructor_get {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)(void*, void*, size_t );

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
    static void call_copy_operator( void * to, void * from, size_t act_size ) {
	reinterpret_cast<T *>( to )->T::operator = ( *reinterpret_cast<T *>( from ) );
	}
};

template<typename T>
struct q_destructor_get<T, false> {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)(void*, void*, size_t );

    static destructor_fn_ty get_destructor() { return 0; }
	
    static copy_fn_ty get_copy_operator() {
	return &call_copy_operator;
    }
	
private:
    static void call_copy_operator( void * to, void * from, size_t act_size ) {
	//*reinterpret_cast<T *>(to) = *reinterpret_cast<T *>(from);
	std::copy((T*)from, (T*)from+act_size, (T*)to);
	//memcpy ( to, from, actual_size );
    }
};

class q_typeinfo {
    typedef void (*destructor_fn_ty)( void * );
    typedef void (*copy_fn_ty)(void*, void*, size_t act_size );
    destructor_fn_ty dfn;
    copy_fn_ty cp_opr;
    //size_t size;
	
    q_typeinfo( destructor_fn_ty dfn_ , copy_fn_ty cp_opr_, uint32_t size_, uint32_t ru_size) : dfn( dfn_ ),cp_opr(cp_opr_),
		size(size_), roundup_size(ru_size) { }
    uint32_t size;                // size of the data space in bytes
    uint32_t roundup_size;
    
    static size_t round_pow_2(size_t v) {
	v--;
	v |= v>>1;
	v |= v>>2;
	v |= v>>4;
	v |= v>>8;
	v |= v>>16;
	v++;
	return v;
    }
	
public:
    //creates a new typeinfo
    template<typename T>
    static q_typeinfo create() {
	q_typeinfo ti( q_destructor_get<T>::get_destructor(),  q_destructor_get<T>::get_copy_operator(), 
		       sizeof(T), round_pow_2(((sizeof(T)+7)&~(size_t)7)));
	return ti;
    }

    //calls a T constructor
    template<typename T>
    static void construct( void * ptr ) {
	new (ptr) T();
    }

    void destruct( void * ptr ) {
	if( dfn )
	    (*dfn)( ptr );
    }
	
    //casts from and to TO T and then copies from TO to
    void copy( void * to, void * from) {
	(*cp_opr)(to, from, size);
    }
    
    size_t get_size() {
	return roundup_size;
    }
	
    size_t get_actual_size() {
	    return size;
    }
};

#endif // QUEUE_TYPEINFO_H

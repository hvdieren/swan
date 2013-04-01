// -*- c++ -*-

#ifndef QUEUE_QUEUE_T_H
#define QUEUE_QUEUE_T_H

#include "swan/queue/queue_version.h"

namespace obj {

// queue_base: an instance of a queue, base class for hyperqueue, pushdep, popdep,
// pushpopdep, prefixdep, suffixdep.
// This class may not have non-trival constructors nor destructors in order to
// reap the simplified x86-64 calling conventions for small structures (the
// only case we support), in particular for passing pushdep and popdep
// as direct arguments. We don't support this for hyperqueue.
template<typename MetaData>
class queue_base
{
public:
    typedef MetaData metadata_t;

    template<typename T>
    friend class hyperqueue;

    template<typename T>
    friend class popdep;

protected:
    queue_version<metadata_t> * queue_v;

public:
    const queue_version<metadata_t> * get_version() const { return queue_v; }
    queue_version<metadata_t> * get_version() { return queue_v; }
	
    void set_version( queue_version<metadata_t> * v ) { queue_v = v; }

protected:
    queue_version<metadata_t> * get_nc_version() const { return queue_v; }

    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<!is_prefixdep<T>::value && !is_suffixdep<T>::value,
			    DepTy<T>>::type
    create_dep_ty() const {
	DepTy<T> od;
	od.queue_v = this->get_nc_version();
	return od;
    }

    template<typename T, template<typename U> class DepTy>
    typename std::enable_if<is_prefixdep<T>::value || is_suffixdep<T>::value,
			    DepTy<T>>::type
    create_dep_ty( size_t n, const T & dflt ) const {
	DepTy<T> od;
	od.queue_v = get_nc_version();
	od.count = n;
	od.dflt = dflt;
	return od;
    }
};

// hyperqueue: programmer's instance of a queue

template<typename T>
class hyperqueue : protected queue_version<queue_metadata>
{
    // There is one index structure per hyperqueue
    queue_index qindex;

public:
    explicit hyperqueue( size_t size = 128 )
	: queue_version<queue_metadata>( qindex, size ) { }
	
    operator pushdep<T>() const { return create_dep_ty< pushdep<T> >(); }
    operator popdep<T>()  const { return create_dep_ty< popdep<T> >(); }
    operator pushpopdep<T>()  const { return create_dep_ty< pushpopdep<T> >(); }

    prefixdep<T> prefix( size_t n ) const {
	return create_dep_ty< prefixdep<T> >( n, 0 );
    }
    prefixdep<T> prefix( size_t n, const T & dflt ) const {
	return create_dep_ty< prefixdep<T> >( n, dflt );
    }
    suffixdep<T> suffix( size_t n ) const {
	return create_dep_ty< suffixdep<T> >( n, 0 );
    }
    suffixdep<T> suffix( size_t n, const T & dflt ) const {
	return create_dep_ty< suffixdep<T> >( n, dflt );
    }
	
    // The hyperqueue works in push/pop mode and so supports empty, pop and push.
    bool empty() { return queue_version<queue_metadata>::empty(); }

    T pop() {
	T t;
	queue_version<queue_metadata>::pop( t );
	return t;
    }

    void push( const T & t ) {
	queue_version<queue_metadata>::push( t );
    }
	
private:
    template<typename DepTy>
    typename std::enable_if<!is_prefixdep<DepTy>::value
	&& !is_suffixdep<DepTy>::value, DepTy>::type
    create_dep_ty() const {
	DepTy od;
	od.queue_v = get_nc_version();
	return od;
    }

protected:
    queue_version<metadata_t> * get_nc_version() const {
	return const_cast<queue_version<queue_metadata>*>(
	    static_cast<const queue_version<queue_metadata>*>( this ) );
    }
	
public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};


template<typename T>
class pushdep : public queue_base<queue_metadata>
{
public:
    typedef queue_metadata metadata_t;
    typedef pushdep_tags dep_tags;
    typedef pushdep_type_tag _object_tag;
	
	// For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);

    static pushdep<T> create( queue_version<metadata_t> * v ) {
	pushdep<T> dep;
	dep.queue_v = v;
	return dep;
    }
    
    void push( const T & value ) { queue_v->push( value ); }
};

template<typename T>
class popdep : public queue_base<queue_metadata>
{
public:
    typedef queue_metadata metadata_t;
    typedef popdep_tags dep_tags;
    typedef popdep_type_tag _object_tag;
	
	
    static popdep<T> create( queue_version<metadata_t> * v ) {
	popdep<T> newpop;
	newpop.queue_v = v;
	return newpop;
    }

    prefixdep<T> prefix( size_t n ) const {
	return create_dep_ty< prefixdep >( n, 0 );
    }
    prefixdep<T> prefix( size_t n, const T & dflt ) const {
	return create_dep_ty< prefixdep >( n, dflt );
    }
    suffixdep<T> suffix( size_t n ) const {
	return create_dep_ty< suffixdep >( n, 0 );
    }
    suffixdep<T> suffix( size_t n, const T & dflt ) const {
	return create_dep_ty< suffixdep >( n, dflt );
    }
	
    T pop() {
	T t;
	queue_v->pop( t );
	return t;
    }
	
    bool empty() { return queue_v->empty(); }

protected:

    template<template<typename U> class DepTy>
    typename std::enable_if<is_prefixdep<DepTy<T>>::value
	|| is_suffixdep<DepTy<T>>::value, DepTy<T>>::type
    create_dep_ty( size_t n, const T & dflt ) const {
	DepTy<T> od;
	od.queue_v = get_nc_version();
	od.count = n;
	od.dflt = dflt;
	return od;
    }


public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<typename T>
class pushpopdep;

template<typename T>
class prefixdep : public queue_base<queue_metadata> {
public:
    typedef queue_metadata metadata_t;
    typedef prefixdep_tags dep_tags;
    typedef prefixdep_type_tag _object_tag;
    size_t count;
    T dflt;
	
    static prefixdep<T> create( queue_version<metadata_t> * v, size_t n,
				const T & dflt ) {
	prefixdep<T> d;
	d.queue_v = v;
	d.count = n;
	d.dflt = dflt;
	return d;
    }
	
    T pop() {
	T t;
	queue_v->pop_fixed( t, dflt );
	return t;
    }

    size_t get_index() const { return queue_v->get_index(); }
	
    // We must consume the whole prefix
    bool empty() const { return queue_v->get_count() == 0; }

    size_t get_length() const { return queue_v->get_count(); }
    size_t get_length_setting() const { return count; }
    const T & get_default() const { return dflt; }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<typename T>
class suffixdep : public queue_base<queue_metadata> {
public:
    typedef queue_metadata metadata_t;
    typedef suffixdep_tags dep_tags;
    typedef suffixdep_type_tag _object_tag;
    size_t count;
    T dflt;
	
    static suffixdep<T> create( queue_version<metadata_t> * v, size_t n,
				const T & dflt ) {
	suffixdep<T> d;
	d.queue_v = v;
	d.count = n;
	d.dflt = dflt;
	return d;
    }

    void push( const T & value ) { queue_v->push( value ); }
	
    size_t get_length() const { return queue_v->get_count(); }
    size_t get_length_setting() const { return count; }
    const T & get_default() const { return dflt; }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};


} //end namespace obj

#ifdef __x86_64__
#include "swan/platform_x86_64.h"

namespace platform_x86_64 {

// Specialization - declare to the implementation of the calling convention
// that obj::indep a.o. are a struct with two members. The implementation
// of the calling convention will then figure out whether this struct is
// passed in registers or not. (If it is not - you should not pass it and
// a compile-time error will be triggered). This approach is a low-cost
// way to by-pass the lack of data member introspection in C++.
template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::hyperqueue<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::hyperqueue<T>, obj::queue_version<typename obj::hyperqueue<T>::metadata_t> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::popdep<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::popdep<T>, obj::queue_version<typename obj::popdep<T>::metadata_t> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::pushdep<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::pushdep<T>, obj::queue_version<typename obj::pushdep<T>::metadata_t> *> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::prefixdep<T> >
    : arg_passing_struct3<ireg, freg, loff, obj::prefixdep<T>, obj::queue_version<typename obj::pushdep<T>::metadata_t> *, size_t, T> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::suffixdep<T> >
    : arg_passing_struct3<ireg, freg, loff, obj::suffixdep<T>, obj::queue_version<typename obj::pushdep<T>::metadata_t> *, size_t, T> {
};

} // namespace platform_x86_64
#endif

#endif // QUEUE_QUEUE_T_H

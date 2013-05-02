// -*- c++ -*-

#ifndef QUEUE_QUEUE_T_H
#define QUEUE_QUEUE_T_H

#include "swan/queue/queue_version.h"

namespace obj {

template<typename MetaData, typename T>
class read_slice;

// queue_base: an instance of a queue, base class for hyperqueue, pushdep, popdep,
// pushpopdep.
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
    DepTy<T> create_dep_ty() const {
	DepTy<T> od;
	od.queue_v = this->get_nc_version();
	return od;
    }
};

// hyperqueue: programmer's instance of a queue
template<typename T>
class hyperqueue : protected queue_version<queue_metadata>
{
public:
    explicit hyperqueue( size_t size = 128, size_t peekoff = 0 )
	: queue_version<queue_metadata>( size, peekoff,
					 queue_version::initializer<T>() ) { }
	
    operator pushdep<T>() const { return create_dep_ty< pushdep >(); }
    operator popdep<T>()  const { return create_dep_ty< popdep >(); }
    operator pushpopdep<T>()  const { return create_dep_ty< pushpopdep >(); }

    // The hyperqueue works in push/pop mode and so supports empty, pop and push.
    bool empty() { return queue_version<queue_metadata>::empty(); }

    const T & pop() {
	return queue_version<queue_metadata>::pop<T>();
    }

    void push( const T & t ) {
	queue_version<queue_metadata>::push<T>( t );
    }
	
private:
    template<template<typename U> class DepTy>
    DepTy<T> create_dep_ty() const {
	DepTy<T> od;
	od.queue_v = this->get_nc_version();
	return od;
    }

protected:
    queue_version<queue_metadata> * get_nc_version() const {
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
	
    static pushdep<T> create( queue_version<metadata_t> * v ) {
	pushdep<T> dep;
	dep.queue_v = v;
	return dep;
    }

    void push( const T & value ) { queue_v->push<T>( value ); }

    write_slice<queue_metadata, T> get_write_slice( size_t length ) {
	return queue_v->get_write_slice<T>( length );
    }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
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

    read_slice<queue_metadata, T>
    get_read_slice_upto( size_t npop_max, size_t npeek ) {
	return queue_v->get_read_slice_upto<T>( npop_max, npeek );
    }
	
    const T & pop() {
	return queue_v->pop<T>();
    }
	
    const T & peek( size_t off ) {
	return queue_v->peek<T>( off );
    }
	
    bool empty() { return queue_base<queue_metadata>::queue_v->empty(); }

public:
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
};

template<typename T>
class pushpopdep;

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

} // namespace platform_x86_64
#endif

#endif // QUEUE_QUEUE_T_H

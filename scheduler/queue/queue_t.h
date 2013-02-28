// -*- c++ -*-

#ifndef QUEUE_QUEUE_T_H
#define QUEUE_QUEUE_T_H

#include "swan/queue/queue_version.h"

namespace obj {

struct popdep_tags_base : public all_tags_base { };
struct pushdep_tags_base : public all_tags_base { };
struct queue_t_tags_base : public all_tags_base { };

// Input dependency tags
class popdep_tags : public popdep_tags_base {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
    tkt_metadata::tag_t rd_tag;
};

// Output dependency tags require fully serialized tags in the worst case
class pushdep_tags : public pushdep_tags_base, public serial_dep_tags {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// Input dependency tags
class queue_t_tags : public popdep_tags_base {
    template<typename MetaData, typename Task, template<typename T> class DepTy>
    friend class dep_traits;
};

// parent: an instance of a queue, base class for queue_t, pushdep, popdep.
// This class may not have non-trival constructors nor destructors in order to
// reap the simplified x86-64 calling conventions for small structures (the
// only case we support), in particular for passing pushdep and popdep
// as direct arguments. We don't support this for queue_t.
class parent
{
public:
    queue_version *queue_v;
	
    typedef tkt_metadata queue_tkt_metadata;
    typedef queue_tkt_metadata metadata_t;
	
    queue_version * get_version()       { return this->queue_v; }
    queue_version * get_version() const { return this->queue_v; }
	
    void set_version(queue_version *version) {
	this->queue_v = version;
    }
	
    const parent & operator = ( parent input ) {
	this->queue_v = input.queue_v;
	return *this;
    }
};

template <class T>
class pushdep : public parent
{
public:
	typedef tkt_metadata queue_tkt_metadata;
	typedef queue_tkt_metadata metadata_t;
    typedef pushdep_tags dep_tags;
    typedef pushdep_type_tag _object_tag;
	
	// For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
	operator pushdep<T>() { 
		pushdep<T> newpush;
		this->queue_v->set_instance((parent*)&newpush);
		newpush.set_version(this->queue_v);
		return newpush;
	}
	
	static pushdep<T> create(queue_version* v, queue_segment* qs) {
		pushdep<T>  newpush;
		newpush.queue_v = v;
		newpush.queue_v->set_private_queue_segment(qs);
		return newpush;
	}
	
	void push(T value) {
		T* push_value = new T();
		*push_value = value;
		this->queue_v->get_queue()->push(push_value, (this->queue_v->get_private_queue_segment()));
	}
	
	void clearBusy() {
		queue_segment** pqs = (this->queue_v->get_private_queue_segment());
		(*pqs)->clearBusy();
	}
	
	static pushdep<T> create(queue_version* v) {
		pushdep<T>  newpush;
		newpush.queue_v = v;
		newpush.queue_v->set_private_queue_segment(NULL);
		return newpush;
	}
	
    pushdep<T> operator=(pushdep<T> input) {
		this->queue_v = input.queue_v;
		return *this;
	}
};

template <typename T>
class popdep : public parent
{
public:
	typedef tkt_metadata queue_tkt_metadata;
	typedef queue_tkt_metadata metadata_t;
    typedef popdep_tags dep_tags;
    typedef popdep_type_tag _object_tag;
	
	// For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
	
	static popdep<T> create(queue_version* v) {
		popdep<T>  newpop;
		newpop.queue_v = v;
		return newpop;
	}
	
	T pop()	{
		return *reinterpret_cast<T *>( this->queue_v->pop() );
	}
	
	bool empty() {
		return this->queue_v->empty();
	}
    popdep<T> operator=(popdep<T> input) {
		this->queue_v = input.queue_v;
		return *this;
	}
};

template <class T>
class queue_t : public parent
{
public:
    typedef tkt_metadata queue_tkt_metadata;
    typedef queue_tkt_metadata metadata_t;
    typedef queue_t_tags dep_tags;
    typedef queue_t_type_tag _object_tag;
	
    queue_t() {
	this->queue_v = new queue_version(q_typeinfo::create<T>(), this);
    }
	
    virtual inline ~queue_t() {
	//if(this->queue_v != NULL) { 
	//errs() <<"DESTRUCT QUEUE_T!\n";
	//this->queue_v->destruct();
	//this->queue_v = NULL;
	//}
    }
	
    // For concepts: need not be implemented, must be non-static and public
    void is_object_decl(void);
    operator pushdep<T>() {
	errs() <<"operator pushdep in queue_t\n";
	pushdep<T> newpush;
	this->queue_v->set_instance((parent*)&newpush);
	newpush.set_version(this->queue_v);
	return newpush;
    }
	
    operator popdep<T>() {
	return popdep<T>::create(this->queue_v);
    }
	
    queue_segment* privatize_segment() {
	return *(this->queue_v->privatize_segment());
    }
	
    segmented_queue *get_queue() {
	return this->queue_v->get_queue();
    }
	
    void push(T value) {
	this->queue_v->get_tail()->conc_push(value);
    }
	
    pushdep<T> operator=(pushdep<T> input) {
	pushdep<T> newpush =  pushdep<T>::create(input.queue_v, *(input.queue_v->get_private_queue_segment()));
	return newpush;
    }
	
    queue_t<T> operator=(popdep<T> input) {
	this->queue_v = input.queue_v;
	return *this;
    }
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
struct arg_passing<ireg, freg, loff, obj::queue_t<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::queue_t<T>, obj::queue_version *> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::popdep<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::popdep<T>, obj::queue_version *> {
};

template<size_t ireg, size_t freg, size_t loff, typename T>
struct arg_passing<ireg, freg, loff, obj::pushdep<T> >
    : arg_passing_struct1<ireg, freg, loff, obj::pushdep<T>, obj::queue_version *> {
};

} // namespace platform_x86_64
#endif

#endif // QUEUE_QUEUE_T_H

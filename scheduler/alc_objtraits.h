// -*- c++ -*-
#ifndef ALC_OBJTRAITS_H
#define ALC_OBJTRAITS_H

// Based on http://www.codeproject.com/KB/cpp/allocator.aspx

namespace alc {

template<typename T>
class object_traits_base {
public : 
    // convert an object_traits<T> to object_traits<U>
    template<typename U>
    struct rebind {
        typedef object_traits_base<U> other;
    };

public : 
    inline explicit object_traits_base() {}
    inline ~object_traits_base() {}
    template <typename U>
    inline explicit object_traits_base(object_traits_base<U> const&) {}

    // address
    inline T* address(T& r) { return &r; }
    inline T const* address(T const& r) { return &r; }

    inline void construct(T* p, const T& t) { new(p) T(t); }
    inline void destroy(T* p) { p->~T(); }
};

template<typename T>
class object_traits : public object_traits_base<T> {
public : 
    inline explicit object_traits() : object_traits_base<T>() {}
    inline ~object_traits() {}
    template <typename U>
    inline explicit object_traits(object_traits<U> const&ref)
	: object_traits_base<T>(ref) {}

};

};

#endif // ALC_OBJTRAITS_H

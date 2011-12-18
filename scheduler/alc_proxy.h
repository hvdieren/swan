// -*- c++ -*-
#ifndef ALC_PROXY_H
#define ALC_PROXY_H

#include <limits>
#include <vector>

namespace alc {

template<typename T>
class proxy_alloc_policy {
public : 
    // typedefs
    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

public : 
    // convert a proxy_alloc_policy<T> to proxy_alloc_policy<U>
    template<typename U>
    struct rebind {
        typedef proxy_alloc_policy<U> other;
    };

public : 
    inline explicit proxy_alloc_policy() { }
    inline ~proxy_alloc_policy() { }
    inline explicit proxy_alloc_policy(proxy_alloc_policy const& pa) { }
    template <typename U, typename A>
    inline explicit proxy_alloc_policy(proxy_alloc_policy<U, A> const& pa) { }

    const Allocator * get_proxy_allocator() const {
	extern __thread Allocator * tls_allocator;
	return tls_allocator;
    }
    
    // memory allocation
    inline pointer allocate(size_type cnt, 
			    typename std::allocator<void>::const_pointer
			    p = 0) { 
	extern __thread Allocator * tls_allocator;
	assert( tls_allocator );
	return (pointer)tls_allocator->allocate( sizeof(T)*cnt, p );
    }
    inline void deallocate(pointer p, size_type s) {
	extern __thread Allocator * tls_allocator;
	assert( tls_allocator );
	tls_allocator->deallocate( (typename Allocator::pointer)p, s );
    }

    // size
    inline size_type max_size() const { 
	extern __thread Allocator * tls_allocator;
        return tls_allocator->max_size();
    }
};    //    end of class proxy_alloc_policy


// determines if memory from another
// allocator can be deallocated from this one
template<typename T, typename T2, typename Allocator>
inline bool operator==(proxy_alloc_policy<T, Allocator> const&, 
		       proxy_alloc_policy<T2, Allocator> const&) { 
    return true;
}
template<typename T, typename OtherAllocator, typename Allocator>
inline bool operator==(proxy_alloc_policy<T, Allocator> const&, 
		       OtherAllocator const&) { 
    return false; 
}

};

#endif // ALC_PROXY_H

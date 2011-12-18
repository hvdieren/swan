// -*- c++ -*-
#ifndef ALC_STDPOL_H
#define ALC_STDPOL_H

#include <limits>

namespace alc {

template<typename T>
class standard_alloc_policy {
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
    // convert a standard_alloc_policy<T> to standard_alloc_policy<U>
    template<typename U>
    struct rebind {
        typedef standard_alloc_policy<U> other;
    };

public : 
    inline explicit standard_alloc_policy() {}
    inline ~standard_alloc_policy() {}
    inline explicit standard_alloc_policy(standard_alloc_policy const&) {}
    template <typename U>
    inline explicit standard_alloc_policy(standard_alloc_policy<U> const&) {}
    
    // memory allocation
    inline pointer allocate(size_type cnt, 
			    typename std::allocator<void>::const_pointer = 0) { 
        return reinterpret_cast<pointer>(::operator new(cnt * sizeof (T))); 
    }
    inline void deallocate(pointer p, size_type) { ::operator delete(p); }

    // size
    inline size_type max_size() const { 
        return std::numeric_limits<size_type>::max(); 
    }
};    //    end of class standard_alloc_policy


// determines if memory from another
// allocator can be deallocated from this one
template<typename T, typename T2>
inline bool operator==(standard_alloc_policy<T> const&, 
		       standard_alloc_policy<T2> const&) { 
    return true;
}
template<typename T, typename OtherAllocator>
inline bool operator==(standard_alloc_policy<T> const&, 
		       OtherAllocator const&) { 
    return false; 
}

};

#endif // ALC_STDPOL_H

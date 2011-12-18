// -*- c++ -*-

#ifndef ALC_ALLOCATOR_H
#define ALC_ALLOCATOR_H

#include "alc_objtraits.h"
#include "alc_stdpol.h"

namespace alc {

template<typename T, typename Policy = standard_alloc_policy<T>,
	 typename Traits = object_traits<T> >
class allocator : public Policy, public Traits {
private : 
    typedef Policy AllocationPolicy;
    typedef Traits TTraits;

public : 
    typedef typename AllocationPolicy::size_type size_type;
    typedef typename AllocationPolicy::difference_type difference_type;
    typedef typename AllocationPolicy::pointer pointer;
    typedef typename AllocationPolicy::const_pointer const_pointer;
    typedef typename AllocationPolicy::reference reference;
    typedef typename AllocationPolicy::const_reference const_reference;
    typedef typename AllocationPolicy::value_type value_type;

public : 
    template<typename U>
    struct rebind {
    private:
	typedef typename AllocationPolicy::template rebind<U>::other other_pol;
	typedef typename TTraits::template rebind<U>::other other_traits;
    public:
	typedef allocator<U, other_pol, other_traits > other;
	// typedef allocator<U, typename AllocationPolicy::rebind<U>::other, 
	// typename TTraits::rebind<U>::other > other;
    };

public : 
    inline explicit allocator() {}
    inline ~allocator() { }
    inline allocator(allocator const& rhs) : Policy(rhs), Traits(rhs) { }
    template <typename U>
    inline allocator(allocator<U> const&) { }
    template <typename U, typename P, typename T2>
    inline allocator(allocator<U, P, T2> const& rhs)
	: Policy(rhs), Traits(rhs) { }
};    //    end of class allocator


// determines if memory from another
// allocator can be deallocated from this one
template<typename T, typename P, typename Tr>
inline bool operator==(allocator<T, P, 
		       Tr> const& lhs, allocator<T, 
		       P, Tr> const& rhs) { 
    return operator==(static_cast<P&>(lhs), 
		      static_cast<P&>(rhs)); 
}
template<typename T, typename P, typename Tr, 
	 typename T2, typename P2, typename Tr2>
inline bool operator==(allocator<T, P, 
		       Tr> const& lhs, allocator<T2, P2, Tr2> const& rhs) { 
    return operator==(static_cast<P&>(lhs), 
		      static_cast<P2&>(rhs)); 
}
template<typename T, typename P, typename Tr, typename OtherAllocator>
inline bool operator==(allocator<T, P, 
		       Tr> const& lhs, OtherAllocator const& rhs) { 
    return operator==(static_cast<P&>(lhs), rhs); 
}
template<typename T, typename P, typename Tr>
inline bool operator!=(allocator<T, P, Tr> const& lhs, 
		       allocator<T, P, Tr> const& rhs) { 
    return !operator==(lhs, rhs); 
}
template<typename T, typename P, typename Tr, 
	 typename T2, typename P2, typename Tr2>
inline bool operator!=(allocator<T, P, Tr> const& lhs, 
		       allocator<T2, P2, Tr2> const& rhs) { 
    return !operator==(lhs, rhs); 
}
template<typename T, typename P, typename Tr, 
	 typename OtherAllocator>
inline bool operator!=(allocator<T, P, 
		       Tr> const& lhs, OtherAllocator const& rhs) { 
    return !operator==(lhs, rhs); 
}

};

#endif // ALC_ALLOCATOR_H

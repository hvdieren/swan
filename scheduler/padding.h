// -*- c++ -*-
#ifndef PADDING_H
#define PADDING_H

#include "config.h"

#include <cassert>

template<size_t nbytes>
struct padding {
private:
    char padding[nbytes];
};

template<>
struct padding<0> {
};

template<size_t multi, size_t nbytes>
struct pad_multiple {
private:
    char padding[(multi-(nbytes%multi))%multi];
};

// ---------------------------------------------------------------------
// Aligning objects of a class.
// ---------------------------------------------------------------------
template<typename Base, size_t Alignment>
class aligned_class : public Base {
    // Padding
    static const size_t data_size = sizeof(Base);
    pad_multiple<Alignment,data_size> padding;

    void pad_check() const {
	static_assert( (sizeof(*this) % Alignment) == 0,
		       "template instantiation of aligned_mutex: "
		       "alignment failed" );
    }
};


#endif // PADDING_H

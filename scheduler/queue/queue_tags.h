// -*- c++ -*-

#ifndef SWAN_QUEUE_QUEUE_DEP_H
#define SWAN_QUEUE_QUEUE_DEP_H

#include "swan/queue/queue_version.h"

namespace obj {

// Base classes for dependency tags

template<typename QMetaData>
class queuedep_tags_base : public all_tags_base {
    queue_version<QMetaData> queue;

public:
    queuedep_tags_base( queue_version<QMetaData> * parent,
			typename queue_version<QMetaData>::qmode_t mode,
			long fixed_length = -1 )
	: queue( parent, mode, fixed_length ) { }

    queue_version<QMetaData> * get_queue_version() { return &queue; }
};


template<typename QMetaData>
class popdep_tags_base : public queuedep_tags_base<QMetaData> {
public:
    popdep_tags_base( queue_version<QMetaData> * parent )
	: queuedep_tags_base<QMetaData>(
	    parent, queue_version<QMetaData>::qm_pop ) { }
};

template<typename QMetaData>
class pushpopdep_tags_base : public queuedep_tags_base<QMetaData> {
public:
    pushpopdep_tags_base( queue_version<QMetaData> * parent )
	: queuedep_tags_base<QMetaData>(
	    parent, queue_version<QMetaData>::qm_pushpop ) { }
};

template<typename QMetaData>
class pushdep_tags_base : public queuedep_tags_base<QMetaData> {
public:
    pushdep_tags_base( queue_version<QMetaData> * parent )
	: queuedep_tags_base<QMetaData>(
	    parent, queue_version<QMetaData>::qm_push ) { }
};

template<typename QMetaData>
class prefixdep_tags_base : public queuedep_tags_base<QMetaData> {
public:
    prefixdep_tags_base( queue_version<QMetaData> * parent, size_t length )
	: queuedep_tags_base<QMetaData>(
	    parent, queue_version<QMetaData>::qm_prefix, length ) { }
};

template<typename QMetaData>
class suffixdep_tags_base : public queuedep_tags_base<QMetaData> {
public:
    suffixdep_tags_base( queue_version<QMetaData> * parent, size_t length )
	: queuedep_tags_base<QMetaData>(
	    parent, queue_version<QMetaData>::qm_suffix, length ) { }
};

} //end namespace obj

#endif // SWAN_QUEUE_QUEUE_DEP_H

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
			typename queue_version<QMetaData>::qmode_t mode )
	: queue( parent, mode ) { }

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

} //end namespace obj

#endif // SWAN_QUEUE_QUEUE_DEP_H

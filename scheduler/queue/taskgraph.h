// -*- c++ -*-
/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.gr)
 * 
 * This file is part of Swan.
 * 
 * Swan is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Swan is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Swan.  If not, see <http://www.gnu.org/licenses/>.
 */

/* queue/taskgraph.h
 * This file implements a minimal taskgraph as it applies to queues.
 */
#ifndef QUEUE_TASKGRAPH_H
#define QUEUE_TASKGRAPH_H

#include "swan/wf_frames.h"
#include "swan/lock.h"
#include "swan/padding.h"

namespace obj {

// ----------------------------------------------------------------------
// queue_tags: tag storage for queue dependency usage types
// ----------------------------------------------------------------------
class task_metadata;

// queue_tags stores the accessed generation and links the task on the task list.
// Required for queue types 
class queue_tags {
    task_metadata * st_task;
    queue_tags * qt_next;

    friend class queue_metadata;
};

// ----------------------------------------------------------------------
// queue_metadata: dependency-tracking metadata for queues
// ----------------------------------------------------------------------
class queue_metadata {
    typedef cas_mutex_v<uint8_t> mutex_t;

    queue_tags * youngest;
    mutex_t mutex;

private:
    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }

public:
    queue_metadata() : youngest( 0 ) { }
    ~queue_metadata() {
	assert( !youngest && "Must have finished all tasks when "
		"destructing queue_metadata" );
    }

    // Register users of this queue.
    void add_task( task_metadata * t, queue_tags * tags );

    // Erase links if we are about to destroy a task
    void del_task( task_metadata * fr, queue_tags * tags );

    bool has_tasks() const { return youngest != 0; }
};

// ----------------------------------------------------------------------
// Queue dependency tags
// ----------------------------------------------------------------------
// Popdep (input) dependency tags - fully serialized with other pop and pushpop
class popdep_tags : public popdep_tags_base<queue_metadata>,
		    public queue_tags {
public:
    popdep_tags( queue_version<queue_metadata> * parent )
	: popdep_tags_base( parent ) { }
};

// Pushpopdep (input/output) dependency tags - fully serialized with other
// pop and pushpop
class pushpopdep_tags : public pushpopdep_tags_base<queue_metadata>,
			public queue_tags {
public:
    pushpopdep_tags( queue_version<queue_metadata> * parent )
	: pushpopdep_tags_base( parent ) { }
};

// Pushdep (output) dependency tags
class pushdep_tags : public pushdep_tags_base<queue_metadata> {
public:
    pushdep_tags( queue_version<queue_metadata> * parent )
	: pushdep_tags_base( parent ) { }
};

// ----------------------------------------------------------------------
// Dependency handling traits
// ----------------------------------------------------------------------
// queue pop dependency traits
template<>
struct dep_traits<queue_metadata, task_metadata, popdep> {
    template<typename T>
    static void
    arg_issue( task_metadata * fr, popdep<T> & obj,
	       typename popdep<T>::dep_tags * tags ) {
	queue_metadata * md = obj.get_version()->get_metadata();
	md->add_task( fr, tags );
    }
    template<typename T>
    static bool
    arg_ini_ready( const popdep<T> & obj ) {
	const queue_metadata * md = obj.get_version()->get_metadata();
	return !md->has_tasks();
    }
    template<typename T>
    static void
    arg_release( task_metadata * fr, popdep<T> & obj,
		 typename popdep<T>::dep_tags & tags  ) {
	queue_metadata * md = obj.get_version()->get_metadata();
	md->del_task( fr, &tags );
    }
};

// queue push dependency traits
template<>
struct dep_traits<queue_metadata, task_metadata, pushdep> {
    template<typename T>
    static void arg_issue( task_metadata * fr, pushdep<T> & obj,
			   typename pushdep<T>::dep_tags * sa ) {
    }
    template<typename T>
    static bool arg_ini_ready( const pushdep<T> & obj ) {
	return true;
    }
    template<typename T>
    static void arg_release( task_metadata * fr, pushdep<T> & obj,
			     typename pushdep<T>::dep_tags & sa  ) {
    }
};

} // end of namespace obj

#endif // QUEUE_TASKGRAPH_H

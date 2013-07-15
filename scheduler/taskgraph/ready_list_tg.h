// -*- c++ -*-
/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.gr)
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

/* ready_list_tg.h
 * This file implements a ready-list for any of the taskgraph schemes,
 * typically those where links between tasks are explicitly maintained.
 */
#ifndef TASKGRAPH_READYLIST_TG_H
#define TASKGRAPH_READYLIST_TG_H

#include <cstdint>
#include <iostream>

#include "swan/lock.h"

template<typename task_type_, typename stored_task_type_,
	 typename container_type_>
struct taskgraph_traits {
    typedef task_type_ task_type;
    typedef stored_task_type_ stored_task_type;

    // Convert stored task type to required task type
    static task_type * get_task( stored_task_type * );

    // Run acquire action on task and return true if successful
    static bool acquire( task_type * );
};

// ----------------------------------------------------------------------
// taskgraph: task graph roots in ready_list
// ----------------------------------------------------------------------
template<typename task_type_, typename stored_task_type_,
	 typename container_type_>
class taskgraph {
    typedef task_type_ task_type;
    typedef stored_task_type_ stored_task_type;
    typedef container_type_ ready_list_type;
    typedef mcs_mutex mutex_t;

    typedef taskgraph_traits<task_type, stored_task_type,
			     container_type_> traits;

private:
    ready_list_type ready_list;
    mutable mutex_t mutex;

public:
    ~taskgraph() {
	assert( ready_list.empty() && "Pending tasks at destruction time" );
    }

    task_type * get_ready_task() {
	mcs_mutex::node node;
	mutex.lock( &node );
	task_type * task = 0;
	if( !ready_list.empty() ) {
	    for( auto I=ready_list.begin(), E=ready_list.end();
		 I != E; ++I ) {
		task_type * t = traits::get_task( *I );
		if( traits::acquire( t ) ) {
		    ready_list.erase( I );
		    task = t;
		    break;
		}
	    }
	}
	mutex.unlock( &node );
	return task;
    }

    void add_ready_task( task_type * fr ) {
	mcs_mutex::node node;
	mutex.lock( &node );
	ready_list.push_back( fr );
	mutex.unlock( &node );
    }

    // Don't need a lock in this check because it is based on polling a
    // single variable
    bool empty() const { return ready_list.empty(); }
};

#endif // TASKGRAPH_READYLIST_TG_H

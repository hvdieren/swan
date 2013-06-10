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

#include "swan/object.h"

namespace obj {

// We do not use this specialization of a taskgraph in any of the tickets
// schemes.
#if OBJECT_TASKGRAPH != 1 && OBJECT_TASKGRAPH != 8 && OBJECT_TASKGRAPH != 11 && OBJECT_TASKGRAPH != 16
void queue_metadata::add_task( task_metadata * t, queue_tags * tags ) {
    lock();
    tags->st_task = t;
    tags->qt_next = 0;
    if( youngest ) {
	t->add_incoming(); // 1
	youngest->qt_next = tags;
    }
    youngest = tags;
    unlock();
}

void queue_metadata::del_task( task_metadata * fr, queue_tags * tags ) {
    lock();
    if( youngest == tags )
	youngest = 0;
    unlock();
    if( queue_tags * nt = tags->qt_next ) {
	// By construction, nt->st_task is a pending frame. When selecting
	// the task, erase it from the tags because it will changed from
	// a pending frame to a stack frame, which invalidates the pointer.
	task_metadata * t = nt->st_task;
	if( t->del_incoming() )
	    fr->get_graph()->add_ready_task(
		pending_metadata::get_from_task( t ) );
	nt->st_task = 0;
    }
}
#endif

};

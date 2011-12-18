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

// -*- c++ -*-
/* wf_frames.h
 * This file defines the types of stack frames in the scheduler.
 */
#ifndef WF_FRAMES_H
#define WF_FRAMES_H

class full_frame;
class stack_frame;
class pending_frame;

//----------------------------------------------------------------------
// Retrieving the frame that holds the task graph metadata
//----------------------------------------------------------------------
template<typename T>
struct stack_frame_traits {
    // static typename T::stack_frame_type * get_frame( T * elm );

    // static T * get_pending_frame( typename T::obj_base_frame_type * pf );
    // static typename T::obj_base_frame_type * get_obj_base_frame( T * pf );
};

template<typename T>
struct pending_frame_traits {
};


#endif // WF_FRAMES_H

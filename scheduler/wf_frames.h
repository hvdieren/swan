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

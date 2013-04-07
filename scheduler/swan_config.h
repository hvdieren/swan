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

/*
 * Header file to summarize all compile-time configuration switches.
 */

#ifndef CONFIG_H
#define CONFIG_H

#include "auto_config.h"

/* If MODE is defined as 1, we set the configuration to a fast, non-debugging
 * configuration. If MODE is 0, we set a debugging configuration. Else we
 * use the list below.
 */
#if defined(MODE)
#if MODE
// Fast mode
#define SUPPORT_GDB 0
#define DBG_CONTINUATION 0
#define DBG_VERIFY 0
#define DBG_LOGGER 0
#define DBG_SF_LOCKER 0
#define DBG_MCS_MUTEX 0 // with is faster due to assembly code sequence generated
#define PROFILE_WORKER 0
#define PROFILE_WORKER_SUMMARY 0
#define PROFILE_SPAWN_DEQUE 0
#define PROFILE_OBJECT 0
#define OBJECT_INOUT_RENAME 0
#define TIME_STEALING 0
#define PREFERRED_MUTEX cas_mutex
#define FF_MCS_MUTEX 1
#define NDEBUG
#define TRACING 0
#define DEBUG_CERR 0
#define IMPROVED_STUBS 1
#else
// Debugging mode
#define SUPPORT_GDB 1
#define DBG_CONTINUATION 1
#define DBG_VERIFY 0
#define DBG_LOGGER 1
#define DBG_SF_LOCKER 1
#define DBG_MCS_MUTEX 1
#define PROFILE_WORKER 0
#define PROFILE_WORKER_SUMMARY 1
#define PROFILE_SPAWN_DEQUE 0
#define PROFILE_OBJECT 0
#define OBJECT_INOUT_RENAME 0
#define TIME_STEALING 0
#define PREFERRED_MUTEX cas_mutex
#define FF_MCS_MUTEX 1
#define TRACING 0
#define DEBUG_CERR 1
#define IMPROVED_STUBS 1
#endif

#else
// Manual configuration

/* When SUPPORT_GDB is 1, the split_stub's arguments are copied to the
 * local stack to help gdb backtrace the call stack.
 */
#define SUPPORT_GDB 0

/* When DBG_CONTINUATION is 1, the return base pointer and stack pointer are
 * checkpointed each time a frame is created and their validity is checked
 * when resuming the frame.
 */
#define DBG_CONTINUATION 0

/* When DBG_VERIFY is 1, it is regularly checked that the return base pointer,
 * return stack pointer and our own stack pointer all point to the correct
 * class stack_frame.
 */
#define DBG_VERIFY 0

/* Activate the Logger.
 */
#define DBG_LOGGER 0

/* When DBG_SF_LOCKER is 1, stack_frames keep track of who has a lock on them
 */
#define DBG_SF_LOCKER 0

/* PROFILE_WORKER: enable profiling counters in the workers.
 * PROFILE_WORKER_SUMMARY: only show summary over all workers.
 */
#define PROFILE_WORKER 0
#define PROFILE_WORKER_SUMMARY 1

/* PROFILE_SPAWN_DEQUE: enable profiling counters in the spawn deques.
 */
#define PROFILE_SPAWN_DEQUE 0

/* PROFILE_SPAWN_DEQUE: enable profiling counters in object.h.
 */
#define PROFILE_OBJECT 0

/* OBJECT_INOUT_RENAME: enable renaming of inout dependencies (value 1),
 * potentially by scheduling an additional task (value 2).
 */
#define OBJECT_INOUT_RENAME 0

/* TIME_STEALING: measure how much time is spent in stealing.
 */
#define TIME_STEALING 0

/* PREFERRED_MUTEX: the preferred mutex type.
 */ 
#define PREFERRED_MUTEX cas_mutex

/* FF_MCS_MUTEX: full_frame contains an mcs_mutex.
 */
#define FF_MCS_MUTEX 1

/* DBG_MCS_MUTEX: debug mcs_mutex.
 */
#define DBG_MCS_MUTEX 0

/* TRACING: output a trace of function pointers and ready/pending state.
 */
#define TRACING 0

/* DEBUG_CERR: separate each thread's error output stream on a distinct
 * file, tagged with timestamp and thread id.
 */
#define DEBUG_CERR 0

/* IMPROVED_STUBS: try better stub execution, but on the brink of being
 * incorrect, due to gcc assumptions.
 */
#define IMPROVED_STUBS 1
#endif // MODE

/*
 * The remaining flags concern functionality of the runtime system.
 */

/* OBJECT_TASKGRAPH: implementation of task grapn
 * 0: no task graphs, no object dependency tracking
 * 1: tickets
 *    file = tickets.h
 * 2: taskgraph
 *    file = taskgraph.h
 * 3: taskgraph with linked lists embedded in existing structures
 *    including the deps list, but does not support commutativity and reductions
 *    file = etaskgraph.h
 * 4: embeddded task graph with multiple generations
 *    file = egtaskgraph.h
 * 5: taskgraph (2) but with support for commutativity and reductions
 *    file = ctaskgraph.h
 *    currently best non-embedded single-generation/graph scheme
 * 6: generational taskgraph (4), not embedded, with support for commutativity
 *    file = ecgtaskgraph.h
 * 7: embedded generational taskgraph (4) with support for commutativity
 *    and reductions
 *    file = ecgtaskgraph.h
 * 8: vector-optimized tickets
 * 9: taskgraph with commutativity and reductions (5) with storage of
 *    generation task lists embedded in tags
 *    file = ctaskgraph.h
 *    currently best embedded single-generation/graph scheme
 * 10: like 6, but traverse next generation list only for last releasing task
 *     in a generation
 *    file = ecgtaskgraph.h
 *    currently best non-embedded generational/multi-generation/hypergraph
 * 11: like 7, but traverse next generation list only for last releasing task
 *     in a generation
 *    file = ecgtaskgraph.h
 *    currently best embedded generational/multi-generation/hypergraph
 * 12: global tickets with ROB
 * 13/14: Hypergraph idea implemented as a list with flags on the tasks
 *     indicating when a generation ends. 13 uses distinct list nodes, 14 uses
 *     list nodes embedded in the tasks.
 */
#ifndef OBJECT_TASKGRAPH
#define OBJECT_TASKGRAPH 1
#endif

/* Enable/disable support for commutativity.
 */
#ifndef OBJECT_COMMUTATIVITY
#define OBJECT_COMMUTATIVITY 1
#endif

/* Enable/disable support for reductions.
 */
#ifndef OBJECT_REDUCTION
#define OBJECT_REDUCTION 1
#endif

/* Aligning to cache block size (log2)
 */
#define CACHE_ALIGNMENT 64
#define __cache_aligned __attribute__((aligned (CACHE_ALIGNMENT)))

/* Stack frame size
 */
// #define STACK_FRAME_SIZE 8192
#define STACK_FRAME_SIZE 32768
// #define STACK_FRAME_SIZE 4096

/* Using HWLOC to schedule OS threads and analyze cache hierarchy
 */
#define USE_HWLOC 0

/* Disable techniques not present in the PACT11_VERSION of the scheduler.
 * This concerns only techniques added in the March 2011 - August 2011
 * timeframe.
 */
#define PACT11_VERSION 0

#endif // CONFIG_H

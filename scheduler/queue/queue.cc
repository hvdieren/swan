#include "swan/debug.h"
#include "swan/platform.h"
#include "swan/object.h" // proxy for tickets.h
#include "swan/queue/queue_t.h"

namespace obj {

__thread queue_version_allocator * queue_v_allocator = 0;
__thread segmented_queue_allocator * segm_queue_allocator = 0;
__thread queue_segment_allocator * qs_allocator = 0;

} // namespace obj

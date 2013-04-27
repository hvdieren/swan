#include "swan/debug.h"
#include "swan/platform.h"
#include "swan/object.h" // proxy for tickets.h
#include "swan/queue/queue_t.h"
#include "swan/queue/queue_segment.h"

#include "swan/wf_worker.h"

namespace obj {

#if PROFILE_QUEUE
profile_queue & get_profile_queue() {
    return worker_state::get_thread_worker_state()->get_profile_worker().queue;
}
#endif // PROFILE_QUEUE

} // namespace obj

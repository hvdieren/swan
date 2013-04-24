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

void queue_index::insert( queue_segment * seg ) {
    assert( seg->get_logical_pos() >= 0 );
    lock();
    idx.insert( seg );
    unlock();
    // errs() << "Index " << this << " insert logical="
	   // << seg->get_logical_head() << '-'
	   // << seg->get_logical_tail()
	   // << " seg " << *seg << std::endl;
}

queue_segment * queue_index::lookup( size_t logical, size_t push_seqno ) {
    queue_key_t key = { logical, push_seqno };

    lock();
    // errs() << "Index " << this << " lookup logical="
	   // << logical << " seqno=" << push_seqno << " end=" << get_end() << std::endl;
    queue_segment * secondary = 0;
    queue_segment * eq = idx.find( key, secondary );
    if( !eq )
	eq = secondary;
    if( eq )
	eq->check_hash();
    // errs() << "Index " << this << " lookup logical="
	   // << logical << " found " << eq << std::endl;
    unlock();
    return eq;
}

void queue_index::erase( queue_segment * seg ) {
    lock();
    // errs() << "Index " << this << " erase " << *seg << std::endl;
    idx.remove( seg );
    unlock();
}

} // namespace obj

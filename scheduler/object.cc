#include "object.h"

namespace obj {

#if PROFILE_OBJECT && OBJECT_TASKGRAPH > 0
statistics statistic;

void dump_statistics() {
    statistic.dump_statistics();
}
#endif // PROFILE_OBJECT && OBJECT_TASKGRAPH > 0

#if OBJECT_TASKGRAPH == 4
__thread generation_allocator * tls_egtg_allocator = 0;
#endif

#if OBJECT_TASKGRAPH == 7 || OBJECT_TASKGRAPH == 6 \
    || OBJECT_TASKGRAPH == 10 || OBJECT_TASKGRAPH == 11
__thread generation_allocator * tls_ecgtg_allocator = 0;
#endif

};

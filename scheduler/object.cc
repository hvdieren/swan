/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.org)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.org)
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

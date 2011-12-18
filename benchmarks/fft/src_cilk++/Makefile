#
# Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
# Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.gr)
# Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.gr)
# 
# This file is part of Swan.
# 
# Swan is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Swan is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Swan.  If not, see <http://www.gnu.org/licenses/>.
#

PROG=fft

include ../../Makefile.cilk++
include ../../util/Makefile.use_us

$(PROG): $(PROG).cc getoptions.o

CFLAGS += -I../common
CXXFLAGS += -I../common

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

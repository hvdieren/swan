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

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AC_CONFIG_SRCDIR([scheduler/alc_allocator.h])
AC_CONFIG_HEADERS([auto_config.h])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_RANLIB
AC_PATH_PROG([PERL],[perl])

# Checks for libraries.
# Replace `pthread_create' is a function in `-lpthread':
AC_CHECK_LIB([pthread], [pthread_create])
AC_CHECK_LIB([hwloc], [hwloc_topology_init])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_CHECK_FUNCS([atexit gettimeofday memset munmap strerror])

# Declare package options
AC_ARG_ENABLE([fortify-source],
	[AS_HELP_STRING([--disable-fortify-source],
		[gcc's fortify source functionality may break swan @<:@default=2@:>@])],
	[value_fortify_source=$enableval],
	[value_fortify_source=default])

AS_IF([test "x$value_fortify_source" != "xdefault"],
	    [AS_IF([test "x$value_fortify_source" == "xno"],
	    	    [value_fortify_source=0]);
	     AS_VAR_APPEND(CFLAGS,[" -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=$value_fortify_source"]);
	     AS_VAR_APPEND(CXXFLAGS,[" -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=$value_fortify_source"])
	     ])

AC_CONFIG_FILES([Makefile
		 scheduler/Makefile.flags
		 scheduler/Makefile
		 tests/Makefile
		 benchmarks/Makefile.wf
		 benchmarks/bzip2f/src_cilk++/Makefile
		 benchmarks/bzip2f/src_wf/Makefile
		 benchmarks/fft/src_wf/Makefile
		 benchmarks/jacobi/src_wfo/Makefile
		 benchmarks/jacobi/src_wfo/create.sh
		 benchmarks/ubench/src_wf/Makefile
		 benchmarks/ubench/src_wf/gen.pl
		 benchmarks/ubench/src_wf/create.sh
		 benchmarks/matmul2/src_wfo/Makefile
		 benchmarks/rectmul_bis/src_wfo/Makefile
		 benchmarks/rectmul_bis/src_wfo/create.sh
		 benchmarks/cholesky2/src_wfo/Makefile
		 benchmarks/cholesky2/src_wfo/create.sh
		 benchmarks/sparse_lu/src_wfo/Makefile
		 benchmarks/sparse_lu/src_wfo/create.sh
                 scripts/create.sh
                 util/Makefile
                 util/Makefile.use_us
                 util/Makefile.use_cy])
AC_CONFIG_COMMANDS([exec-scripts-create.sh],[chmod u+x scripts/create.sh])
AC_CONFIG_COMMANDS([exec-benchmarks-ubench-src_wf-scripts],[chmod u+x benchmarks/ubench/src_wf/create.sh benchmarks/ubench/src_wf/gen.pl])
AC_CONFIG_COMMANDS([create-swan-queue-dir],[mkdir scheduler/queue])
AC_OUTPUT

CC       = gcc-mp-4.9
CXX      = g++-mp-4.9
LDFLAGS  = 
CFLAGS   = -g -O2 -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0
CXXFLAGS = -g -O2 -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0

srcdir = .

top_srcdir = ..
builddir = .
top_builddir = ..

EXAMPLES = fib pipeline hyperqueue matmul

.PHONY: all

all: examples


include $(top_builddir)/scheduler/Makefile.flags

CXXFLAGS += -I$(top_srcdir)/scheduler -I$(top_srcdir)/util
LDFLAGS += -L$(top_builddir)/util
LDLIBS += -ltime_us

examples: $(EXAMPLES)

matmul: LDLIBS+=../util/getoptions.o 

# Rebuild examples if any of the header files has changed
$(patsubst %,%.o,$(EXAMPLES)): $(patsubst %,$(top_srcdir)/scheduler/%,$(HDRS))

# Rebuild examples if the compiled library has changed
$(patsubst %,%.o,$(EXAMPLES)): $(top_builddir)/scheduler/libschedulers.a

# Generic rule to build an example program
%: %.o
	$(CXX) $(LDFLAGS) $< $(LDLIBS) $(top_builddir)/scheduler/libschedulers.a -o $*

# Cleanup
clean:
	rm -f $(EXAMPLES) $(patsubst %,%.o,$(EXAMPLES))

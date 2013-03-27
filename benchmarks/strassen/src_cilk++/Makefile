PROG=strassen

include ../../Makefile.cilk++
include ../../lib/Makefile.use_us

$(PROG): $(PROG).cc getoptions.o

CXXFLAGS+=-I../common

getoptions.o: ../common/getoptions.c
	$(CXX) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

PROG=lu

include ../../Makefile.cilk++
include ../../lib/Makefile.use_us

$(PROG): $(PROG).cc getoptions.o

CFLAGS += -I../common
CXXFLAGS += -I../common

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

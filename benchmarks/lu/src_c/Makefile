PROG=lu

include ../../Makefile.c
include ../../lib/Makefile.use_us

$(PROG): $(PROG).c getoptions.o

CFLAGS += -I../common
CXXFLAGS += -I../common

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

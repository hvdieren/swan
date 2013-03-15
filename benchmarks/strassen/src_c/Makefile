PROG=strassen

include ../../Makefile.c
include ../../lib/Makefile.use_us

$(PROG): $(PROG).c getoptions.o

CXXFLAGS+=-I../common
CFLAGS+=-I../common

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

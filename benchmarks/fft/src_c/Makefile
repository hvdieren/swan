PROG=fft

include ../../Makefile.c
include ../../util/Makefile.use_us

$(PROG): $(PROG).c getoptions.o

CFLAGS += -I../common
CXXFLAGS += -I../common
LDLIBS += -lm

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

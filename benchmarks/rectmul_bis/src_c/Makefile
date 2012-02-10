PROG=rectmul

include ../../Makefile.c
include ../../lib/Makefile.use_us

$(PROG): LDLIBS+=$(HOME)/ray/lib/libgoto2_nehalem-r1.13_T1.a

$(PROG): $(PROG).c getoptions.o

CFLAGS += -I../common
CXXFLAGS += -I../common

getoptions.o: ../common/getoptions.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f getoptions.o $(PROG)

PROG=J_jacobi

all: $(PROG)

include ../../Makefile.c
include ../../lib/Makefile.use_us

J_jacobi: J_jacobi.c
	$(CC) $(CFLAGS) $< $(LDFLAGS) $(LDLIBS) -o $@

clean:
	rm -f $(PROG)

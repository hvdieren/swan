PROG=J_jacobi

all: $(PROG)

include ../../Makefile.smpss
include ../../lib/Makefile.use_us

J_jacobi: J_jacobi.c
	$(CC) $(CFLAGS) J_jacobi.c $(LDFLAGS) $(LDLIBS) -o $@

clean:
	rm -f $(PROG)

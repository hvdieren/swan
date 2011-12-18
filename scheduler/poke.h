// -*- c++ -*-
#ifndef POKE_H
#define POKE_H

inline void poke(void)  __attribute__((always_inline));

inline void poke(void) {
    void * g_esp, * g_ebp, * g_ebx;
    GET_BP( g_ebp );
    GET_SP( g_esp );
    GET_PR( g_ebx );

    std::cerr << "poke esp: " << g_esp << "\n"
	      << "poke ebp: " << g_ebp << "\n"
	      << "poke ebx: " << g_ebx << "\n";
}

#endif // POKE_H

#ifndef PLATFORM_H
#define PLATFORM_H

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#ifdef __x86_64__
#include "platform_x86_64.h"
#else
#include "platform_i386.h"
#endif
#endif // PLATFORM_H

/*
 * atomic.h
 *
 *  Created on: Jan 10, 2014
 *      Author: wangqiying
 */

#ifndef ATOMIC_H_
#define ATOMIC_H_

#include "config.h"
#include <stdint.h>

namespace ardb
{
    uint64_t atomic_add_uint64(uint64_t *p, uint64_t v);
    uint64_t atomic_sub_uint64(uint64_t *p, uint64_t v);
    int atomic_cmp_set_uint64(uint64_t *p, uint64_t o, uint64_t n);

    uint32_t atomic_add_uint32(uint32_t *p, uint32_t v);
    uint32_t atomic_sub_uint32(uint32_t *p, uint32_t v);
    int atomic_cmp_set_uint32(uint32_t *p, uint32_t o, uint32_t n);

#ifdef HAVE_SYNC_OP
    inline uint64_t atomic_add_uint64(uint64_t *p, uint64_t v)
    {
        return __sync_add_and_fetch(p, v);
    }

    inline uint64_t atomic_sub_uint64(uint64_t *p, uint64_t v)
    {
        return __sync_sub_and_fetch(p, v);
    }
    inline int atomic_cmp_set_uint64(uint64_t *p, uint64_t o, uint64_t n)
    {
        return __sync_bool_compare_and_swap(p, o, n);
    }
#elif (defined(__amd64_) || defined(__x86_64__))
    inline uint64_t atomic_add_uint64(uint64_t *p, uint64_t v)
    {
        asm volatile (
                "lock; xaddq %0, %1;"
                : "+r" (v), "=m" (*p)
                : "m" (*p)
        );
        return (*p);
    }
    inline uint64_t atomic_sub_uint64(uint64_t *p, uint64_t v)
    {
        v = -v;
        asm volatile (
                "lock; xaddq %0, %1;"
                : "+r" (v), "=m" (*p)
                : "m" (*p)
        );
        return (*p);
    }
    inline int atomic_cmp_set_uint64(uint64_t *p, uint64_t o, uint64_t n)
    {
        char result;
        asm volatile (
                "lock; cmpxchgq %3, %0; setz %1;"
                : "=m"(*p), "=q" (result)
                : "m" (*p), "r" (n), "a" (o) : "memory"
        );
        return (result);
    }
#else
#  define HAVE_NO_ATOMIC_OP 1
#endif  //end define the 64 bit
#ifdef HAVE_SYNC_OP
    inline uint32_t atomic_add_uint32(uint32_t *p, uint32_t v)
    {
        return __sync_add_and_fetch(p, v);
    }
    inline uint32_t atomic_sub_uint32(uint32_t *p, uint32_t v)
    {
        return __sync_sub_and_fetch(p, v);
    }
    inline int atomic_cmp_set_uint32(uint32_t *p, uint32_t o, uint32_t n)
    {
        return __sync_bool_compare_and_swap(p, o, n);
    }
#elif (defined(__i386__) || defined(__amd64_) || defined(__x86_64__))
inline uint32_t atomic_add_uint32(uint32_t *p, uint32_t v)
{
    asm volatile (
            "lock; xaddl %0, %1;"
            : "+r" (v), "=m" (*p)
            : "m" (*p)
    );
    return (*p);
}
inline uint32_t atomic_sub_uint32(uint32_t *p, uint32_t v)
{
    v = -v;
    asm volatile (
            "lock; xaddl %0, %1;"
            : "+r" (v), "=m" (*p)
            : "m" (*p)
    );
    return (*p);
}
inline int atomic_cmp_set_uint32(uint32_t *p, uint32_t o, uint32_t n)
{
    char result;
    asm volatile (
            "lock; cmpxchgl %3, %0; setz %1;"
            : "=m"(*p), "=q" (result)
            : "m" (*p), "r" (n), "a" (o) : "memory"
    );
    return (result);
}
#else
#  define HAVE_NO_ATOMIC_OP 1
#endif
}

#endif /*ATOMIC head define**/


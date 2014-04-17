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
    uint64_t atomic_add_uint64(volatile uint64_t *p, uint64_t v);
    uint64_t atomic_sub_uint64(volatile uint64_t *p, uint64_t v);
    int atomic_cmp_set_uint64(volatile uint64_t *p, uint64_t o, uint64_t n);

    uint32_t atomic_add_uint32(volatile uint32_t *p, uint32_t v);
    uint32_t atomic_sub_uint32(volatile uint32_t *p, uint32_t v);
    int atomic_cmp_set_uint32(volatile uint32_t *p, uint32_t o, uint32_t n);

#ifdef HAVE_SYNC_OP
    inline uint64_t atomic_add_uint64(volatile uint64_t *p, uint64_t v)
    {
        return __sync_add_and_fetch(p, v);
    }

    inline uint64_t atomic_sub_uint64(volatile uint64_t *p, uint64_t v)
    {
        return __sync_sub_and_fetch(p, v);
    }
    inline int atomic_cmp_set_uint64(volatile uint64_t *p, uint64_t o, uint64_t n)
    {
        return __sync_bool_compare_and_swap(p, o, n);
    }
#elif (defined(__amd64_) || defined(__x86_64__))
    __inline__ uint64_t xadd_8(volatile uint64_t * pv, const uint64_t av)
        {
          //:"=a" (__res), "=q" (pv): "m"(av), "1" (pv));
          register unsigned long __res;
          __asm__ __volatile__ (
              "movq %2,%0;"
              "lock; xaddq %0,(%1);"
              :"=a" (__res), "=q" (pv): "mr"(av), "1" (pv));
          return __res;
        }

    inline uint64_t atomic_add_uint64(volatile uint64_t *p, uint64_t x)
    {
        return (xadd_8(p, x) + x);
    }
    inline uint64_t atomic_sub_uint64(volatile uint64_t *p, uint64_t x)
    {
        return (xadd_8(p, -x) - x);
    }

    inline int atomic_cmp_set_uint64(volatile uint64_t *p, uint64_t o, uint64_t n)
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
    inline uint32_t atomic_add_uint32(volatile uint32_t *p, uint32_t v)
    {
        return __sync_add_and_fetch(p, v);
    }
    inline uint32_t atomic_sub_uint32(volatile uint32_t *p, uint32_t v)
    {
        return __sync_sub_and_fetch(p, v);
    }
    inline int atomic_cmp_set_uint32(volatile uint32_t *p, uint32_t o, uint32_t n)
    {
        return __sync_bool_compare_and_swap(p, o, n);
    }
#elif (defined(__i386__) || defined(__amd64_) || defined(__x86_64__))
inline uint32_t xadd_4(volatile uint32_t* pVal, uint32_t inc)
{
    unsigned int result;
    unsigned int* pValInt = (unsigned int*)pVal;
    asm volatile(
            "lock; xaddl %%eax, %2;"
            :"=a" (result)
            : "a" (inc), "m" (*pValInt)
            :"memory" );

    return (result);

}
inline uint32_t atomic_add_uint32(volatile uint32_t *p, uint32_t x)
{
    return (xadd_4(p, x) + x);
}
inline uint32_t atomic_sub_uint32(volatile uint32_t *p, uint32_t x)
{
    return (xadd_4(p, -x) - x);
}
inline int atomic_cmp_set_uint32(volatile uint32_t *p, uint32_t o, uint32_t n)
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


/*
 * debug.hpp
 *
 *  Created on: 2011-3-16
 *      Author: qiyingwang
 */

#ifndef DEBUG_H_
#define DEBUG_H_

#define FORCE_CORE_DUMP() do {int *p = NULL; *p=0;} while(0)

/* Evaluates to the same boolean value as 'p', and hints to the compiler that
 * we expect this value to be false. */
#ifdef __GNUC__
#define __UNLIKELY(p) __builtin_expect(!!(p),0)
#else
#define __UNLIKELY(p) (p)
#endif

#define ASSERT(cond)                     \
    do {                                \
        if (__UNLIKELY(!(cond))) {             \
            /* In case a user-supplied handler tries to */  \
            /* return control to us, log and abort here. */ \
            (void)fprintf(stderr,               \
                "%s:%d: Assertion %s failed in %s",     \
                __FILE__,__LINE__,#cond,__func__);      \
            abort();                    \
        }                           \
    } while (0)


#endif /* DEBUG_H_ */

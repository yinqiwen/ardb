/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef DEBUG_H_
#define DEBUG_H_
#include <stdio.h>

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

#define ALLOC_ASSERT(x) \
    do {\
        if (__UNLIKELY (!x)) {\
            fprintf (stderr, "FATAL ERROR: OUT OF MEMORY (%s:%d)\n",\
                __FILE__, __LINE__);\
            abort();\
        }\
    } while (false)

#define CHECK_FATAL(cond, ...)  do{\
	if(cond){\
		 (void)fprintf(stderr,               \
		                "\e[1;35m%-6s\e[m%s:%d: Assertion %s failed in %s\n",     \
		                "[FAIL]", __FILE__,__LINE__,#cond,__func__);      \
         fprintf(stderr, "\e[1;35m%-6s\e[m", "[FAIL]:"); \
         fprintf(stderr, __VA_ARGS__);\
         fprintf(stderr, "\n"); \
         exit(-1);\
	}else{\
		fprintf(stdout, "\e[1;32m%-6s\e[m%s:%d: Assertion %s\n", "[PASS]", __FILE__,__LINE__,#cond);\
	}\
}while(0)

/* Anti-warning macro... */
#define ARDB_NOTUSED(V) ((void) V)

#endif /* DEBUG_H_ */

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

#ifndef COMMON_HPP_
#define COMMON_HPP_
#include <stddef.h>
#define NEW(Ptr,Type) do\
    { \
        try                  \
        {                    \
           Ptr = new Type;   \
        }                    \
        catch(std::bad_alloc)\
        {                    \
            Ptr = NULL;      \
        }                    \
    }while(0)

#define DELETE(Ptr)     do\
    {                   \
        if(NULL != Ptr) \
        {               \
            delete Ptr; \
            Ptr = NULL; \
        }               \
    }while(0)

#define DELETE_A(Ptr) do\
    {                   \
        if(NULL != Ptr) \
        {               \
            delete[] Ptr; \
            Ptr = NULL; \
        }               \
    }while(0)

#define DELETE_R(Ref)     do\
    {                   \
        delete &Ref ;   \
    }while(0)

#define RETURN_FALSE_IF_NULL(x)  do\
    {                    \
        if(NULL == x)    \
           return false; \
    }while(0)

#define RETURN_NULL_IF_NULL(x)  do\
    {                    \
        if(NULL == x)    \
           return NULL; \
    }while(0)

#define RETURN_VALUE_IF_NOTNULL(x) do\
    {                    \
        if(NULL != x)    \
           return x; \
    }while(0)

#define RETURN_IF_NULL(x)  do\
    {                    \
        if(NULL == x)    \
           return; \
    }while(0)

#include <stdint.h>
#ifndef INTTYPES_DEF
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
#endif

namespace ardb
{
    template<typename T>
    struct Type
    {
            typedef void Destructor(T* obj);
    };

    template<typename T>
    void StandardDestructor(T* obj)
    {
        if (NULL != obj)
        {
            delete obj;
        }
    }
    /**
     * Verify type information
     */
    template<typename InheritType>
    struct InstanceOf
    {
        public:
            bool OK;
            template<typename BaseType>
            inline InstanceOf(const BaseType* ptr) :
                    OK(false)
            {
                if (NULL != ptr)
                {
                    const InheritType* sp = dynamic_cast<const InheritType*>(ptr);
                    OK = (NULL != sp);
                }
            }
    };

    /**
     * A tag interface
     */
    class Object
    {
        public:
            virtual ~Object()
            {
            }
    };
}
template<typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];
#define arraysize(array) (sizeof(ArraySizeHelper(array)))

#include <stdio.h>
#include <unistd.h>
#include "config.h"
#include "constants.hpp"
#include "util/address.hpp"
#include "util/runnable.hpp"
#include "util/debug.h"
#include "logger.hpp"

#endif /* COMMON_HPP_ */

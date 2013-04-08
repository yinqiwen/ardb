/*
 * common.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
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

namespace rddb
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
					const InheritType* sp =
							dynamic_cast<const InheritType*>(ptr);
					OK = (NULL != sp);
				}
			}
	};
}
#include "util/address.hpp"
#include "util/runnable.hpp"
#include "util/debug.h"

#include "logger.hpp"

#endif /* COMMON_HPP_ */

/*
 * common.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */

#ifndef COMMON_HPP_
#define COMMON_HPP_

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

#endif /* COMMON_HPP_ */

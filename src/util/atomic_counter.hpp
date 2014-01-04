/*
 Copyright (c) 2007-2014 Contributors as noted in the AUTHORS file

 This file is part of 0MQ.

 0MQ is free software; you can redistribute it and/or modify it under
 the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation; either version 3 of the License, or
 (at your option) any later version.

 0MQ is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Lesser General Public License for more details.

 You should have received a copy of the GNU Lesser General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __ARDB_ATOMIC_COUNTER_HPP_INCLUDED__
#define __ARDB_ATOMIC_COUNTER_HPP_INCLUDED__

#include <stdint.h>
#include "common.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#if (defined __i386__ || defined __x86_64__) && defined __GNUC__
#define HAVE_ATOMIC_COUNTER_X86
#endif

namespace ardb
{

    //  This class represents an integer that can be incremented/decremented
    //  in atomic fashion.
    class atomic_counter_t
    {
        private:
#if defined HAVE_ATOMIC
            //do nothing
#elif defined HAVE_ATOMIC_COUNTER_X86
            //do nothing
#else
            ThreadMutexLock m_lock;
#endif
        public:
            typedef uint32_t integer_t;
            inline atomic_counter_t(integer_t value_ = 0) :
                    value(value_)
            {
            }

            inline ~atomic_counter_t()
            {
            }

            //  Set counter value (not thread-safe).
            inline void set(integer_t value_)
            {
                value = value_;
            }

            //  Atomic addition. Returns the old value.
            inline integer_t add(integer_t increment_)
            {
                integer_t old_value;
#if defined HAVE_ATOMIC
                old_value = __sync_add_and_fetch(&value, increment_);
#elif defined HAVE_ATOMIC_COUNTER_X86
                __asm__ volatile (
                        "lock; xadd %0, %1 \n\t"
                        : "=r" (old_value), "=m" (value)
                        : "0" (increment_), "m" (value)
                        : "cc", "memory");
#else
                m_lock.Lock();
                old_value = value;
                value++;
                m_lock.Unlock();
#endif
                return old_value;
            }

            //  Atomic subtraction. Returns false if the counter drops to zero.
            inline bool sub(integer_t decrement)
            {
#if defined HAVE_ATOMIC
                integer_t old_value = __sync_fetch_and_sub(&value, decrement);
                return old_value - decrement != 0;
#elif defined HAVE_ATOMIC_COUNTER_X86
                integer_t oldval = -decrement;
                volatile integer_t *val = &value;
                __asm__ volatile ("lock; xaddl %0,%1"
                        : "=r" (oldval), "=m" (*val)
                        : "0" (oldval), "m" (*val)
                        : "cc", "memory");
                return oldval != decrement;
#else
                m_lock.Lock();
                if(value > decrement)
                {
                    value -= decrement;
                }else
                {
                    value = 0;
                }
                m_lock.Unlock();
#endif
            }

            inline integer_t get()
            {
                return value;
            }

        private:

            volatile integer_t value;

            atomic_counter_t(const atomic_counter_t&);
            const atomic_counter_t& operator =(const atomic_counter_t&);
    };

}

#endif


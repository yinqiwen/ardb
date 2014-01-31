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
#include "atomic.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#if (defined __i386__ || defined __x86_64__) && defined __GNUC__
#define HAVE_ATOMIC_COUNTER_X86
#endif

namespace ardb
{

    //  This class represents an integer that can be incremented/decremented
    //  in atomic fashion.
    class AtomicInt64
    {
        private:
#if defined HAVE_NO_ATOMIC_OP
            ThreadMutexLock m_lock;
#endif
            volatile int64_t value;
        public:
            inline AtomicInt64(int64_t value_ = 0) :
                    value(value_)
            {
            }

            //  Set counter value (not thread-safe).
            inline void Set(int64_t value_)
            {
                value = value_;
            }

            //  Atomic addition. Returns the old value.
            inline int64_t Add(int64_t increment_)
            {
#if defined HAVE_NO_ATOMIC_OP
                m_lock.Lock();
                value++;
                int64_t new_value = value;
                m_lock.Unlock();
                return new_value;
#else
                return (int64_t) atomic_add_uint64((uint64_t*) &value, increment_);
#endif
            }

            //  Atomic subtraction. Returns false if the counter drops to zero.
            inline bool Sub(int64_t decrement)
            {
#if defined HAVE_NO_ATOMIC_OP
                m_lock.Lock();
                value--;
                int64_t new_value = value;
                m_lock.Unlock();
                return new_value;
#else
                return (int64_t) atomic_sub_uint64((uint64_t*) &value, decrement);
#endif
            }

            inline int64_t Get()
            {
                return value;
            }

        private:
            AtomicInt64(const AtomicInt64&)
            {
            }
            const AtomicInt64& operator =(const AtomicInt64&)
            {
                return *this;
            }
    };

}

#endif


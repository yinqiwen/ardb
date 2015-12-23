/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#ifndef AUTO_LOCK_HPP_
#define AUTO_LOCK_HPP_
#include "lock_mode.hpp"
namespace ardb
{
    template<typename T>
    class LockGuard
    {
        private:
            T& m_lock_impl;
            bool m_check;
        public:
            inline LockGuard(T& lock, bool check = true) :
                    m_lock_impl(lock), m_check(check)
            {
                if (m_check)
                {
                    m_lock_impl.Lock();
                }

            }
            ~LockGuard()
            {
                if (m_check)
                {
                    m_lock_impl.Unlock();
                }
            }
    };

    template<typename T>
    class RWLockGuard
    {
        private:
            T& m_lock_impl;
            bool m_readonly;
        public:
            inline RWLockGuard(T& lock, bool readonly) :
                    m_lock_impl(lock), m_readonly(readonly)
            {
                m_lock_impl.Lock(m_readonly ? READ_LOCK : WRITE_LOCK);

            }
            ~RWLockGuard()
            {
                m_lock_impl.Unlock(m_readonly ? READ_LOCK : WRITE_LOCK);
            }
    };

    template<typename T>
    class ReadLockGuard
    {
        private:
            T& m_lock_impl;
            bool m_check;
        public:
            ReadLockGuard(T& lock, bool check = true) :
                    m_lock_impl(lock), m_check(check)
            {
                if (m_check)
                {
                    m_lock_impl.Lock(READ_LOCK);
                }

            }
            ~ReadLockGuard()
            {
                if (m_check)
                {
                    m_lock_impl.Unlock(READ_LOCK);
                }
            }
    };

    template<typename T>
    class WriteLockGuard
    {
        private:
            T& m_lock_impl;
            bool m_check;
        public:
            WriteLockGuard(T& lock, bool check = true) :
                    m_lock_impl(lock), m_check(check)
            {
                if (m_check)
                {
                    m_lock_impl.Lock(WRITE_LOCK);
                }

            }
            ~WriteLockGuard()
            {
                if (m_check)
                {
                    m_lock_impl.Unlock(WRITE_LOCK);
                }
            }
    };
}

#endif /* AUTO_LOCK_HPP_ */

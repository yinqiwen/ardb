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

#ifndef NOVA_THREADMUTEXLOCK_HPP_
#define NOVA_THREADMUTEXLOCK_HPP_
#include "thread_mutex.hpp"
#include "thread_condition.hpp"
#include "util/time_unit.hpp"
#include "util/time_helper.hpp"
#include <time.h>
namespace ardb
{
	class ThreadMutexLock: public ThreadMutex, public ThreadCondition
	{
		public:
			ThreadMutexLock()
			{
			}
			ThreadMutexLock(ThreadMutex& mutex, ThreadCondition& cond) :
					ThreadMutex(mutex), ThreadCondition(cond)
			{
			}
			//ThreadMutexLock(ThreadMutexCondition& mutexCond);
			/**
			 * wait for specified timeout value, if timeout < 0, then wait for ever
			 */
			bool Wait(uint64 timeout = 0, TimeUnit unit = MILLIS)
			{
				if (timeout <= 0)
				{
					return 0 == pthread_cond_wait(&m_cond, &m_mutex);
				}
				else
				{
					uint64 now = get_current_epoch_micros();
					uint64 inc = microstime(timeout, unit);
					struct timespec ts;
					init_timespec(now + inc, MICROS, ts);
					return 0 == pthread_cond_timedwait(&m_cond, &m_mutex, &ts);
				}
			}
			virtual bool Notify()
			{
				return 0 == pthread_cond_signal(&m_cond);
			}
			virtual bool NotifyAll()
			{
				return 0 == pthread_cond_broadcast(&m_cond);
			}
			bool Lock(uint64 timeout = 0)
			{
				if (0 == timeout)
				{
					return 0 == pthread_mutex_lock(&m_mutex);
				}
				else
				{
#if defined( _WIN64 ) || defined( _WIN32 ) || 1
					return -1;
#else

					struct timespec next;
					get_current_epoch_time(next);
					uint64 nanos = nanostime(timeout, MILLIS);
					add_nanos(next, nanos);
					return 0 == pthread_mutex_timedlock(&m_mutex, &next);

#endif
				}
			}
			bool Unlock()
			{
				return 0 == pthread_mutex_unlock(&m_mutex);
			}
			bool TryLock()
			{
				return 0 == pthread_mutex_trylock(&m_mutex);
			}
			virtual ~ThreadMutexLock()
			{
			}
	};
}

#endif /* THREADMUTEXLOCK_HPP_ */

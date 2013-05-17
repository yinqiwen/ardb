/*
 * ThreadMutexLock.hpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
 */

#ifndef NOVA_THREADMUTEXLOCK_HPP_
#define NOVA_THREADMUTEXLOCK_HPP_
#include "thread_mutex.hpp"
#include "thread_condition.hpp"
#include "util/time_unit.hpp"
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

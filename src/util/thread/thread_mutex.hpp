/*
 * ThreadMutex.hpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
 */

#ifndef NOVA_THREADMUTEX_HPP_
#define NOVA_THREADMUTEX_HPP_
#include <pthread.h>

namespace ardb
{
	class ThreadMutex
	{
		protected:
			pthread_mutex_t m_mutex;
		public:
			ThreadMutex()
			{
				pthread_mutexattr_t attr;
				pthread_mutexattr_init(&attr);
				pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
				pthread_mutex_init(&m_mutex, &attr);
				pthread_mutexattr_destroy(&attr);
			}
			virtual ~ThreadMutex()
			{
				pthread_mutex_destroy(&m_mutex);
			}
			pthread_mutex_t& GetRawMutex()
			{
				return m_mutex;
			}
	};
}
#endif /* THREADMUTEX_HPP_ */

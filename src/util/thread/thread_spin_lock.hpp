/*
 * thread_spin_lock.hpp
 *
 *  Created on: 2011-5-5
 *      Author: qiyingwang
 */

#ifndef THREAD_SPIN_LOCK_HPP_
#define THREAD_SPIN_LOCK_HPP_
#include <pthread.h>

namespace ardb
{
	class ThreadSpinLock
	{
		private:
			pthread_spinlock_t m_raw_lock;
		public:
			inline ThreadSpinLock()
			{
				pthread_spin_init(&m_raw_lock, 0);
			}
			inline bool Lock()
			{
				return pthread_spin_lock(&m_raw_lock) == 0;
			}
			inline bool Unlock()
			{
				return pthread_spin_unlock(&m_raw_lock) == 0;
			}
			inline bool TryLock()
			{
				return pthread_spin_trylock(&m_raw_lock) == 0;
			}
			inline ~ThreadSpinLock()
			{
				pthread_spin_destroy(&m_raw_lock);
			}
	};
}

#endif /* THREAD_SPIN_LOCK_HPP_ */

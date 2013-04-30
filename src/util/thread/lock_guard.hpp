/*
 * auto_lock.hpp
 *
 *  Created on: 2011-4-2
 *      Author: qiyingwang
 */

#ifndef AUTO_LOCK_HPP_
#define AUTO_LOCK_HPP_
namespace ardb
{
	template<typename T>
	class LockGuard
	{
		private:
			T& m_lock_impl;
		public:
			inline LockGuard(T& lock) :
					m_lock_impl(lock)
			{
				m_lock_impl.Lock();
			}
			~LockGuard()
			{
				m_lock_impl.Unlock();
			}
	};
}

#endif /* AUTO_LOCK_HPP_ */

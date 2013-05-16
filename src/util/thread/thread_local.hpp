/*
 * thread_local.hpp
 *
 *  Created on: 2011-4-11
 *      Author: qiyingwang
 */

#ifndef THREAD_LOCAL_HPP_
#define THREAD_LOCAL_HPP_
#include <pthread.h>
namespace ardb
{
	template<typename T>
	class ThreadLocal
	{
		private:
			pthread_key_t m_key;
			static void Destroy(void *x)
			{
				T* obj = static_cast<T*>(x);
				delete obj;
			}
			virtual T* InitialValue()
			{
				return NULL;
			}
		public:
			ThreadLocal()
			{
				pthread_key_create(&m_key, &ThreadLocal::Destroy);
			}

			~ThreadLocal()
			{
				pthread_key_delete(m_key);
			}

			T& GetValue()
			{
				T* local_thread_value = static_cast<T*>(pthread_getspecific(
						m_key));
				if (NULL == local_thread_value)
				{
					T* newObj = InitialValue();
					pthread_setspecific(m_key, newObj);
					local_thread_value = newObj;
				}
				return *local_thread_value;
			}
			void SetValue(const T& v)
			{
				T& t = GetValue();
				t = v;
			}
	};
}
#endif /* THREAD_LOCAL_HPP_ */

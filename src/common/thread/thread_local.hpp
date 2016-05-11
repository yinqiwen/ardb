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
			T* InitialValue()
			{
				return new T;
			}
		public:
			ThreadLocal()
			{
				pthread_key_create(&m_key, &ThreadLocal::Destroy);
			}

			~ThreadLocal()
			{
				//pthread_key_delete(m_key);
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

			typedef T* InstanceCreator(void* data);

			T& GetValue(InstanceCreator* creator, void* data)
			{
				T* local_thread_value = static_cast<T*>(pthread_getspecific(
				        m_key));
				if (NULL == local_thread_value)
				{
					T* newObj = creator(data);
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

	template<typename T>
	class ThreadLocal<T*>
	{
		private:
			pthread_key_t m_key;
			static void Destroy(void *x)
			{
				T* obj = static_cast<T*>(x);
				delete obj;
			}
			static void Empty(void *x)
			{

			}
			T** InitialValue()
			{
				T** t = new (T*);
				*t = NULL;
				return t;
			}
		public:
			ThreadLocal(bool destroy = true)
			{
				pthread_key_create(&m_key,
				        destroy ? &ThreadLocal::Destroy : &ThreadLocal::Empty);
			}

			~ThreadLocal()
			{
				//pthread_key_delete(m_key);
			}

			T*& GetValue()
			{
				T** local_thread_value = static_cast<T**>(pthread_getspecific(
				        m_key));
				if (NULL == local_thread_value)
				{
					T** newObj = InitialValue();
					pthread_setspecific(m_key, newObj);
					local_thread_value = newObj;
				}
				return *local_thread_value;
			}

			void SetValue(T* v)
			{
				T*& t = GetValue();
				t = v;
			}
	};
}
#endif /* THREAD_LOCAL_HPP_ */

/*
 * Thread.hpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
 */

#ifndef NOVA_THREAD_HPP_
#define NOVA_THREAD_HPP_
#include <vector>
#include <queue>
#include <pthread.h>
#include <unistd.h>
#include "time_helper.hpp"
#include "common.hpp"

using ardb::TimeUnit;
using ardb::MILLIS;
namespace ardb
{
	enum ThreadState
	{
		INITIALIZED, RUNNING, TERMINATED
	};

	class Thread: public Runnable
	{
		private:
			pthread_t m_tid;
			Runnable* m_target;
			ThreadState m_state;
			static void* ThreadFunc(void* data);
		public:
			Thread(Runnable* runner = NULL);
			virtual ~Thread();
			virtual void Run();
			void Start();
			void Stop();
			ThreadState GetState()
			{
				return m_state;
			}
			static void Sleep(int64_t time, TimeUnit unit = MILLIS);
	};
}
#endif /* THREAD_HPP_ */

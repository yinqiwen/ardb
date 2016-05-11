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

#ifndef NOVA_THREAD_HPP_
#define NOVA_THREAD_HPP_
#include <vector>
#include <queue>
#include <pthread.h>
#include <unistd.h>
#include "util/time_helper.hpp"
#include "common.hpp"

using ardb::TimeUnit;
using ardb::MILLIS;
namespace ardb
{
	enum ThreadState
	{
		INITIALIZED, RUNNING, TERMINATED
	};

	struct ThreadOptions
	{
			size_t max_stack_size;
			ThreadOptions() :
					max_stack_size(8192 * 1024)
			{
			}
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
			void Start(const ThreadOptions& options = ThreadOptions());
			void Stop();
			void Join();
			ThreadState GetState()
			{
				return m_state;
			}
			static pthread_t CurrentThreadID();
			static void Sleep(int64_t time, TimeUnit unit = MILLIS);
	};
}
#endif /* THREAD_HPP_ */

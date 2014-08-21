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

#include "thread.hpp"

using namespace ardb;

void* Thread::ThreadFunc(void* data)
{
	Thread* thread = (Thread*) data;
	thread->m_state = RUNNING;
	thread->Run();
	thread->m_state = TERMINATED;
	return NULL;
}

Thread::Thread(Runnable* runner) :
	m_target(runner), m_state(INITIALIZED)
{

}

Thread::~Thread()
{
	if (m_state == RUNNING)
	{
		pthread_join(m_tid, NULL);
	}
}

void Thread::Run()
{
	if (NULL != m_target)
	{
		m_target->Run();
	}
	m_state = TERMINATED;
}

void Thread::Stop()
{
	if (m_state == RUNNING)
	{
		pthread_cancel(m_tid);
	}
	m_state = TERMINATED;
}

void Thread::Join()
{
	pthread_join(m_tid, NULL);
}

void Thread::Start(const ThreadOptions& options)
{
	if (m_state == INITIALIZED)
	{
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		if(options.max_stack_size > 0)
		{
			pthread_attr_setstacksize(&attr, options.max_stack_size);
		}
		if (0 != pthread_create(&m_tid, &attr, ThreadFunc, this))
		{
			m_state = TERMINATED;
		}

	}
}

void Thread::Sleep(int64_t time, TimeUnit unit)
{
	usleep(microstime(time, unit));
}

pthread_t Thread::CurrentThreadID()
{
	return pthread_self();
}

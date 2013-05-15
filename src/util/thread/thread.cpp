/*
 * Thread.cpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
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

void Thread::Start()
{
	if (m_state == INITIALIZED)
	{
		if (0 != pthread_create(&m_tid, NULL, ThreadFunc, this))
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

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

#include "timer.hpp"
#include "util/helpers.hpp"
#include <stdlib.h>
#include <algorithm>

using namespace ardb;

Timer::Timer()
{

}

TimerTask* Timer::GetNearestTimerTask()
{
	return m_task_queue.GetMin();
}

int64 Timer::GetNearestTaskTriggerTime()

{
	TimerTask* task = GetNearestTimerTask();
	if (NULL != task)
	{
		return task->m_nextTriggerTime;
	}
	return -1;
}

uint32 Timer::GenerateTimerTaskID()
{
	static uint32 base_id = 1;
	if (base_id >= 0x7FFFFFFF)
	{
		base_id = 1;
	}
	return base_id++;
}

void Timer::DoTerminated(TimerTask* task, bool eraseFromTable)
{
	if (eraseFromTable
			&& m_task_table.find(task->GetID()) != m_task_table.end())
	{
		m_task_table.erase(task->GetID());
	}
	OnTerminated(task);
	DELETE(task);
}

void Timer::DoFinalSchedule(TimerTask* task)
{
	BeforeScheduled(task);
	OnScheduled(task);
	m_task_queue.Add(task);
	AfterScheduled(task);
}

void Timer::DoSchedule(TimerTask* task, int64 delay, int64 period,
		TimeUnit unit)
{
	task->m_delay = delay;
	task->m_period = period;
	task->m_unit = unit;
	task->m_state = SCHEDULED;
	uint64 now = get_current_epoch_millis();
	task->m_nextTriggerTime = now + millistime(delay, unit);
	DoFinalSchedule(task);
}

int32 Timer::DoSchedule(Runnable* task, int64 delay, int64 period,
		TimeUnit unit, RunnableDestructor* destructor)
{
	uint32 id = GenerateTimerTaskID();
	if (m_task_table.find(id) != m_task_table.end())
	{
		return -1;
	}
	TimerTask* wrapper = NULL;
	NEW(wrapper, TimerTask(id, task, destructor));
	if (NULL == wrapper)
	{
		return -1;
	}
	DoSchedule(wrapper, delay, period, unit);
	m_task_table[wrapper->GetID()] = wrapper;
	return (int32) (wrapper->GetID());
}

int32 Timer::Schedule(Runnable* task, int64 delay, int64 period, TimeUnit unit)
{
	return DoSchedule(task, delay, period, unit, NULL);
}

int32 Timer::ScheduleHeapTask(Runnable* task, int64 delay, int64 period,
		TimeUnit unit)
{
	return DoSchedule(task, delay, period, unit, StandardDestructor<Runnable>);
}

int32 Timer::ScheduleWithDestructor(Runnable* task,
		RunnableDestructor* destructor, int64 delay, int64 period,
		TimeUnit unit)
{
	return DoSchedule(task, delay, period, unit, destructor);
}

bool Timer::Cancel(uint32 taskID)
{
	TimerTaskMap::iterator found = m_task_table.find(taskID);
	if (found != m_task_table.end())
	{
		found->second->Cancel();
		return true;
	}
	return false;
}

uint32 Timer::GetAlivedTaskNumber()
{
	return m_task_queue.GetSize();
}

int64 Timer::GetNextTriggerMillsTime(uint32 taskID)
{
	TimerTaskMap::iterator found = m_task_table.find(taskID);
	if (found != m_task_table.end())
	{
		TimerTask* task = found->second;
		return task->GetNextTriggerTime();
	}
	return -1;
}

bool Timer::AdjustNextTriggerTime(uint32 taskID, int64 value, TimeUnit unit)
{
	TimerTaskMap::iterator found = m_task_table.find(taskID);
	if (found != m_task_table.end())
	{
		TimerTask* task = found->second;
		uint64 temp = llabs(value);
		uint64 adjustvalue = millistime(temp, unit);
		uint64 newTime = task->m_nextTriggerTime;
		if (value > 0)
		{
			newTime += adjustvalue;
		} else
		{
			newTime -= adjustvalue;
		}
		bool ret = m_task_queue.RescheduleTask(task, newTime);
		ASSERT(ret);
		return true;
	}
	return false;
}

int64 Timer::Routine()
{
	uint32 scheduled = 0;
	int64 ret = -1;

	while (!m_task_queue.IsEmpty())
	{
		ret = -1;
		TimerTask* task = m_task_queue.GetMin();
		uint64 now = get_current_epoch_millis();
		if (task->m_nextTriggerTime > now)
		{
			ret = task->m_nextTriggerTime - now;
			break;
		} else
		{
			if (NULL != task->m_runner)
			{
				if (SCHEDULED == task->GetState())
				{
					Runnable* runner = task->m_runner;
					scheduled++;
					if (task->m_period > 0)
					{
						//task->m_delay = task->m_period;
						//DoSchedule(task, task->m_delay, task->m_period,
						//        task->m_unit);
						m_task_queue.RescheduleMin(
								get_current_epoch_millis()
										+ millistime(task->m_period,
												task->m_unit));
						runner->Run();
						continue;
					} else
					{
						m_task_queue.RemoveMin();
						task->m_state = EXECUTED;
						runner->Run();
						DoTerminated(task);
					}
				} else
				{
					m_task_queue.RemoveMin();
					DoTerminated(task);
				}
			} else //runner is NULL
			{
				m_task_queue.RemoveMin();
			}
		}

	}

	return ret;
}

Timer::~Timer()
{
	TimerTaskMap::iterator it = m_task_table.begin();
	while (it != m_task_table.end())
	{
		TimerTask* task = it->second;
		DoTerminated(task, false);
		it++;
	}
}

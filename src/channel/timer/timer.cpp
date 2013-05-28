/*
 * Timer.cpp
 *
 *  Created on: 2011-2-11
 *      Author: qiyingwang
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

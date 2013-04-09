/*
 * ScheduleTimerTask.hpp
 *
 *  Created on: 2011-2-11
 *      Author: qiyingwang
 */

#ifndef NOVA_SCHEDULETIMERTASK_HPP_
#define NOVA_SCHEDULETIMERTASK_HPP_
#include "common.hpp"
#include "util/time_unit.hpp"
using ardb::TimeUnit;
namespace ardb
{
	enum TimerTaskState
	{
		VIRGIN, SCHEDULED, EXECUTED, CANCELLED
	};
	class Timer;
	class TimerTaskQueue;
	class TimerTask
	{
		protected:
			uint32 m_id;
			TimerTaskState m_state;
			int64 m_delay;
			int64 m_period;
			TimeUnit m_unit;
			uint64 m_nextTriggerTime;
			Runnable* m_runner;
			RunnableDestructor* m_runner_destructor;
			uint32 m_queue_index;
			inline uint32 GetID()
			{
				return m_id;
			}
			friend class Timer;
			friend class TimerTaskQueue;
		public:
			TimerTask(uint32 id, Runnable* runner,
					RunnableDestructor* destructor) :
					m_id(id), m_state(VIRGIN), m_delay(0), m_period(0), m_unit(
							ardb::MILLIS), m_nextTriggerTime(0), m_runner(
							runner), m_runner_destructor(destructor), m_queue_index(
							0)
			{
			}
			inline int64 GetDelay()
			{
				return m_delay;
			}
			inline int64 GetPeriod()
			{
				return m_period;
			}
			inline TimerTaskState GetState()
			{
				return m_state;
			}
			inline TimeUnit GetTimeUnit()
			{
				return m_unit;
			}
			uint64 GetNextTriggerTime()
			{
				return m_nextTriggerTime;
			}
			inline void Cancel()
			{
				m_state = CANCELLED;
			}
			~TimerTask()
			{
				if (NULL != m_runner_destructor)
				{
					m_runner_destructor(m_runner);
				}
			}
	};
}

#endif /* SCHEDULETIMERTASK_HPP_ */

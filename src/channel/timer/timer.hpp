/*
 * Timer.hpp
 *
 *  Created on: 2011-1-7
 *      Author: qiyingwang
 */

#ifndef NOVA_TIMER_HPP_
#define NOVA_TIMER_HPP_
#include "common.hpp"
#include "util/time_unit.hpp"
#include "timer_task.hpp"
#include "timer_task_queue.hpp"
#include <map>
#include <tr1/unordered_map>

using ardb::TimeUnit;
namespace ardb
{
	class Timer
	{
		protected:
			typedef std::tr1::unordered_map<uint32, TimerTask*> TimerTaskMap;
			TimerTaskQueue m_task_queue;
			TimerTaskMap m_task_table;
			virtual void BeforeScheduled(TimerTask* task)
			{
			}
			virtual void AfterScheduled(TimerTask* task)
			{
			}
			virtual void OnScheduled(TimerTask* task)
			{
			}
			virtual void OnTerminated(TimerTask* task)
			{
			}
			virtual uint32 GenerateTimerTaskID();
			void DoFinalSchedule(TimerTask* task);
			void DoSchedule(TimerTask* task, int64 delay, int64 period,
					TimeUnit unit);
			void DoTerminated(TimerTask* task, bool eraseFromTable = true);

			TimerTask* GetNearestTimerTask();
			int64 GetNearestTaskTriggerTime();
			int32 DoSchedule(Runnable* task, int64_t delay, int64_t period,
					TimeUnit unit, RunnableDestructor* destructor);
		public:
			Timer();
			int32 Schedule(Runnable* task, int64 delay, int64_t period = -1,
					TimeUnit unit = ardb::MILLIS);
			int32 ScheduleHeapTask(Runnable* task, int64 delay, int64 period =
					-1, TimeUnit unit = ardb::MILLIS);
			int32 ScheduleWithDestructor(Runnable* task,
					RunnableDestructor* destructor, int64 delay, int64 period =
							-1, TimeUnit unit = ardb::MILLIS);
			bool Cancel(uint32 taskID);
			int64 GetNextTriggerMillsTime(uint32 taskID);
			bool AdjustNextTriggerTime(uint32 taskID, int64 value,
					TimeUnit unit = ardb::MILLIS);
			uint32 GetAlivedTaskNumber();
			int64 Routine();
			virtual ~Timer();

	};
}

#endif /* TIMER_HPP_ */

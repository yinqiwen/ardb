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

#ifndef NOVA_TIMER_HPP_
#define NOVA_TIMER_HPP_
#include "common.hpp"
#include "util/time_unit.hpp"
#include "timer_task.hpp"
#include "timer_task_queue.hpp"
#include <map>

using ardb::TimeUnit;
namespace ardb
{
	class Timer
	{
		protected:
			typedef TreeMap<uint32, TimerTask*>::Type TimerTaskMap;
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

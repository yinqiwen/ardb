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

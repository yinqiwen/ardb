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

#ifndef TIMER_TASK_QUEUE_HPP_
#define TIMER_TASK_QUEUE_HPP_
#include "common.hpp"
#include "timer_task.hpp"
namespace ardb
{

	class TimerTaskQueue
	{
		private:
			TimerTask** m_queue;
			uint32 m_queue_capacity;
			uint32 m_size;
			void FixUp(uint32 k);
			void FixDown(uint32 k);
		public:
			TimerTaskQueue();
			inline uint32 GetSize()
			{
				return m_size;
			}
			inline bool IsEmpty()
			{
				return m_size == 0;
			}
			void Add(TimerTask* task);
			TimerTask* Get(uint32 i);
			TimerTask* GetMin();
			void RemoveMin();
			void QuickRemove(uint32 i);
			void Heapify();
			void Clear();
			int32 FindTaskIndex(TimerTask* task);
			void RescheduleMin(uint64 newTime);
			bool RescheduleTask(TimerTask* task, uint64 newTime);
			~TimerTaskQueue();
	};
}

#endif /* TIMER_TASK_QUEUE_HPP_ */

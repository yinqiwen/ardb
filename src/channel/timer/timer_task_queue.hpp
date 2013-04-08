/*
 * timer_task_queue.hpp
 *
 *  Created on: 2011-10-25
 *      Author: qiyingwang
 */

#ifndef TIMER_TASK_QUEUE_HPP_
#define TIMER_TASK_QUEUE_HPP_
#include "common.hpp"
#include "timer_task.hpp"
namespace rddb
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

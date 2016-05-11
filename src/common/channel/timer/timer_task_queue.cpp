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

#include "timer_task_queue.hpp"
#include <string.h>

using namespace ardb;
static const uint32 kDefaultQueueSize = 128;

TimerTaskQueue::TimerTaskQueue() :
    m_queue(NULL), m_queue_capacity(0), m_size(0)
{
	m_queue = new (TimerTask*[kDefaultQueueSize]);
    //NEW(m_queue, TimerTask*[kDefaultQueueSize]);
    memset(m_queue, 0, sizeof(TimerTask*)*kDefaultQueueSize);
    m_queue_capacity = kDefaultQueueSize;
}

void TimerTaskQueue::FixUp(uint32 k)
{
    while (k > 1)
    {
        uint32 j = k >> 1;
        if (m_queue[j]->GetNextTriggerTime()
                <= m_queue[k]->GetNextTriggerTime())
            break;
        TimerTask* tmp = m_queue[j];
        m_queue[j] = m_queue[k];
        m_queue[k] = tmp;
        m_queue[j]->m_queue_index = j;
        m_queue[k]->m_queue_index = k;
        k = j;
    }
}

void TimerTaskQueue::FixDown(uint32 k)
{
    uint32 j;
    while ((j = k << 1) <= m_size && j > 0)
    {
        if (j < m_size && m_queue[j]->GetNextTriggerTime()
                > m_queue[j + 1]->GetNextTriggerTime())
            j++; // j indexes smallest kid
        if (m_queue[k]->GetNextTriggerTime()
                <= m_queue[j]->GetNextTriggerTime())
            break;
        TimerTask* tmp = m_queue[j];
        m_queue[j] = m_queue[k];
        m_queue[k] = tmp;
        m_queue[j]->m_queue_index = j;
        m_queue[k]->m_queue_index = k;
        k = j;
    }
}

void TimerTaskQueue::Add(TimerTask* task)
{
    if (m_size + 1 == m_queue_capacity)
    {
        TimerTask** new_queue = NULL;
        new_queue = new TimerTask*[2*m_queue_capacity];
        //NEW(new_queue, TimerTask*[2*m_queue_capacity]);
        memset(new_queue, 0, sizeof(TimerTask*)*2*m_queue_capacity);
        RETURN_IF_NULL(new_queue);
        memcpy(new_queue, m_queue, m_queue_capacity*sizeof(TimerTask*));
        DELETE_A(m_queue);
        m_queue = new_queue;
        m_queue_capacity = 2 * m_queue_capacity;
    }
    uint32 index = ++m_size;
    m_queue[index] = task;
    task->m_queue_index = index;
    FixUp(m_size);
}

void TimerTaskQueue::Clear()
{
    // Null out task references to prevent memory leak
    for (uint32 i = 1; i <= m_size; i++)
        m_queue[i] = NULL;
    m_size = 0;
}

void TimerTaskQueue::RescheduleMin(uint64 newTime)
{
    m_queue[1]->m_nextTriggerTime = newTime;
    FixDown(1);
}

bool TimerTaskQueue::RescheduleTask(TimerTask* task, uint64 newTime)
{
    int32 index = FindTaskIndex(task);
    if(index > 0)
    {
        bool greater = newTime > task->m_nextTriggerTime;
        task->m_nextTriggerTime = newTime;
        if(greater)
        {
            FixDown(index);
        }
        else
        {
            FixUp(index);
        }
        return true;
    }
    return false;
}

void TimerTaskQueue::RemoveMin()
{
    if(m_size == 0)
    {
        return;
    }
    m_queue[1] = m_queue[m_size];
    m_queue[1]->m_queue_index = 1;
    m_queue[m_size--] = NULL; // Drop extra reference to prevent memory leak
    FixDown(1);
}

int32 TimerTaskQueue::FindTaskIndex(TimerTask* task)
{
    if (m_size == 0)
    {
        return -1;
    }
    if(NULL != task && task->m_queue_index > 0)
    {
        if(m_queue[task->m_queue_index] == task)
        {
            return task->m_queue_index;
        }
    }
    return -1;
}

/**
 * Removes the ith element from queue without regard for maintaining
 * the heap invariant.  Recall that queue is one-based, so
 * 1 <= i <= size.
 */
void TimerTaskQueue::QuickRemove(uint32 i)
{
    m_queue[i] = m_queue[m_size];
    m_queue[i]->m_queue_index = i;
    m_queue[m_size--] = NULL; // Drop extra ref to prevent memory leak
}

TimerTask* TimerTaskQueue::GetMin()
{
    return m_queue[1];
}

/**
 * Return the ith task in the priority queue, where i ranges from 1 (the
 * head task, which is returned by getMin) to the number of tasks on the
 * queue, inclusive.
 */
TimerTask* TimerTaskQueue::Get(uint32 i)
{
    return m_queue[i];
}

void TimerTaskQueue::Heapify()
{
    for (uint32 i = m_size / 2; i >= 1; i--)
        FixDown(i);
}

TimerTaskQueue::~TimerTaskQueue()
{
    Clear();
    DELETE_A(m_queue);
}


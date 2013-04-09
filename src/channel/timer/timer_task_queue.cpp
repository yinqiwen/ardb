/*
 * timer_task_queue.cpp
 *
 *  Created on: 2011-10-25
 *      Author: qiyingwang
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


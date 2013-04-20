/*
 * SocketChannel.cpp
 *
 *  Created on: 2011-1-12
 *      Author: qiyingwang
 */
#include "channel/all_includes.hpp"
#include "util/time_helper.hpp"

using namespace ardb;

int TimerChannel::TimeoutCB(struct aeEventLoop *eventLoop, long long id,
        void *clientData)
{
    TimerChannel* channel = (TimerChannel*) clientData;
    int64 nextTime = channel->Routine();
    //int64 nextTime = channel->GetNearestTaskTriggerTime();
    if (nextTime > 0)
    {
        //uint64 now = getCurrentTimeMillis();
        //return nextTime - now;
        return nextTime;
    }
    //ASSERT(nextTime > 0);
    channel->m_timer_id = -1;
    return AE_NOMORE;
}

void TimerChannel::OnScheduled(TimerTask* task)
{
    //TimerTask* nearestTask = getNearestTimerTask();
    int64 nextTime = GetNearestTaskTriggerTime();
    if (nextTime < 0
            || static_cast<uint64>(nextTime) > task->GetNextTriggerTime())
    {
        if (-1 != m_timer_id)
        {
            if (nextTime > 0)
            {
                aeModifyTimeEvent(m_service.GetRawEventLoop(), m_timer_id,
                        task->GetDelay());
            }
            else
            {
                //just ignore, let timer handle it later
            }
        }
        else
        {
            if (-1 != m_timer_id)
            {
                aeDeleteTimeEvent(m_service.GetRawEventLoop(), m_timer_id);
                m_timer_id = -1;
            }
            m_timer_id = aeCreateTimeEvent(m_service.GetRawEventLoop(),
                    task->GetDelay(), TimeoutCB, this, NULL);
        }
    }
}

TimerChannel::~TimerChannel()
{
    if (-1 != m_timer_id)
    {
        aeDeleteTimeEvent(m_service.GetRawEventLoop(), m_timer_id);
    }
}

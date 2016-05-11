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
                aeModifyTimeEvent(GetService().GetRawEventLoop(), m_timer_id,
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
                aeDeleteTimeEvent(GetService().GetRawEventLoop(), m_timer_id);
                m_timer_id = -1;
            }
            m_timer_id = aeCreateTimeEvent(GetService().GetRawEventLoop(),
                    task->GetDelay(), TimeoutCB, this, NULL);
        }
    }
}

TimerChannel::~TimerChannel()
{
    if (-1 != m_timer_id)
    {
        aeDeleteTimeEvent(GetService().GetRawEventLoop(), m_timer_id);
    }
}

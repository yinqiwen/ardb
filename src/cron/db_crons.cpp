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
#include "db_crons.hpp"

namespace ardb
{
    DBCrons* DBCrons::g_crons = NULL;
    DBCrons::DBCrons() :
            m_db_server(NULL), m_expire_check(NULL), m_gc(NULL)
    {
    }

    DBCrons& DBCrons::GetSingleton()
    {
        if (NULL == g_crons)
        {
            g_crons = new DBCrons;
        }
        return *g_crons;
    }

    void DBCrons::DestroySingleton()
    {
        delete g_crons;
    }

    int DBCrons::Init(ArdbServer* server)
    {
        m_db_server = server;
        if (NULL != server)
        {
            NEW(m_expire_check, ExpireCheck(server));
            NEW(m_gc, CompactGC(server));
        }
        return 0;
    }

    void DBCrons::Run()
    {
        if (NULL != m_expire_check)
        {
            m_serv.GetTimer().ScheduleHeapTask(m_expire_check, 100, 100, MILLIS);
        }
        if (NULL != m_gc)
        {
            m_serv.GetTimer().ScheduleHeapTask(m_gc, 10, 10, SECONDS);
        }
        m_serv.Start();
    }

    CompactGC& DBCrons::GetCompactGC()
    {
        static CompactGC empty(NULL);
        if(NULL != m_gc)
        {
            return *m_gc;
        }
        return empty;
    }

    void DBCrons::StopSelf()
    {
        m_serv.Stop();
        m_serv.Wakeup();
    }
}


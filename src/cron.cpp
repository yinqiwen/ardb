/*
 *Copyright (c) 2013-2015, yinqiwen <yinqiwen@gmail.com>
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
#include "network.hpp"
#include "statistics.hpp"
#include "db/db.hpp"

OP_NAMESPACE_BEGIN

    struct ServerCronTask: public Runnable
    {
            void Run()
            {
                static time_t lastDumpTime = time(NULL);
                time_t now = time(NULL);
                Statistics::GetSingleton().TrackQPSPerSecond();

                if (now - lastDumpTime >= g_db->GetConf().statistics_log_period)
                {
                    if(g_db->GetConf().statistics_log_period % 60 == 0)
                    {
                        if(get_current_minute_secs(now) != 0)
                        {
                            return;
                        }
                        int64 factor = g_db->GetConf().statistics_log_period / 60;
                        if(get_current_minute(now) % factor != 0)
                        {
                            return;
                        }
                    }
                    lastDumpTime = now;
                    INFO_LOG("========================Period Statistics Dump Begin===========================");
                    Statistics::GetSingleton().DumpLog(STAT_DUMP_PERIOD);
                    INFO_LOG("========================Period Statistics Dump End===========================");
                }
            }
    };
    class ServerCronThread: public Thread
    {
            ChannelService serv;
            void Run()
            {
                serv.GetTimer().ScheduleHeapTask(new ServerCronTask, 1, 1, SECONDS);
                serv.Start();
            }
    };

    void Server::StartCrons()
    {
        if (NULL == m_cron_thread)
        {
            NEW(m_cron_thread, ServerCronThread);
            m_cron_thread->Start();
        }
    }
OP_NAMESPACE_END


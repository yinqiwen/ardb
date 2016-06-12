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
#include "repl/snapshot.hpp"

OP_NAMESPACE_BEGIN

    static void period_dump_statistics()
    {
        static time_t nextDumpTime = 0;
        time_t now = time(NULL);
        /*
         * Period dump statistics into log
         */
        if (0 == nextDumpTime)
        {
            if (g_db->GetConf().statistics_log_period % 60 == 0)
            {
                if (get_current_minute_secs(now) != 0)
                {
                    return;
                }
                int64 factor = g_db->GetConf().statistics_log_period / 60;
                if (get_current_minute(now) % factor != 0)
                {
                    return;
                }
            }
            nextDumpTime = now;
        }

        if (now >= nextDumpTime)
        {
            nextDumpTime += g_db->GetConf().statistics_log_period;
            INFO_LOG("========================Period Statistics Dump Begin===========================");
            Statistics::GetSingleton().DumpLog(STAT_DUMP_PERIOD);
            INFO_LOG("========================Period Statistics Dump End===========================");
        }
    }

    struct FastCronTask: public Runnable
    {
            void Run()
            {
                Statistics::GetSingleton().TrackQPSPerSecond();
                period_dump_statistics();

                /*
                 * just let storage engine routine every 1s to do sth.
                 */
                g_engine->Routine();
            }
    };

    struct CronThread: public Thread
    {
            ChannelService serv;
            virtual ~CronThread()
            {
            }
    };

    struct FastCronThread: public CronThread
    {
            void Run()
            {
                serv.GetTimer().ScheduleHeapTask(new FastCronTask, 1, 1, SECONDS);
                serv.Start();
            }
    };

    struct SlowCronTask: public Runnable
    {
            void Run()
            {
                g_db->ScanExpiredKeys();
                g_snapshot_manager->Routine();
            }
    };

    /*
     * slow cron task which would do DB operations block current thread
     */
    struct SlowCronThread: public CronThread
    {
            void Run()
            {
                serv.GetTimer().ScheduleHeapTask(new SlowCronTask, 1, 1, SECONDS);
                serv.Start();
            }
    };

    void Server::StartCrons()
    {
        if (m_cron_threads.empty())
        {
            Thread* cron = NULL;
            NEW(cron, FastCronThread);
            cron->Start();
            m_cron_threads.push_back(cron);
            NEW(cron, SlowCronThread);
            cron->Start();
            m_cron_threads.push_back(cron);
        }
    }

    void Server::StopCrons()
    {
        if (!m_cron_threads.empty())
        {
            for(size_t i = 0; i < m_cron_threads.size(); i++)
            {
                ((CronThread*) m_cron_threads[i])->serv.Stop();
                m_cron_threads[i]->Join();
                DELETE(m_cron_threads[i]);
            }
            m_cron_threads.clear();
        }
    }
OP_NAMESPACE_END


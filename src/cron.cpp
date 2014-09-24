/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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
#include "cron.hpp"
#include "ardb.hpp"

OP_NAMESPACE_BEGIN

    struct ExpireCheck: public Runnable
    {
            DBIDSet checkdbs;
            void Run()
            {
                if (!g_db->GetConfig().master_host.empty())
                {
                    /*
                     * no expire on slave, master would sync the expire/del operations to slaves.
                     */
                    return;
                }
                if (checkdbs.empty())
                {
                    g_db->GetAllDBIDSet(checkdbs);
                    if (checkdbs.empty())
                    {
                        return;
                    }
                }
                DBID db = *(checkdbs.begin());
                checkdbs.erase(db);

                KeyObject start;
                start.db = db;
                start.type = KEY_EXPIRATION_ELEMENT;
                start.score.SetInt64(0);
                Iterator* iter = g_db->IteratorKeyValue(start, false);
                while (NULL != iter && iter->Valid())
                {
                    KeyObject k;
                    decode_key(iter->Key(), k);
                    if (k.db != db || k.type != KEY_EXPIRATION_ELEMENT)
                    {
                        break;
                    }
                    if (k.score.value.iv <= get_current_epoch_millis())
                    {
                        Context tmpctx;
                        tmpctx.currentDB = db;
                        g_db->DeleteKey(tmpctx, k.key);
                        iter->Next();

                        RedisCommandFrame del("del");
                        std::string keystr;
                        keystr.assign(k.key.data(), k.key.size());
                        del.AddArg(keystr);
                        g_db->m_master.FeedSlaves(db, del);
                    }
                    else
                    {
                        break;
                    }
                }
                DELETE(iter);
            }
    };

    struct CompactTask: public Runnable
    {

            void Run()
            {
                static time_t last_compact_time = time(NULL);
                if (!g_db->GetConfig().compact_enable)
                {
                    return;
                }
                time_t now = time(NULL);
                bool should_compact = false;
                if (now - last_compact_time < g_db->GetConfig().compact_min_interval)
                {
                    return;
                }
                if (now - last_compact_time >= g_db->GetConfig().compact_max_interval)
                {
                    should_compact = true;
                }
                if (!should_compact)
                {
                    if (g_db->GetStatistics().GetLatencyStat().write_count_since_last_compact
                            >= g_db->GetConfig().compact_trigger_write_count)
                    {
                        should_compact = true;
                    }
                }
                if (should_compact)
                {
                    INFO_LOG("Start compact DB.");
                    uint64 start = get_current_epoch_millis();
                    g_db->DoCompact("", "");
                    uint64 end = get_current_epoch_millis();
                    g_db->GetStatistics().GetLatencyStat().write_count_since_last_compact = 0;
                    uint64 duration = end - start;
                    INFO_LOG("Cost %llums to compact DB.", duration);
                    last_compact_time = time(NULL);
                }
            }
    };

    static void ChannelCloseCallback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->Close();
        }
    }

    struct ConnectionTimeout: public Runnable
    {
            void Run()
            {
                static uint32 last_check_id = 0;
                if (g_db->GetConfig().timeout == 0)
                {
                    return;
                }
                uint32 max_check = 100;
                uint32 cursor = 0;
                LockGuard<SpinMutexLock> guard(g_db->m_clients_lock);
                ContextTable::iterator it = g_db->m_clients.lower_bound(last_check_id);
                while (it != g_db->m_clients.end())
                {
                    if (!it->second->processing
                            && (get_current_epoch_micros() - it->second->last_interaction_ustime)
                                    > g_db->GetConfig().timeout * 1000000)
                    {
                        it->second->client->GetService().AsyncIO(it->first, ChannelCloseCallback, NULL);
                    }
                    last_check_id = it->first;
                    cursor++;
                    if (cursor >= max_check)
                    {
                        break;
                    }
                    it++;
                }
            }
    };

    struct TrackOpsTask: public Runnable
    {
            void Run()
            {
                g_db->GetStatistics().TrackOperationsPerSecond();
            }
    };

    struct LatencyStatClearTask: public Runnable
    {
            void Run()
            {
                g_db->GetStatistics().GetLatencyStat().Clear();
            }
    };

    struct RedisCursorClearTask: public Runnable
    {
            void Run()
            {
                g_db->ClearExpireRedisCursor();
            }
    };

    CronManager::CronManager()
    {

    }
    void CronManager::Start()
    {
        m_db_cron.serv.GetTimer().ScheduleHeapTask(new ExpireCheck, 100, 100, MILLIS);
        m_db_cron.serv.GetTimer().ScheduleHeapTask(new CompactTask, 10, 10, SECONDS);

        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new ConnectionTimeout, 100, 100, MILLIS);
        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new TrackOpsTask, 1, 1, SECONDS);
        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new LatencyStatClearTask, 5, 5, MINUTES);
        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new RedisCursorClearTask, 1, 1, SECONDS);

        m_db_cron.Start();
        m_misc_cron.Start();
    }

    void CronManager::StopSelf()
    {
        m_db_cron.StopSelf();
        m_misc_cron.StopSelf();
    }

OP_NAMESPACE_END


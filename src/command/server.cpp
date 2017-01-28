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

#include "db/db.hpp"
#include "repl/repl.hpp"
#include "util/socket_address.hpp"
#include "util/lru.hpp"
#include "util/system_helper.hpp"
#include "statistics.hpp"
#include <sstream>
#include <sys/utsname.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include "repl/snapshot.hpp"

namespace ardb
{
    static int RDBSaveLoadRoutine(SnapshotState state, Snapshot* snapshot, void* cb)
    {
        ChannelService* serv = (ChannelService*) cb;
        if (NULL != serv)
        {
            serv->Continue();
        }
        return 0;
    }

    int Ardb::Save(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        SnapshotType type = REDIS_DUMP;
        if (cmd.GetArguments().size() == 1)
        {
            type = Snapshot::GetSnapshotTypeByName(cmd.GetArguments()[0]);
            if (type == -1)
            {
                reply.SetErrCode(ERR_INVALID_ARGS);
                return 0;
            }
        }
        ChannelService* io_serv = NULL;
        uint32 conn_id = 0;
        if (NULL != ctx.client && NULL != ctx.client->client)
        {
            io_serv = &(ctx.client->client->GetService());
            conn_id = ctx.client->client->GetID();
            ctx.client->client->BlockRead();
        }
        Snapshot* snapshot = g_snapshot_manager->NewSnapshot(type, false, RDBSaveLoadRoutine, io_serv);
        if (NULL == io_serv || io_serv->GetChannel(conn_id) != NULL)
        {
            if (NULL != snapshot)
            {
                reply.SetStatusCode(STATUS_OK);
            }
            else
            {
                reply.SetErrorReason("Save snapshot failed.");
            }
        }
        if (NULL != ctx.client && NULL != ctx.client->client)
        {
            ctx.client->client->UnblockRead();
        }
        return 0;
    }

    int Ardb::LastSave(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(g_snapshot_manager->LastSave());
        return 0;
    }

    int Ardb::BGSave(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        SnapshotType type = REDIS_DUMP;
        if (cmd.GetArguments().size() == 1)
        {
            type = Snapshot::GetSnapshotTypeByName(cmd.GetArguments()[0]);
            if (type == -1)
            {
                reply.SetErrCode(ERR_INVALID_ARGS);
                return 0;
            }
        }
        Snapshot* snapshot = g_snapshot_manager->NewSnapshot(type, true, NULL, NULL);
        if (NULL != snapshot)
        {
            reply.SetStatusCode(STATUS_OK);
        }
        else
        {
            reply.SetErrorReason("BGSave snapshot failed.");
        }
        return 0;
    }

    int Ardb::Import(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (IsLoadingData())
        {
            reply.SetErrCode(ERR_LOADING);
            return 0;
        }
        const std::string& file = cmd.GetArguments()[0];
        m_loading_data = true;
        ChannelService* io_serv = NULL;
        uint32 conn_id = 0;
        if (NULL != ctx.client && NULL != ctx.client->client)
        {
            io_serv = &(ctx.client->client->GetService());
            conn_id = ctx.client->client->GetID();
            ctx.client->client->BlockRead();
        }
        /*
         * wait until only 'import' be processing
         */
        while (m_db_caller_num != 1)
        {
            usleep(10);
        }
        Snapshot snapshot;
        /*
         * use 4 threads to write db
         */
        DBWriter load_writer;
        snapshot.SetDBWriter(&load_writer);
        int err = snapshot.Load(file, RDBSaveLoadRoutine, io_serv);
        snapshot.SetDBWriter(NULL);
        load_writer.Stop();
        if (NULL == io_serv || io_serv->GetChannel(conn_id) != NULL)
        {
            if (err == 0)
            {
                reply.SetStatusCode(STATUS_OK);
            }
            else
            {
                reply.SetErrorReason("Import snapshot failed.");
            }
        }
        m_loading_data = false;
        if (NULL != ctx.client && NULL != ctx.client->client)
        {
            ctx.client->client->UnblockRead();
        }
        return 0;
    }

    void Ardb::FillInfoResponse(Context& ctx, const std::string& section, std::string& info)
    {
        const char* all = "all";
        const char* def = "default";
        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "server"))
        {
            struct utsname name;
            uname(&name);
            info.append("# Server\r\n");
            info.append("ardb_version:").append(ARDB_VERSION).append("\r\n");
            info.append("redis_version:").append(GetConf().redis_compatible_version).append("\r\n");
            info.append("engine:").append(g_engine_name).append("\r\n");
            info.append("ardb_home:").append(GetConf().home).append("\r\n");
            info.append("os:").append(name.sysname).append(" ").append(name.release).append(" ").append(name.machine).append("\r\n");
            char tmp[256];
            sprintf(tmp, "%d.%d.%d",
#ifdef __GNUC__
                    __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__
#else
                    0,0,0
#endif
                    );
            info.append("gcc_version:").append(tmp).append("\r\n");
            info.append("process_id:").append(stringfromll(getpid())).append("\r\n");

            if (!g_repl->GetReplLog().GetReplKey().empty())
            {
                info.append("run_id:").append(g_repl->GetReplLog().GetReplKey()).append("\r\n");
            }
            info.append("tcp_port:").append(stringfromll(GetConf().PrimaryPort())).append("\r\n");
            for (size_t i = 0; i < GetConf().servers.size(); i++)
            {
                info.append("listen:").append(GetConf().servers[i].Address()).append("\r\n");
            }
            time_t uptime = time(NULL) - m_starttime;
            info.append("uptime_in_seconds:").append(stringfromll(uptime)).append("\r\n");
            info.append("uptime_in_days:").append(stringfromll(uptime / (3600 * 24))).append("\r\n");
            info.append("executable:").append(GetConf()._executable).append("\r\n");
            info.append("config_file:").append(GetConf()._conf_file).append("\r\n");
            info.append("\r\n");
        }
        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "databases"))
        {
            info.append("# Databases\r\n");
            info.append("data_dir:").append(GetConf().data_base_path).append("\r\n");
            int64 filesize = file_size(GetConf().data_base_path);
            char tmp[256];
            sprintf(tmp, "%" PRId64, filesize);
            info.append("used_disk_space:").append(tmp).append("\r\n");
            std::string stats;
            m_engine->Stats(ctx, stats);
            info.append(stats).append("\r\n");
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "clients"))
        {
            info.append("# Clients\r\n");
            {
                LockGuard<SpinMutexLock> guard(m_clients_lock);
                info.append("connected_clients:").append(stringfromll(m_all_clients.size())).append("\r\n");
            }
            {
                LockGuard<SpinMutexLock> guard(m_block_keys_lock);
                info.append("blocked_clients:").append(stringfromll(m_blocked_ctxs.size())).append("\r\n");
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "persistence"))
        {
            info.append("# Persistence\r\n");
            info.append("loading:").append(IsLoadingData() ? " 1" : "0").append("\r\n");
            info.append("rdb_last_save_time:").append(stringfromll(g_snapshot_manager->LastSave())).append("\r\n");
            info.append("rdb_last_bgsave_status:").append(g_snapshot_manager->LastSaveErr() != 0 ? "error" : "ok").append("\r\n");
            info.append("rdb_last_bgsave_time_sec:").append(stringfromll(g_snapshot_manager->LastSaveCost())).append("\r\n");
            info.append("rdb_bgsave_in_progress:").append(g_snapshot_manager->CurrentSaverNum() > 0 ? "1" : "0").append("\r\n");
            time_t lastsave_elapsed = 0;
            if (g_snapshot_manager->LastSaveStartUnixTime() > 0)
            {
                lastsave_elapsed = time(NULL) - g_snapshot_manager->LastSaveStartUnixTime();
            }
            info.append("rdb_current_bgsave_time_sec:").append(stringfromll(lastsave_elapsed)).append("\r\n");
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "cpu"))
        {
            struct rusage self_ru;
            getrusage(RUSAGE_SELF, &self_ru);
            info.append("# CPU\r\n");
            char tmp[100];
            sprintf(tmp, "%.2f", (float) self_ru.ru_stime.tv_sec + (float) self_ru.ru_stime.tv_usec / 1000000);
            info.append("used_cpu_sys:").append(tmp).append("\r\n");
            sprintf(tmp, "%.2f", (float) self_ru.ru_utime.tv_sec + (float) self_ru.ru_utime.tv_usec / 1000000);
            info.append("used_cpu_user:").append(tmp).append("\r\n");
            info.append("\r\n");

        }
        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "replication"))
        {
            info.append("# Replication\r\n");
            if (GetConf().repl_backlog_size > 0)
            {
                if (GetConf().master_host.empty())
                {
                    info.append("role:master\r\n");
                }
                else
                {
                    info.append("role:slave\r\n");
                }
                info.append("repl_dir: ").append(GetConf().repl_data_dir).append("\r\n");
            }
            else
            {
                info.append("role: singleton\r\n");
            }
//
            if (g_repl->IsInited())
            {
                if (!GetConf().master_host.empty())
                {
                    info.append("master_host:").append(GetConf().master_host).append("\r\n");
                    info.append("master_port:").append(stringfromll(GetConf().master_port)).append("\r\n");
                    info.append("master_link_status:").append(g_repl->GetSlave().IsConnected() ? "up" : "down").append("\r\n");
                    if (g_repl->GetSlave().IsConnected())
                    {
                        info.append("master_last_io_seconds_ago:").append(stringfromll(time(NULL) - g_repl->GetSlave().GetMasterLastinteractionTime())).append(
                                "\r\n");
                    }
                    else
                    {
                        info.append("master_last_io_seconds_ago:-1").append("\r\n");
                    }
                    info.append("master_sync_in_progress:").append(g_repl->GetSlave().IsSyncing() ? "1" : "0").append("\r\n");
                    info.append("slave_load_in_progress:").append(g_repl->GetSlave().IsLoading() ? "1" : "0").append("\r\n");
                    if (g_repl->GetSlave().IsSyncing())
                    {
                        info.append("master_sync_left_bytes:").append(stringfromll(g_repl->GetSlave().SyncLeftBytes())).append("\r\n");
                        info.append("master_sync_last_io_seconds_ago:").append(stringfromll(time(NULL) - g_repl->GetSlave().GetMasterLastinteractionTime())).append(
                                "\r\n");
                    }
                    if (g_repl->GetSlave().IsLoading())
                    {
                        info.append("slave_loading_left_bytes:").append(stringfromll(g_repl->GetSlave().LoadLeftBytes())).append("\r\n");
                    }
                    info.append("slave_repl_offset:").append(stringfromll(g_repl->GetSlave().SyncOffset())).append("\r\n");
                    info.append("slave_sync_queue_size:").append(stringfromll(g_repl->GetSlave().SyncQueueSize())).append("\r\n");
                    if (!g_repl->GetSlave().IsConnected())
                    {
                        info.append("master_link_down_since_seconds:").append(stringfromll(time(NULL) - g_repl->GetSlave().GetMasterLinkDownTime())).append(
                                "\r\n");
                    }
                    info.append("slave_priority:").append(stringfromll(GetConf().slave_priority)).append("\r\n");
                    info.append("slave_read_only:").append(GetConf().slave_readonly ? "1" : "0").append("\r\n");
                }
                info.append("repl_current_namespace:").append(g_repl->GetReplLog().CurrentNamespace()).append("\r\n");
                info.append("connected_slaves: ").append(stringfromll(g_repl->GetMaster().ConnectedSlaves())).append("\r\n");
                /* If min-slaves-to-write is active, write the number of slaves
                 * currently considered 'good'. */
                if (GetConf().repl_min_slaves_to_write && GetConf().repl_min_slaves_max_lag)
                {
                    info.append("min_slaves_good_slaves:").append(stringfromll(g_repl->GetMaster().GoodSlavesCount())).append("\r\n");
                }
                g_repl->GetMaster().PrintSlaves(info);

                info.append("master_repl_offset: ").append(stringfromll(g_repl->GetReplLog().WALEndOffset())).append("\r\n");
                info.append("repl_backlog_size: ").append(stringfromll(GetConf().repl_backlog_size)).append("\r\n");
                info.append("repl_backlog_cache_size: ").append(stringfromll(GetConf().repl_backlog_cache_size)).append("\r\n");
                info.append("repl_backlog_first_byte_offset: ").append(stringfromll(g_repl->GetReplLog().WALStartOffset())).append("\r\n");
                info.append("repl_backlog_histlen: ").append(stringfromll(g_repl->GetReplLog().WALEndOffset() - g_repl->GetReplLog().WALStartOffset() + 1)).append(
                        "\r\n");
                info.append("repl_backlog_cksm: ").append(base16_stringfromllu(g_repl->GetReplLog().WALCksm())).append("\r\n");
                g_snapshot_manager->PrintSnapshotInfo(info);
            }
            else
            {
                info.append("repl_backlog_size: 0").append("\r\n");
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "memory"))
        {
            info.append("# Memory\r\n");
            std::string tmp;
            info.append("used_memory_rss:").append(stringfromll(mem_rss_size())).append("\r\n");
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "stats"))
        {
            info.append("# Stats\r\n");
            std::string tmp;
            Statistics::GetSingleton().DumpString(tmp, STAT_DUMP_INFO_CMD, STAT_TYPE_ALL & (~STAT_TYPE_COST));
            info.append(tmp);
            info.append("sync_full:").append(stringfromll(g_repl->GetMaster().FullSyncCount())).append("\r\n");
            info.append("sync_partial_ok:").append(stringfromll(g_repl->GetMaster().ParitialSyncOKCount())).append("\r\n");
            info.append("sync_partial_err:").append(stringfromll(g_repl->GetMaster().ParitialSyncErrCount())).append("\r\n");
            {
                ReadLockGuard<SpinRWLock> guard(m_pubsub_lock);
                info.append("pubsub_channels:").append(stringfromll(m_pubsub_channels.size())).append("\r\n");
                info.append("pubsub_patterns:").append(stringfromll(m_pubsub_patterns.size())).append("\r\n");
            }
            {
                LockGuard<SpinMutexLock> guard(m_expires_lock);
                info.append("expire_scan_keys:").append(stringfromll(m_expires.size())).append("\r\n");
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), def) || !strcasecmp(section.c_str(), "keyspace"))
        {
            DataArray nss;
            m_engine->ListNameSpaces(ctx, nss);
            if (!nss.empty())
            {
                info.append("# Keyspace\r\n");
                for (size_t i = 0; i < nss.size(); i++)
                {
                    if (nss[i].AsString() == TTL_DB_NSMAESPACE)
                    {
                        continue;
                    }
                    info.append("db").append(nss[i].AsString()).append(":").append("keys=").append(stringfromll(m_engine->EstimateKeysNum(ctx, nss[i]))).append(
                            "\r\n");
                }
                info.append("\r\n");
            }
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), "commandstats"))
        {
            info.append("# Commandstats\r\n");
            RedisCommandHandlerSettingTable::iterator cit = m_settings.begin();
            while (cit != m_settings.end())
            {
                RedisCommandHandlerSetting& setting = cit->second;
                if (setting.calls > 0)
                {
                    info.append("cmdstat_").append(setting.name).append(":").append("calls=").append(stringfromll(setting.calls)).append(",usec=").append(
                            stringfromll(setting.microseconds)).append(",usecpercall=").append(stringfromll(setting.microseconds / setting.calls)).append(
                            "\r\n");
                }

                cit++;
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), all) || !strcasecmp(section.c_str(), "coststats"))
        {
            info.append("# Coststats\r\n");
            std::string tmp;
            Statistics::GetSingleton().DumpString(tmp, STAT_DUMP_INFO_CMD, STAT_TYPE_COST);
            info.append(tmp);
            info.append("\r\n");
        }
    }

    int Ardb::Info(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string info;
        std::string section = "default";
        if (cmd.GetArguments().size() == 1)
        {
            section = cmd.GetArguments()[0];
        }
        FillInfoResponse(ctx, section, info);
        ctx.GetReply().SetString(info);
        return 0;
    }

    int Ardb::DBSize(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(m_engine->EstimateKeysNum(ctx, ctx.ns));
        return 0;
    }

    static void channel_close_callback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->Close();
        }
    }
    static void channel_pause_callback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->DetachFD();
        }
    }

    int Ardb::Client(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (NULL == ctx.client)
        {
            reply.SetErrCode(ERR_AUTH_FAILED);
            return 0;
        }
        std::string subcmd = string_tolower(cmd.GetArguments()[0]);
        if (subcmd == "setname")
        {
            if (cmd.GetArguments().size() != 2)
            {
                reply.SetErrorReason("Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            ctx.client->name = cmd.GetArguments()[1];
            reply.SetStatusCode(STATUS_OK);
        }
        else if (subcmd == "getname")
        {
            if (cmd.GetArguments().size() != 1)
            {
                reply.SetErrorReason("Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            if (ctx.client->name.empty())
            {
                reply.Clear();
            }
            else
            {
                reply.SetString(ctx.client->name);
            }
        }
        else if (subcmd == "reply")
        {
            if (cmd.GetArguments().size() != 2)
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
            reply.SetStatusCode(STATUS_OK);
            if (!strcasecmp(cmd.GetArguments()[1].c_str(), "on"))
            {
                ctx.flags.reply_off = 0;
            }
            else if (!strcasecmp(cmd.GetArguments()[1].c_str(), "off"))
            {
                ctx.flags.reply_off = 1;
            }
            else if (!strcasecmp(cmd.GetArguments()[1].c_str(), "skip"))
            {
                if(!ctx.flags.reply_off)
                {
                    ctx.flags.reply_skip = 1;
                }
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }
        else if (subcmd == "pause")
        {
            //pause all clients
            if (cmd.GetArguments().size() != 2)
            {
                reply.SetErrorReason("Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            uint32 timeout;
            if (!string_touint32(cmd.GetArguments()[1], timeout) || timeout == 0)
            {
                reply.SetErrorReason("timeout is not an integer or out of range");
                return 0;
            }
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            ContextSet::iterator it = m_all_clients.begin();
            while (it != m_all_clients.end())
            {
                Context* client = *it;
                if (NULL != client->client && NULL != client->client->client)
                {
                    SocketChannel* conn = (SocketChannel*) (client->client->client);
                    conn->GetService().AsyncIO(conn->GetID(), channel_pause_callback, NULL);
                    client->client->resume_ustime = get_current_epoch_micros() + timeout * 1000;
                }
                it++;
            }
            reply.SetStatusCode(STATUS_OK);
        }
        else if (subcmd == "kill")
        {
            if (cmd.GetArguments().size() < 2)
            {
                reply.SetErrorReason("Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }

            std::string kill_addr;
            std::string kill_pattern;
            bool kill_slaves = false;
            bool kill_master = false;
            bool kill_pubsub = false;
            bool kill_normal = false;
            bool skipme = true;
            if (cmd.GetArguments().size() == 2)
            {
                kill_addr = cmd.GetArguments()[1];
            }
            else
            {
                if ((cmd.GetArguments().size() - 1) % 2 != 0)
                {
                    reply.SetErrCode(ERR_INVALID_SYNTAX);
                    return 0;
                }
                for (size_t i = 1; i < cmd.GetArguments().size(); i += 2)
                {
                    if (!strcasecmp(cmd.GetArguments()[i].c_str(), "addr"))
                    {
                        kill_addr = cmd.GetArguments()[i + 1];
                    }
                    else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "type"))
                    {
                        const std::string& typestr = cmd.GetArguments()[i + 1];
                        if (!strcasecmp(typestr.c_str(), "slave"))
                        {
                            kill_slaves = true;
                        }
                        else if (!strcasecmp(typestr.c_str(), "master"))
                        {
                            kill_master = true;
                        }
                        else if (!strcasecmp(typestr.c_str(), "pubsub"))
                        {
                            kill_pubsub = true;
                        }
                        else if (!strcasecmp(typestr.c_str(), "normal"))
                        {
                            kill_normal = true;
                        }
                        else
                        {
                            reply.SetErrorReason("Unknown client type '" + typestr + "'");
                            return 0;
                        }
                    }
                    else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "SKIPME"))
                    {
                        if (!strcasecmp(cmd.GetArguments()[i + 1].c_str(), "yes"))
                        {
                            skipme = true;
                        }
                        else if (!strcasecmp(cmd.GetArguments()[i + 1].c_str(), "no"))
                        {
                            skipme = false;
                        }
                        else
                        {
                            reply.SetErrCode(ERR_INVALID_SYNTAX);
                            return 0;
                        }
                    }
                    else
                    {
                        reply.SetErrCode(ERR_INVALID_SYNTAX);
                        return 0;
                    }
                }
            }
            if (!kill_addr.empty())
            {
                if (is_pattern_string(kill_addr))
                {
                    kill_pattern = kill_addr;
                    kill_addr.clear();
                }
            }
            if (kill_slaves)
            {
                g_repl->GetMaster().DisconnectAllSlaves();
            }
            if (kill_master)
            {
                g_repl->GetSlave().Close();
            }
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            ContextSet::iterator it = m_all_clients.begin();
            while (it != m_all_clients.end())
            {
                Context* client = *it;
                SocketChannel* conn = (SocketChannel*) (client->client->client);
                bool to_kill = false;

                if (kill_normal)
                {
                    to_kill = (!client->flags.lua) && (!client->flags.pubsub);
                }
                if (!to_kill && kill_pubsub)
                {
                    to_kill = client->flags.pubsub;
                }

                if (!to_kill)
                {
                    std::string conn_addr = conn->GetRemoteStringAddress();
                    if (conn_addr == kill_addr)
                    {
                        to_kill = true;
                    }
                    else if (!kill_pattern.empty())
                    {
                        to_kill = stringmatchlen(kill_pattern.c_str(), kill_pattern.size(), conn_addr.c_str(), conn_addr.size(), 0);
                    }
                }
                if (to_kill)
                {
                    /*
                     * do not kill current connection if 'skipme = true'
                     */
                    if (!skipme || (NULL != ctx.client && ctx.client->client != conn))
                    {
                        conn->GetService().AsyncIO(conn->GetID(), channel_close_callback, NULL);
                    }
                    /*
                     * only one connection has same address with kill args
                     */
                    if (!kill_addr.empty())
                    {
                        break;
                    }
                }
                it++;
            }
            reply.SetStatusCode(STATUS_OK);
        }
        else if (subcmd == "list")
        {
            std::string info;
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            ContextSet::iterator it = m_all_clients.begin();
            uint64 now = get_current_epoch_micros();
            while (it != m_all_clients.end())
            {
                Context* client_ctx = *it;
                SocketChannel* conn = (SocketChannel*) (client_ctx->client->client);
                info.append("id=").append(stringfromll(conn->GetID())).append(" ");
                info.append("addr=").append(conn->GetRemoteStringAddress()).append(" ");
                info.append("fd=").append(stringfromll(conn->GetReadFD())).append(" ");
                info.append("name=").append(client_ctx->client->name).append(" ");
                uint64 borntime = client_ctx->client->uptime;
                uint64 elpased = (now <= borntime ? 0 : now - borntime) / 1000000;
                info.append("age=").append(stringfromll(elpased)).append(" ");
                uint64 activetime = client_ctx->client->last_interaction_ustime;
                elpased = (now <= activetime ? 0 : now - activetime) / 1000000;
                info.append("idle=").append(stringfromll(elpased)).append(" ");
                info.append("db=").append(client_ctx->ns.AsString()).append(" ");
                std::string cmd;
                RedisCommandHandlerSettingTable::iterator cit = m_settings.begin();
                while (cit != m_settings.end())
                {
                    if (cit->second.type == client_ctx->last_cmdtype)
                    {
                        cmd = cit->first;
                        break;
                    }
                    cit++;
                }
                info.append("cmd=").append(cmd).append(" ");
                info.append("\n");
                it++;
            }
            reply.SetString(info);
        }
        else
        {
            reply.SetErrorReason("CLIENT subcommand must be one of LIST, GETNAME, SETNAME, KILL, PAUSE");
        }
        return 0;
    }

    int Ardb::Config(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        std::string arg0 = string_tolower(cmd.GetArguments()[0]);
        if (arg0 != "get" && arg0 != "set" && arg0 != "resetstat" && arg0 != "reload" && arg0 != "rewrite")
        {
            reply.SetErrorReason("CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE, RELOAD");
            return 0;
        }
        if (arg0 == "resetstat")
        {
            if (cmd.GetArguments().size() != 1)
            {
                reply.SetErrorReason("Wrong number of arguments for CONFIG RESETSTAT");
                return 0;
            }
            Statistics::GetSingleton().Clear();
            reply.SetStatusCode(STATUS_OK);
        }
        else if (arg0 == "get")
        {
            if (cmd.GetArguments().size() != 2)
            {
                reply.SetErrorReason("Wrong number of arguments for CONFIG GET");
                return 0;
            }
            reply.ReserveMember(0);
            WriteLockGuard<SpinRWLock> guard(m_conf.lock);
            Properties::iterator it = m_conf.conf_props.begin();
            while (it != m_conf.conf_props.end())
            {
                if (stringmatchlen(cmd.GetArguments()[1].c_str(), cmd.GetArguments()[1].size(), it->first.c_str(), it->first.size(), 0) == 1)
                {
                    RedisReply& r = reply.AddMember();
                    r.SetString(it->first);
                    std::string buf;
                    ConfItemsArray::iterator cit = it->second.begin();
                    while (cit != it->second.end())
                    {
                        ConfItems::iterator ccit = cit->begin();
                        while (ccit != cit->end())
                        {
                            buf.append(*ccit).append(" ");
                            ccit++;
                        }
                        cit++;
                    }
                    RedisReply& r1 = reply.AddMember();
                    r1.SetString(trim_string(buf, " "));
                }
                it++;
            }
        }
        else if (arg0 == "set")
        {
            if (cmd.GetArguments().size() != 3)
            {
                reply.SetErrorReason("Wrong number of arguments for CONFIG SET");
                return 0;
            }
            conf_set(m_conf.conf_props, cmd.GetArguments()[1], cmd.GetArguments()[2]);
            WriteLockGuard<SpinRWLock> guard(m_conf.lock);
            m_conf.Parse(m_conf.conf_props);
            reply.SetStatusCode(STATUS_OK);
        }
        else if (arg0 == "reload")
        {
            if (!m_conf._conf_file.empty())
            {
                Properties props;
                if (parse_conf_file(m_conf._conf_file, props, " ") && m_conf.Parse(props))
                {
                    m_conf.conf_props = props;
                    reply.SetStatusCode(STATUS_OK);
                    return 0;
                }
            }
            reply.SetErrorReason("Failed to reload config");
        }
        else if (arg0 == "rewrite")
        {
            if (GetConf()._conf_file.empty())
            {
                reply.SetErrorReason("The server is running without a config file");
                return 0;
            }
            if (!rewrite_conf_file(GetConf()._conf_file, GetConf().conf_props, " "))
            {
                reply.SetErrorReason("Rewriting config file failed.");
                return 0;
            }
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    int Ardb::Time(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        uint64 micros = get_current_epoch_micros();
        RedisReply& r1 = reply.AddMember();
        RedisReply& r2 = reply.AddMember();
        r1.SetInteger(micros / 1000000);
        r2.SetInteger(micros % 1000000);
        return 0;
    }

    int Ardb::FlushDB(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        FlushDB(ctx, ctx.ns);
        return 0;
    }

    int Ardb::FlushAll(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        FlushAll(ctx);
        return 0;
    }

    int Ardb::Shutdown(Context& ctx, RedisCommandFrame& cmd)
    {
        //less than -1 means shutdown server
        ctx.GetReply().SetEmpty();
        return -2;
    }

    int Ardb::CompactDB(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        CompactDB(ctx, ctx.ns);
        return 0;
    }

    int Ardb::CompactAll(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        CompactAll(ctx);
        return 0;
    }
    /*
     *  SlaveOf host port
     *  Slaveof no one
     */
    int Ardb::Slaveof(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetArguments().size() % 2 != 0)
        {
            reply.SetErrorReason("not enough arguments for slaveof.");
            return 0;
        }
        const std::string& host = cmd.GetArguments()[0];
        uint32 port = 0;
        if (!string_touint32(cmd.GetArguments()[1], port))
        {
            if (!strcasecmp(cmd.GetArguments()[0].c_str(), "no") && !strcasecmp(cmd.GetArguments()[1].c_str(), "one"))
            {
                reply.SetStatusCode(STATUS_OK);
                g_repl->GetSlave().Stop();
                m_conf.master_host = "";
                m_conf.master_port = 0;
                return 0;
            }
            reply.SetErrorReason("value is not an integer or out of range");
            return 0;
        }
        if (!g_repl->IsInited())
        {
            WARN_LOG("Replication service is NOT intied to serve as master.");
            ctx.GetReply().SetErrorReason("server is singleton instance.");
            return -1;
        }
        if (GetConf().master_host != host || GetConf().master_port != port)
        {
            g_repl->GetSlave().Stop();
        }
        m_conf.master_host = host;
        m_conf.master_port = port;
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::ReplConf(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetArguments().size() % 2 != 0)
        {
            reply.SetErrorReason("ERR wrong number of arguments for ReplConf");
            return 0;
        }
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "listening-port"))
            {
                uint32 port = 0;
                string_touint32(cmd.GetArguments()[i + 1], port);
                g_repl->GetMaster().SetSlavePort(ctx.client->client, port);
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "ack"))
            {
                //do nothing
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "capa"))
            {
                //do nothing
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "getack"))
            {
                if (!GetConf().master_host.empty() && g_repl->GetSlave().IsSynced())
                {
                    g_repl->GetSlave().SendACK();
                }
            }
            else
            {
                WARN_LOG("Unknown replcon key:%s", cmd.GetArguments()[i].c_str());
            }
        }
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::Sync(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!g_repl->IsInited())
        {
            WARN_LOG("Replication service is NOT intied to serve as master.");
            ctx.GetReply().SetErrorReason("server is singleton instance.");
            return -1;
        }
        FreeClient(ctx);
        g_repl->GetMaster().AddSlave(ctx.client->client, cmd);
        ctx.GetReply().SetEmpty();
        return 0;
    }

    int Ardb::PSync(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!g_repl->IsInited())
        {
            WARN_LOG("Replication service is NOT intied to serve as master.");
            ctx.GetReply().SetErrorReason("server is singleton instance.");
            return -1;
        }
        FreeClient(ctx);
        g_repl->GetMaster().AddSlave(ctx.client->client, cmd);
        ctx.GetReply().SetEmpty();
        return 0;
    }

    static void monitor_write_callback(Channel* ch, void* data)
    {
        Buffer* buffer = (Buffer*) data;
        if (NULL != ch && !ch->IsClosed())
        {
            ch->GetOutputBuffer().Write(buffer, buffer->ReadableBytes());
            ch->EnableWriting();
        }
        DELETE(buffer);
    }

    void Ardb::FeedMonitors(Context& ctx, const Data& ns, RedisCommandFrame& cmd)
    {
        ReadLockGuard<SpinRWLock> guard(m_monitors_lock);
        if (NULL == m_monitors)
        {
            return;
        }
        Buffer buffer;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        buffer.Write("+", 1);
        buffer.Printf("%ld.%06ld ", (long) tv.tv_sec, (long) tv.tv_usec);
        if (ctx.flags.lua)
        {
            buffer.Printf("[%s lua] ", ns.AsString().c_str());
        }
        else if(NULL != ctx.client && NULL != ctx.client->client)
        {
            std::string addr, a1;
            ctx.client->client->GetRemoteAddress()->ToString(addr);
            buffer.Printf("[%s %s] ", ns.AsString().c_str(), addr.c_str());
        }
        buffer.PrintString(cmd.GetCommand());
        buffer.Write(" ", 1);
        for (size_t j = 0; j < cmd.GetArguments().size(); j++)
        {
            buffer.PrintString(cmd.GetArguments()[j]);
            if (j != cmd.GetArguments().size() - 1)
            {
                buffer.Write(" ", 1);
            }
        }
        buffer.Write("\r\n", 2);
        ContextSet::iterator it = m_monitors->begin();
        while (it != m_monitors->end())
        {
            Channel* client = (*it)->client->client;
            if (NULL != client)
            {
                Buffer* send_buffer = NULL;
                NEW(send_buffer, Buffer);
                send_buffer->Write(buffer.GetRawReadBuffer(), buffer.ReadableBytes());
                client->GetService().AsyncIO(client->GetID(), monitor_write_callback, send_buffer);
                //WriteReply()
            }
            it++;
        }
    }

    int Ardb::Monitor(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        WriteLockGuard<SpinRWLock> guard(m_monitors_lock);
        if (NULL == m_monitors)
        {
            NEW(m_monitors, ContextSet);
        }
        /*
         * monitor client set as slave
         */
        ctx.flags.slave = 1;
        if (!m_monitors->insert(&ctx).second)
        {
            reply.type = 0;
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    struct DebugReplayContext
    {
            int64_t offset;
            DebugReplayContext() :
                    offset(0)
            {
            }
    };

    static size_t debug_replay_wal(const void* log, size_t loglen, void* data)
    {
        DebugReplayContext* ctx = (DebugReplayContext*) data;
        Buffer logbuf((char*) log, 0, loglen);
        Context debug_ctx;
        debug_ctx.flags.no_wal = 1;
        debug_ctx.flags.slave = 1;
        while (logbuf.Readable())
        {
            RedisCommandFrame msg;
            size_t rest = logbuf.ReadableBytes();
            if (!RedisCommandDecoder::Decode(NULL, logbuf, msg))
            {
                break;
            }
            if (msg.GetRawProtocolData().Readable())
            {
                ctx->offset += msg.GetRawProtocolData().ReadableBytes();
                g_db->Call(debug_ctx, msg);
                RedisReply& r = debug_ctx.GetReply();
                if (r.IsErr())
                {
                    WARN_LOG("Debug replay error:%s", r.Error().c_str());
                }
                r.Clear();
            }
            else
            {
                ERROR_LOG("Invalid msg with empty protocol data:%s", msg.GetCommand().c_str());
                break;
            }
        }
        return loglen - logbuf.ReadableBytes();
    }

    int Ardb::Debug(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        if (!strcasecmp(cmd.GetArguments()[0].c_str(), "iterator"))
        {
            KeyObject empty;
            empty.SetNameSpace(ctx.ns);
            ctx.flags.iterate_total_order = 1;
            KeyObject start(ctx.ns, KEY_META, cmd.GetArguments()[1]);
            Iterator* iter = g_db->GetEngine()->Find(ctx, empty);
            if (iter->Valid())
            {
                iter->Jump(start);
            }
            while (iter->Valid())
            {
                KeyObject& k = iter->Key();
                if (k.GetKey() != start.GetKey())
                {
                    break;
                }
                INFO_LOG("Internal key:%s with type:%u", k.GetKey().AsString().c_str(), k.GetType());
                iter->Next();
            }
            DELETE(iter);
        }
        else if (!strcasecmp(cmd.GetArguments()[0].c_str(), "replay"))
        {
            DebugReplayContext replay_ctx;
            string_toint64(cmd.GetArguments()[1], replay_ctx.offset);
            INFO_LOG("Start to replay wal from sync offset:%lld & wal end offset:%llu", replay_ctx.offset, g_repl->GetReplLog().WALEndOffset());

            int64_t replay_init_offset = replay_ctx.offset;
            const int64_t replay_process_events_interval_bytes = 10 * 1024 * 1024;
            const int64_t max_replay_bytes = 1024 * 1024;
            while (true)
            {
                if (replay_ctx.offset < g_repl->GetReplLog().WALStartOffset() || replay_ctx.offset > g_repl->GetReplLog().WALEndOffset())
                {
                    ERROR_LOG("Failed to replay wal with sync_offset:%lld, wal_start_offset:%llu, wal_end_offset:%lld", replay_ctx.offset,
                            g_repl->GetReplLog().WALStartOffset(), g_repl->GetReplLog().WALEndOffset());
                    break;
                }

                int64_t before_replayed_bytes = replay_ctx.offset - replay_init_offset;
                g_repl->GetReplLog().Replay(replay_ctx.offset, max_replay_bytes, debug_replay_wal, &replay_ctx);
                int64_t after_replayed_bytes = replay_ctx.offset - replay_init_offset;
                if (after_replayed_bytes / replay_process_events_interval_bytes > before_replayed_bytes / replay_process_events_interval_bytes)
                {
                    INFO_LOG("%lld bytes replayed from wal log, %llu bytes left.", after_replayed_bytes,
                            g_repl->GetReplLog().WALEndOffset() - replay_ctx.offset);
                }
                if (replay_ctx.offset == g_repl->GetReplLog().WALEndOffset())
                {
                    break;
                }
            }
        }
        else if (!strcasecmp(cmd.GetArguments()[0].c_str(), "dwc"))
        {
            /*
             * dump wal cache
             */
            g_repl->GetReplLog().DebugDumpCache(g_db->GetConf().home + "/dwc.txt");
        }
        return 0;
    }
}


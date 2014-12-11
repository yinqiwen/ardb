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

#include "ardb.hpp"
#include "util/socket_address.hpp"
#include "util/lru.hpp"
#include "util/system_helper.hpp"
#include <sstream>
#include <sys/utsname.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

namespace ardb
{
    static int RDBSaveLoadRoutine(void* cb)
    {
        ChannelService* serv = (ChannelService*) cb;
        serv->Continue();
        return 0;
    }

    DataDumpFile& Ardb::GetDataDumpFile()
    {
        if (m_cfg.backup_redis_format)
        {
            return m_redis_dump;
        }
        else
        {
            return m_ardb_dump;
        }
    }

    int Ardb::Save(Context& ctx, RedisCommandFrame& cmd)
    {
        char tmp[1024];
        uint32 now = time(NULL);
        ChannelService& serv = ctx.client->GetService();
        uint32 conn_id = ctx.client->GetID();
        if (m_cfg.backup_redis_format)
        {
            sprintf(tmp, "%s/dump-%u.rdb", m_cfg.backup_dir.c_str(), now);
        }
        else
        {
            sprintf(tmp, "%s/dump-%u.ardb", m_cfg.backup_dir.c_str(), now);
        }
        int ret = GetDataDumpFile().Save(tmp, RDBSaveLoadRoutine, &serv);
        if (serv.GetChannel(conn_id) != NULL)
        {
            if (ret == 0)
            {
                fill_status_reply(ctx.reply, "OK");
            }
            else
            {
                fill_error_reply(ctx.reply, "Save error");
            }
            return 0;
        }
        return -1;
    }

    int Ardb::LastSave(Context& ctx, RedisCommandFrame& cmd)
    {
        int ret = GetDataDumpFile().LastSave();
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int Ardb::BGSave(Context& ctx, RedisCommandFrame& cmd)
    {
        char tmp[1024];
        uint32 now = time(NULL);
        if (m_cfg.backup_redis_format)
        {
            sprintf(tmp, "%s/dump-%u.rdb", m_cfg.backup_dir.c_str(), now);
        }
        else
        {
            sprintf(tmp, "%s/dump-%u.ardb", m_cfg.backup_dir.c_str(), now);
        }
        int ret = GetDataDumpFile().BGSave(tmp);
        if (ret == 0)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "save error");
        }
        return 0;
    }

    int Ardb::Import(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string file = m_cfg.backup_dir + "/dump.ardb";
        if (cmd.GetArguments().size() == 1)
        {
            file = cmd.GetArguments()[0];
        }
        int ret = 0;
        ChannelService& serv = ctx.client->GetService();
        uint32 conn_id = ctx.client->GetID();
        if (RedisDumpFile::IsRedisDumpFile(file) == 1)
        {
            RedisDumpFile rdb;
            rdb.Init(this);
            ret = rdb.Load(ctx.identity, file, RDBSaveLoadRoutine, &(ctx.client->GetService()));
        }
        else
        {
            ret = m_ardb_dump.Load(ctx.identity, file, RDBSaveLoadRoutine, &(ctx.client->GetService()));
        }
        if (serv.GetChannel(conn_id) != NULL)
        {
            if (ret == 0)
            {
                fill_status_reply(ctx.reply, "OK");
            }
            else
            {
                fill_error_reply(ctx.reply, "Import error");
            }
            return 0;
        }
        return -1;
    }

    void Ardb::FillInfoResponse(const std::string& section, std::string& info)
    {
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "server"))
        {
            struct utsname name;
            uname(&name);
            info.append("# Server\r\n");
            info.append("ardb_version:").append(ARDB_VERSION).append("\r\n");
            info.append("ardb_home:").append(m_cfg.home).append("\r\n");
            info.append("engine:").append(m_engine_factory.GetName()).append("\r\n");
            info.append("os:").append(name.sysname).append(" ").append(name.release).append(" ").append(name.machine).append(
                    "\r\n");
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

            if (NULL != m_repl_backlog.GetServerKey())
            {
                info.append("run_id:").append(m_repl_backlog.GetServerKey()).append("\r\n");
            }
            info.append("tcp_port:").append(stringfromll(m_cfg.PrimayPort())).append("\r\n");
            info.append("listen:").append(string_join_container(m_cfg.listen_addresses, ",")).append("\r\n");
            time_t uptime = time(NULL) - m_starttime;
            info.append("uptime_in_seconds:").append(stringfromll(uptime)).append("\r\n");
            info.append("uptime_in_days:").append(stringfromll(uptime / (3600 * 24))).append("\r\n");
            info.append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "clients"))
        {
            info.append("# Clients\r\n");
            {
                LockGuard<SpinMutexLock> guard(m_clients_lock);
                info.append("connected_clients:").append(stringfromll(m_clients.size())).append("\r\n");
            }
            {
                WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
                info.append("blocked_clients:").append(stringfromll(m_block_context_table.size())).append("\r\n");
            }
            info.append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "databases"))
        {
            info.append("# Databases\r\n");
            info.append("data_dir:").append(m_cfg.data_base_path).append("\r\n");
            info.append(GetKeyValueEngine().Stats()).append("\r\n");
            info.append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "cpu"))
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
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "disk"))
        {
            info.append("# Disk\r\n");
            int64 filesize = file_size(m_cfg.data_base_path);
            char tmp[256];
            sprintf(tmp, "%" PRId64, filesize);
            info.append("db_used_space:").append(tmp).append("\r\n");
            info.append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "replication"))
        {
            info.append("# Replication\r\n");
            if (m_cfg.repl_backlog_size > 0)
            {
                if (m_slave.GetMasterAddress().GetHost().empty())
                {
                    info.append("role:master\r\n");
                }
                else
                {
                    info.append("role:slave\r\n");
                }
                info.append("repl_dir: ").append(m_cfg.repl_data_dir).append("\r\n");
            }
            else
            {
                info.append("role: singleton\r\n");
            }

            if (m_repl_backlog.IsInited())
            {
                if (!m_cfg.master_host.empty())
                {
                    info.append("master_host:").append(m_cfg.master_host).append("\r\n");
                    info.append("master_port:").append(stringfromll(m_cfg.master_port)).append("\r\n");
                    info.append("master_link_status:").append(m_slave.IsConnected() ? "up" : "down").append("\r\n");
                    if (m_slave.IsConnected())
                    {
                        info.append("master_last_io_seconds_ago:").append(
                                stringfromll(time(NULL) - m_slave.GetMasterLastinteractionTime())).append("\r\n");
                    }
                    else
                    {
                        info.append("master_last_io_seconds_ago:").append("-1").append("\r\n");
                    }
                    info.append("master_sync_in_progress:").append(m_slave.IsSyncing() ? "1" : "0").append("\r\n");
                    if (m_slave.GetState() == SLAVE_STATE_SYNING_DUMP_DATA)
                    {
                        info.append("master_sync_left_bytes:").append(stringfromll(m_slave.SyncLeftBytes())).append(
                                "\r\n");
                    }
                    if (m_slave.GetState() == SLAVE_STATE_LOADING_DUMP_DATA)
                    {
                        info.append("slave_loading_left_bytes:").append(stringfromll(m_slave.LoadingLeftBytes())).append(
                                "\r\n");
                    }
                    info.append("slave_repl_offset:").append(stringfromll(m_repl_backlog.GetReplEndOffset())).append(
                            "\r\n");
                    if (!m_slave.IsConnected())
                    {
                        info.append("master_link_down_since_seconds:").append(
                                stringfromll(time(NULL) - m_slave.GetMasterLinkDownTime())).append("\r\n");
                    }
                    info.append("slave_priority:").append(stringfromll(m_cfg.slave_priority)).append("\r\n");
                }

                info.append("connected_slaves: ").append(stringfromll(m_master.ConnectedSlaves())).append("\r\n");
                m_master.PrintSlaves(info);
                info.append("master_repl_offset: ").append(stringfromll(m_repl_backlog.GetReplEndOffset())).append(
                        "\r\n");
                info.append("repl_backlog_size: ").append(stringfromll(m_repl_backlog.GetBacklogSize())).append("\r\n");
                info.append("repl_backlog_first_byte_offset: ").append(
                        stringfromll(m_repl_backlog.GetReplStartOffset())).append("\r\n");
                info.append("repl_backlog_histlen: ").append(stringfromll(m_repl_backlog.GetHistLen())).append("\r\n");
                info.append("repl_backlog_cksm: ").append(base16_stringfromllu(m_repl_backlog.GetChecksum())).append(
                        "\r\n");
            }
            else
            {
                info.append("repl_backlog_size: 0").append("\r\n");
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "memory"))
        {
            info.append("# Memory\r\n");
            std::string tmp;
            info.append("used_memory_rss:").append(stringfromll(mem_rss_size())).append("\r\n");
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "stats"))
        {
            info.append("# Stats\r\n");
            std::string tmp;
            info.append(m_stat.PrintStat(tmp));
            WriteLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
            info.append("pubsub_channels:").append(stringfromll(m_pubsub_channels.size())).append("\r\n");
            info.append("pubsub_patterns:").append(stringfromll(m_pubsub_patterns.size())).append("\r\n");
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "keyspace"))
        {
            DBIDSet ids;
            GetAllDBIDSet(ids);
            if (!ids.empty())
            {
                info.append("# Keyspace\r\n");
                DBIDSet::iterator it = ids.begin();
                while (it != ids.end())
                {
                    /*
                     * Can NOT tell how many keys now
                     */
                    info.append("db").append(stringfromll(*it)).append(":").append("keys=-1").append("\r\n");
                    it++;
                }
                info.append("\r\n");
            }
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "cache"))
        {
            info.append("# Cache\r\n");
            std::string tmp;
            m_cache.PrintStat(tmp);
            info.append(tmp);
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "compact"))
        {
            info.append("# Compact\r\n");
            info.append("compacting:").append(m_compacting ? "yes" : "no").append("\r\n");
            info.append("write_count_since_last_compact:").append(
                    stringfromll(m_stat.GetLatencyStat().write_count_since_last_compact)).append("\r\n");
            if (m_last_compact_start_time > 0)
            {
                struct tm tt;
                if (NULL != localtime_r(&m_last_compact_start_time, &tt))
                {
                    char tmp[1024];
                    strftime(tmp, sizeof(tmp), "%F %T", &tt);
                    info.append("last_compact_start_time:").append(tmp).append("\r\n");
                    info.append("last_compact_duration:").append(stringfromll(m_last_compact_duration)).append(
                            "ms\r\n");
                }
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "commandstats"))
        {
            info.append("# Commandstats\r\n");
            RedisCommandHandlerSettingTable::iterator cit = m_settings.begin();
            while (cit != m_settings.end())
            {
                RedisCommandHandlerSetting& setting = cit->second;
                if (setting.calls > 0)
                {
                    info.append("cmdstat_").append(setting.name).append(":").append("calls=").append(
                            stringfromll(setting.calls)).append(",usec=").append(stringfromll(setting.microseconds)).append(
                            ",usecpercall=").append(stringfromll(setting.microseconds / setting.calls)).append("\r\n");
                }

                cit++;
            }
            info.append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "misc"))
        {
            info.append("# Misc\r\n");
            if (!m_cfg.additional_misc_info.empty())
            {
                string_replace(m_cfg.additional_misc_info, "\\n", "\r\n");
                info.append(m_cfg.additional_misc_info).append("\r\n");
            }
            if (m_cfg.scan_redis_compatible)
            {
                LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
                info.append("redis_scan_cursor_cache_size:").append(stringfromll(m_redis_cursor_table.size())).append(
                        "\r\n");
            }
            info.append("\r\n");
        }
    }

    int Ardb::Info(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string info;
        std::string section = "all";
        if (cmd.GetArguments().size() == 1)
        {
            section = cmd.GetArguments()[0];
        }
        FillInfoResponse(section, info);
        fill_str_reply(ctx.reply, info);
        return 0;
    }

    int Ardb::DBSize(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_int_reply(ctx.reply, file_size(m_cfg.data_base_path));
        return 0;
    }

    static void ChannelCloseCallback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->Close();
        }
    }

    int Ardb::Client(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string subcmd = string_tolower(cmd.GetArguments()[0]);
        if (subcmd == "setname")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply,
                        "Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            ctx.name = cmd.GetArguments()[1];
            fill_status_reply(ctx.reply, "OK");
        }
        else if (subcmd == "getname")
        {
            if (cmd.GetArguments().size() != 1)
            {
                fill_error_reply(ctx.reply,
                        "Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            fill_str_reply(ctx.reply, ctx.name);
        }
        else if (subcmd == "pause")
        {
            //pause all clients
            //todo
            fill_error_reply(ctx.reply, "NOT supported now");
        }
        else if (subcmd == "kill")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply,
                        "Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | PAUSE timeout)");
                return 0;
            }
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            ContextTable::iterator it = m_clients.begin();
            while (it != m_clients.end())
            {
                SocketChannel* conn = (SocketChannel*) (it->second->client);
                if (conn->GetRemoteStringAddress() == cmd.GetArguments()[1])
                {
                    if (it->second->processing)
                    {
                        it->second->close_after_processed = true;
                    }
                    else
                    {
                        conn->GetService().AsyncIO(conn->GetID(), ChannelCloseCallback, NULL);
                    }
                    break;
                }
                it++;
            }
            fill_status_reply(ctx.reply, "OK");
        }
        else if (subcmd == "list")
        {
            std::string& info = ctx.reply.str;
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            ContextTable::iterator it = m_clients.begin();
            uint64 now = get_current_epoch_micros();
            while (it != m_clients.end())
            {
                SocketChannel* conn = (SocketChannel*) (it->second->client);
                info.append("id=").append(stringfromll(it->first)).append(" ");
                info.append("addr=").append(conn->GetRemoteStringAddress()).append(" ");
                info.append("fd=").append(stringfromll(it->second->client->GetReadFD())).append(" ");
                info.append("name=").append(it->second->name).append(" ");
                uint64 borntime = it->second->born_time;
                uint64 elpased = (now <= borntime ? 0 : now - borntime) / 1000000;
                info.append("age=").append(stringfromll(elpased)).append(" ");
                uint64 activetime = it->second->last_interaction_ustime;
                elpased = (now <= activetime ? 0 : now - activetime) / 1000000;
                info.append("idle=").append(stringfromll(elpased)).append(" ");
                info.append("db=").append(stringfromll(it->second->currentDB)).append(" ");
                std::string cmd;
                RedisCommandHandlerSettingTable::iterator cit = m_settings.begin();
                while (cit != m_settings.end())
                {
                    if (cit->second.type == it->second->current_cmd_type)
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
            ctx.reply.type = REDIS_REPLY_STRING;
        }
        else
        {
            fill_error_reply(ctx.reply, "CLIENT subcommand must be one of LIST, GETNAME, SETNAME, KILL, PAUSE");
        }
        return 0;
    }

    int Ardb::Config(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string arg0 = string_tolower(cmd.GetArguments()[0]);
        if (arg0 != "get" && arg0 != "set" && arg0 != "resetstat" && arg0 != "add" && arg0 != "del" && arg0 != "reload")
        {
            fill_error_reply(ctx.reply, "CONFIG subcommand must be one of GET, SET, RESETSTAT, ADD, DEL");
            return 0;
        }
        if (arg0 == "resetstat")
        {
            if (cmd.GetArguments().size() != 1)
            {
                fill_error_reply(ctx.reply, "Wrong number of arguments for CONFIG RESETSTAT");
                return 0;
            }
            fill_error_reply(ctx.reply, "Not supported now");
        }
        else if (arg0 == "get")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply, "Wrong number of arguments for CONFIG GET");
                return 0;
            }
            ctx.reply.type = REDIS_REPLY_ARRAY;
            Properties::iterator it = m_cfg.conf_props.begin();
            while (it != m_cfg.conf_props.end())
            {
                if (stringmatchlen(cmd.GetArguments()[1].c_str(), cmd.GetArguments()[1].size(), it->first.c_str(),
                        it->first.size(), 0) == 1)
                {
                    RedisReply& r = ctx.reply.AddMember();
                    fill_str_reply(r, it->first);

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
                    RedisReply& r1 = ctx.reply.AddMember();
                    fill_str_reply(r1, trim_string(buf, " "));
                }
                it++;
            }
        }
        else if (arg0 == "set")
        {
            if (cmd.GetArguments().size() != 3)
            {
                fill_error_reply(ctx.reply, "Wrong number of arguments for CONFIG SET");
                return 0;
            }
            conf_set(m_cfg.conf_props, cmd.GetArguments()[1], cmd.GetArguments()[2]);
            WriteLockGuard<SpinRWLock> guard(m_cfg_lock);
            m_cfg.Parse(m_cfg.conf_props);
            fill_status_reply(ctx.reply, "OK");
        }
        else if (arg0 == "add")
        {
            if (cmd.GetArguments().size() != 3)
            {
                fill_error_reply(ctx.reply, "Wrong number of arguments for CONFIG ADD");
                return 0;
            }
            conf_set(m_cfg.conf_props, cmd.GetArguments()[1], cmd.GetArguments()[2], false);
            WriteLockGuard<SpinRWLock> guard(m_cfg_lock);
            m_cfg.Parse(m_cfg.conf_props);
            fill_status_reply(ctx.reply, "OK");
        }
        else if (arg0 == "del")
        {
            if (cmd.GetArguments().size() != 3)
            {
                fill_error_reply(ctx.reply, "Wrong number of arguments for CONFIG DEL");
                return 0;
            }
            conf_del(m_cfg.conf_props, cmd.GetArguments()[1], cmd.GetArguments()[2]);
            WriteLockGuard<SpinRWLock> guard(m_cfg_lock);
            m_cfg.Parse(m_cfg.conf_props);
            fill_status_reply(ctx.reply, "OK");
        }
        else if (arg0 == "reload")
        {
            if (!m_cfg.conf_path.empty())
            {
                Properties props;
                if (parse_conf_file(m_cfg.conf_path, props, " ") && m_cfg.Parse(props))
                {
                    m_cfg.conf_props = props;
                    m_stat.Init();
                    fill_status_reply(ctx.reply, "OK");
                    return 0;
                }
            }
            fill_error_reply(ctx.reply, "Failed to reload config");
        }
        else
        {
            //just return error
            fill_error_reply(ctx.reply, "Not supported now");
        }
        return 0;
    }

    int Ardb::Time(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 micros = get_current_epoch_micros();
        RedisReply& r1 = ctx.reply.AddMember();
        RedisReply& r2 = ctx.reply.AddMember();
        fill_int_reply(r1, micros / 1000000);
        fill_int_reply(r2, micros % 1000000);
        return 0;
    }

    void Ardb::GetAllDBIDSet(DBIDSet& ids)
    {
        {
            LockGuard<SpinMutexLock> guard(m_cached_dbids_lock);
            if (!m_cached_dbids.empty())
            {
                ids = m_cached_dbids;
                return;
            }
        }

        KeyObject k;
        k.db = 0;
        k.type = KEY_META;
        Iterator* iter = IteratorKeyValue(k, false);
        if (NULL != iter)
        {
            while (iter->Valid())
            {
                KeyObject kk;
                if (decode_key(iter->Key(), kk) && kk.db != ARDB_GLOBAL_DB)
                {
                    ids.insert(kk.db);
                    KeyObject next;
                    next.db = kk.db + 1;
                    next.type = KEY_META;
                    IteratorSeek(iter, next);
                }
                else
                {
                    break;
                }
            }
            DELETE(iter);
        }
        LockGuard<SpinMutexLock> lockguard(m_cached_dbids_lock);
        m_cached_dbids = ids;
    }

    int Ardb::FlushAllData(Context& ctx)
    {
        KeyObject k;
        k.db = 0;
        k.type = KEY_META;

        m_cache.EvictAll();
        BatchWriteGuard guard(ctx);
        Iterator* iter = IteratorKeyValue(k, false);
        if (NULL != iter)
        {
            while (iter->Valid())
            {
                DelRaw(ctx, iter->Key());
                iter->Next();
            }
            DELETE(iter);
        }
        return 0;
    }
    int Ardb::FlushDBData(Context& ctx)
    {
        KeyObject k;
        k.db = ctx.currentDB;
        k.type = KEY_META;

        m_cache.EvictDB(ctx.currentDB);

        BatchWriteGuard guard(ctx);
        Iterator* iter = IteratorKeyValue(k, false);
        if (NULL != iter)
        {
            while (iter->Valid())
            {
                KeyObject kk;
                if (decode_key(iter->Key(), kk))
                {
                    if (kk.db != ctx.currentDB)
                    {
                        break;
                    }
                    DelRaw(ctx, iter->Key());
                }
                iter->Next();
            }
            DELETE(iter);
        }
        return 0;
    }

    int Ardb::FlushDB(Context& ctx, RedisCommandFrame& cmd)
    {
        FlushDBData(ctx);
        fill_status_reply(ctx.reply, "OK");

        DBIDSet ids;
        GetAllDBIDSet(ids);
        LockGuard<SpinMutexLock> guard(m_cached_dbids_lock);
        m_cached_dbids.erase(ctx.currentDB);
        return 0;
    }

    int Ardb::FlushAll(Context& ctx, RedisCommandFrame& cmd)
    {
        FlushAllData(ctx);
        fill_status_reply(ctx.reply, "OK");
        LockGuard<SpinMutexLock> guard(m_cached_dbids_lock);
        m_cached_dbids.clear();
        return 0;
    }

    int Ardb::Shutdown(Context& ctx, RedisCommandFrame& cmd)
    {
        m_service->Stop();
        return -1;
    }

    int Ardb::DoCompact(const std::string& start, const std::string& end)
    {
        if (m_compacting)
        {
            return 0;
        }
        m_compacting = true;
        m_last_compact_start_time = time(NULL);
        uint64 compact_start = get_current_epoch_millis();
        GetKeyValueEngine().CompactRange(start, end);
        uint64 compact_end = get_current_epoch_millis();
        m_last_compact_duration = compact_end - compact_start;
        m_compacting = false;
        return 0;
    }

    int Ardb::DoCompact(Context& ctx, DBID db, bool sync)
    {
        if (m_compacting)
        {
            return 0;
        }
        std::string raw_start, raw_end;
        if (db != ARDB_GLOBAL_DB)
        {
            KeyObject start;
            start.type = KEY_META;
            start.db = db;
            KeyObject end;
            end.type = KEY_META;
            end.db = db + 1;
            start.Encode();
            end.Encode();
            raw_start = start.encode_buf.AsString();
            raw_end = end.encode_buf.AsString();
        }
        struct CompactTask: public Thread
        {
                Ardb* adb;
                std::string start, end;
                CompactTask(Ardb* db, const std::string& s, const std::string& e) :
                        adb(db), start(s), end(e)
                {
                }
                void Run()
                {
                    adb->DoCompact(start, end);
                    delete this;
                }
        };
        if (sync)
        {
            DoCompact(raw_start, raw_end);
        }
        else
        {
            /*
             * Start a background thread to compact kvs
             */
            Thread* t = new CompactTask(this, raw_start, raw_end);
            t->Start();
        }
        return 0;
    }

    int Ardb::CompactDB(Context& ctx, RedisCommandFrame& cmd)
    {
        if (m_compacting)
        {
            fill_error_reply(ctx.reply, "Server is compacting");
            return 0;
        }
        DoCompact(ctx, ctx.currentDB, false);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int Ardb::CompactAll(Context& ctx, RedisCommandFrame& cmd)
    {
        if (m_compacting)
        {
            fill_error_reply(ctx.reply, "Server is compacting");
            return 0;
        }
        DoCompact(ctx, ARDB_GLOBAL_DB, false);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    /*
     *  SlaveOf host port [include 1|2|3] [exclude 0|1|2]
     *  Slaveof no one
     */
    int Ardb::Slaveof(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "not enough arguments for slaveof.");
            return 0;
        }
        const std::string& host = cmd.GetArguments()[0];
        uint32 port = 0;
        if (!string_touint32(cmd.GetArguments()[1], port))
        {
            if (!strcasecmp(cmd.GetArguments()[0].c_str(), "no") && !strcasecmp(cmd.GetArguments()[1].c_str(), "one"))
            {
                fill_status_reply(ctx.reply, "OK");
                m_slave.Stop();
                m_cfg.master_host = "";
                m_cfg.master_port = 0;
                return 0;
            }
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
        {
            if (cmd.GetArguments()[i] == "include")
            {
                DBIDArray ids;
                split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                m_slave.SetIncludeDBs(ids);
            }
            else if (cmd.GetArguments()[i] == "exclude")
            {
                DBIDArray ids;
                split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                m_slave.SetExcludeDBs(ids);
            }
        }
        m_cfg.master_host = host;
        m_cfg.master_port = port;
        m_slave.ConnectMaster(host, port);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int Ardb::ReplConf(Context& ctx, RedisCommandFrame& cmd)
    {
        //DEBUG_LOG("%s %s", cmd.GetArguments()[0].c_str(), cmd.GetArguments()[1].c_str());
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "ERR wrong number of arguments for ReplConf");
            return 0;
        }
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "listening-port"))
            {
                uint32 port = 0;
                string_touint32(cmd.GetArguments()[i + 1], port);
                Address* addr = const_cast<Address*>(ctx.client->GetRemoteAddress());
                if (InstanceOf<SocketHostAddress>(addr).OK)
                {
                    SocketHostAddress* tmp = (SocketHostAddress*) addr;
                    const SocketHostAddress& master_addr = m_slave.GetMasterAddress();
                    if (master_addr.GetHost() == tmp->GetHost() && master_addr.GetPort() == port)
                    {
                        ERROR_LOG("Can NOT accept this slave connection from master[%s:%u]",
                                master_addr.GetHost().c_str(), master_addr.GetPort());
                        fill_error_reply(ctx.reply, "Reject connection as slave from master instance.");
                        return -1;
                    }
                }
                m_master.AddSlavePort(ctx.client, port);
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "ack"))
            {
                //do nothing
            }
        }
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int Ardb::Sync(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!m_repl_backlog.IsInited())
        {
            fill_error_reply(ctx.reply, "Ardb instance's replication log not enabled, can NOT serve as master.");
            return -1;
        }
        if (!m_cfg.master_host.empty() && !m_slave.IsSynced())
        {
            fill_error_reply(ctx.reply, "Server is not synced with remote master, can NOT serv as master.");
            return -1;
        }
        m_master.AddSlave(ctx.client, cmd);
        FreeClientContext(ctx);
        return 0;
    }

    int Ardb::PSync(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!m_repl_backlog.IsInited())
        {
            fill_error_reply(ctx.reply, "Ardb instance's replication backlog not enabled, can NOT serve as master.");
            return 0;
        }
        if (!m_cfg.master_host.empty() && !m_slave.IsSynced())
        {
            fill_error_reply(ctx.reply, "Server is not synced with remote master, can NOT serv as master.");
            return 0;
        }
        m_master.AddSlave(ctx.client, cmd);
        FreeClientContext(ctx);
        return 0;
    }
}


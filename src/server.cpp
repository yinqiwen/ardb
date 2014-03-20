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

#include "ardb_server.hpp"
#include "util/socket_address.hpp"
#include "util/lru.hpp"
#include <fnmatch.h>
#include <sstream>

namespace ardb
{
    static void RDBSaveLoadRoutine(void* cb)
    {
        Channel* client = (Channel*) cb;
        client->GetService().Continue();
    }

    int ArdbServer::Save(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        char tmp[1024];
        uint32 now = time(NULL);
        sprintf(tmp, "%s/dump-%u.ardb", m_cfg.backup_dir.c_str(), now);
        if (m_rdb.OpenWriteFile(tmp) == -1)
        {
            fill_error_reply(ctx.reply, "ERR Save error");
            return 0;
        }
        int ret = m_rdb.Save(RDBSaveLoadRoutine, ctx.conn);
        if (ret == 0)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "ERR Save error");
        }
        return 0;
    }

    int ArdbServer::LastSave(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_rdb.LastSave();
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::BGSave(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        char tmp[1024];
        uint32 now = time(NULL);
        sprintf(tmp, "%s/dump-%u.ardb", m_cfg.backup_dir.c_str(), now);
        if (m_rdb.OpenWriteFile(tmp) == -1)
        {
            fill_error_reply(ctx.reply, "ERR Save error");
            return 0;
        }
        int ret = m_rdb.BGSave();
        if (ret == 0)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "ERR Save error");
        }
        return 0;
    }

    int ArdbServer::Import(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string file = m_cfg.backup_dir + "/dump.ardb";
        if (cmd.GetArguments().size() == 1)
        {
            file = cmd.GetArguments()[0];
        }
        int ret = 0;
        if (RedisDumpFile::IsRedisDumpFile(file) == 1)
        {
            RedisDumpFile rdb(m_db, file);
            ret = rdb.Load(RDBSaveLoadRoutine, ctx.conn);
        }
        else
        {
            if (m_rdb.OpenReadFile(file) == -1)
            {
                fill_error_reply(ctx.reply, "ERR Import error");
                return 0;
            }
            ret = m_rdb.Load(RDBSaveLoadRoutine, ctx.conn);
        }
        if (ret == 0)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "ERR Import error");
        }
        return 0;
    }

    void ArdbServer::FillInfoResponse(const std::string& section, std::string& info)
    {
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "server"))
        {
            info.append("# Server\r\n");
            info.append("ardb_version:").append(ARDB_VERSION).append("\r\n");
            info.append("ardb_home:").append(m_cfg.home).append("\r\n");
            info.append("engine:").append(m_engine.GetName()).append("\r\n");
            char tmp[256];
            sprintf(tmp, "%d.%d.%d",
#ifdef __GNUC__
                    __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__
#else
                    0,0,0
#endif
                    );
            info.append("gcc_version:").append(tmp).append("\r\n");
            if (NULL != m_master_serv.GetReplBacklog().GetServerKey())
            {
                info.append("run_id:").append(m_master_serv.GetReplBacklog().GetServerKey()).append("\r\n");
            }
            sprintf(tmp, "%"PRId64, m_cfg.listen_port);
            info.append("tcp_port:").append(tmp).append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "clients"))
        {
            info.append("# Clients\r\n");
            info.append("connected_clients:").append(stringfromll(ServerStat::GetSingleton().connected_clients)).append(
                    "\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "databases"))
        {
            info.append("# Databases\r\n");
            info.append("data_dir:").append(m_cfg.data_base_path).append("\r\n");
            info.append(m_db->GetEngine()->Stats()).append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "disk"))
        {
            info.append("# Disk\r\n");
            int64 filesize = file_size(m_cfg.data_base_path);
            char tmp[256];
            sprintf(tmp, "%"PRId64, filesize);
            info.append("db_used_space:").append(tmp).append("\r\n");
        }
        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "replication"))
        {
            info.append("# Replication\r\n");
            if (m_cfg.repl_backlog_size > 0)
            {
                if (m_slave_client.GetMasterAddress().GetHost().empty())
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
                    info.append("master_link_status:").append(m_slave_client.IsConnected() ? "up" : "down").append(
                            "\r\n");
                    info.append("slave_repl_offset:").append(stringfromll(m_repl_backlog.GetReplEndOffset())).append(
                            "\r\n");
                    if (!m_slave_client.IsConnected())
                    {
                        info.append("master_link_down_since_seconds:").append(
                                stringfromll(time(NULL) - m_slave_client.GetMasterLinkDownTime())).append("\r\n");

                    }
                    info.append("slave_priority:").append(stringfromll(m_cfg.slave_priority)).append("\r\n");

                }

                info.append("connected_slaves: ").append(stringfromll(m_master_serv.ConnectedSlaves())).append("\r\n");
                m_master_serv.PrintSlaves(info);
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

        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "stats"))
        {
            info.append("# Stats\r\n");
            info.append("total_commands_processed:").append(stringfromll(ServerStat::GetSingleton().stat_numcommands)).append(
                    "\r\n");
            info.append("total_connections_received:").append(
                    stringfromll(ServerStat::GetSingleton().stat_numconnections)).append("\r\n");
            char qps[100];
            sprintf(qps, "%.2f", ServerStat::GetSingleton().CurrentQPS());
            info.append("current_commands_qps:").append(qps).append("\r\n");
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "memory"))
        {
            info.append("# Memory\r\n");
            if (NULL != m_db->GetL1Cache())
            {
                info.append("L1_cache_enable:1\r\n");
                info.append("L1_cache_max_memory:").append(stringfromll(m_db->GetConfig().L1_cache_memory_limit)).append(
                        "\r\n");
                info.append("L1_cache_entries:").append(stringfromll(m_db->GetL1Cache()->GetEntrySize())).append(
                        "\r\n");
                info.append("L1_cache_estimate_memory:").append(
                        stringfromll(m_db->GetL1Cache()->GetEstimateMemorySize())).append("\r\n");
            }
            else
            {
                info.append("L1_cache_enable:0\r\n");
            }
        }

        if (!strcasecmp(section.c_str(), "all") || !strcasecmp(section.c_str(), "misc"))
        {
            if (!m_cfg.additional_misc_info.empty())
            {
                info.append("# Misc\r\n");
                string_replace(m_cfg.additional_misc_info, "\\n", "\r\n");
                info.append(m_cfg.additional_misc_info).append("\r\n");
            }
        }
    }

    int ArdbServer::Info(ArdbConnContext& ctx, RedisCommandFrame& cmd)
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

    int ArdbServer::DBSize(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        fill_int_reply(ctx.reply, file_size(m_cfg.data_base_path));
        return 0;
    }

    int ArdbServer::Client(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string subcmd = string_tolower(cmd.GetArguments()[0]);
        if (subcmd == "kill")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            Channel* conn = m_clients_holder.GetConn(cmd.GetArguments()[1]);
            if (NULL == conn)
            {
                fill_error_reply(ctx.reply, "ERR No such client");
                return 0;
            }
            fill_status_reply(ctx.reply, "OK");
            if (conn == ctx.conn)
            {
                return -1;
            }
            else
            {
                conn->Close();
            }
        }
        else if (subcmd == "setname")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            m_clients_holder.SetName(ctx.conn, cmd.GetArguments()[1]);
            fill_status_reply(ctx.reply, "OK");
            return 0;
        }
        else if (subcmd == "getname")
        {
            if (cmd.GetArguments().size() != 1)
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            fill_str_reply(ctx.reply, m_clients_holder.GetName(ctx.conn));
        }
        else if (subcmd == "list")
        {
            if (cmd.GetArguments().size() != 1)
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            m_clients_holder.List(ctx.reply);
        }
        else if (subcmd == "stat")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            if (!strcasecmp(cmd.GetArguments()[1].c_str(), "on"))
            {
                m_clients_holder.SetStatEnable(true);
            }
            else if (!strcasecmp(cmd.GetArguments()[1].c_str(), "off"))
            {
                m_clients_holder.SetStatEnable(false);
            }
            else
            {
                fill_error_reply(ctx.reply,
                        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
                return 0;
            }
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "ERR CLIENT subcommand must be one of LIST, GETNAME, SETNAME, KILL, STAT");
        }
        return 0;
    }

    int ArdbServer::SlowLog(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string subcmd = string_tolower(cmd.GetArguments()[0]);
        if (subcmd == "len")
        {
            fill_int_reply(ctx.reply, m_slowlog_handler.Size());
        }
        else if (subcmd == "reset")
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else if (subcmd == "get")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply, "ERR Wrong number of arguments for SLOWLOG GET");
            }
            uint32 len = 0;
            if (!string_touint32(cmd.GetArguments()[1], len))
            {
                fill_error_reply(ctx.reply, "ERR value is not an integer or out of range.");
                return 0;
            }
            m_slowlog_handler.GetSlowlog(len, ctx.reply);
        }
        else
        {
            fill_error_reply(ctx.reply, "ERR SLOWLOG subcommand must be one of GET, LEN, RESET");
        }
        return 0;
    }

    int ArdbServer::Config(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string arg0 = string_tolower(cmd.GetArguments()[0]);
        if (arg0 != "get" && arg0 != "set" && arg0 != "resetstat")
        {
            fill_error_reply(ctx.reply, "ERR CONFIG subcommand must be one of GET, SET, RESETSTAT");
            return 0;
        }
        if (arg0 == "resetstat")
        {
            if (cmd.GetArguments().size() != 1)
            {
                fill_error_reply(ctx.reply, "ERR Wrong number of arguments for CONFIG RESETSTAT");
                return 0;
            }
        }
        else if (arg0 == "get")
        {
            if (cmd.GetArguments().size() != 2)
            {
                fill_error_reply(ctx.reply, "ERR Wrong number of arguments for CONFIG GET");
                return 0;
            }
            ctx.reply.type = REDIS_REPLY_ARRAY;
            Properties::iterator it = m_cfg_props.begin();
            while (it != m_cfg_props.end())
            {
                if (fnmatch(cmd.GetArguments()[1].c_str(), it->first.c_str(), 0) == 0)
                {
                    ctx.reply.elements.push_back(RedisReply(it->first));
                    ctx.reply.elements.push_back(RedisReply(it->second));
                }
                it++;
            }
        }
        else if (arg0 == "set")
        {
            if (cmd.GetArguments().size() != 3)
            {
                fill_error_reply(ctx.reply, "RR Wrong number of arguments for CONFIG SET");
                return 0;
            }
            m_cfg_props[cmd.GetArguments()[1]] = cmd.GetArguments()[2];
            ParseConfig(m_cfg_props, m_cfg);
            fill_status_reply(ctx.reply, "OK");
        }
        return 0;
    }

    int ArdbServer::Time(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint64 micros = get_current_epoch_micros();
        ValueDataArray vs;
        vs.push_back(ValueData((int64) micros / 1000000));
        vs.push_back(ValueData((int64) micros % 1000000));
        fill_array_reply(ctx.reply, vs);
        return 0;
    }
    int ArdbServer::FlushDB(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->FlushDB(ctx.currentDB);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::FlushAll(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->FlushAll();
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::Shutdown(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_service->Stop();
        return -1;
    }

    int ArdbServer::CompactDB(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->CompactDB(ctx.currentDB);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::CompactAll(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->CompactAll();
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    /*
     *  SlaveOf host port [include 1|2|3] [exclude 0|1|2]
     *  Slaveof no one
     */
    int ArdbServer::Slaveof(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "ERR not enough arguments for slaveof.");
            return 0;
        }
        const std::string& host = cmd.GetArguments()[0];
        uint32 port = 0;
        if (!string_touint32(cmd.GetArguments()[1], port))
        {
            if (!strcasecmp(cmd.GetArguments()[0].c_str(), "no") && !strcasecmp(cmd.GetArguments()[1].c_str(), "one"))
            {
                fill_status_reply(ctx.reply, "OK");
                m_slave_client.Stop();
                m_cfg.master_host = "";
                m_cfg.master_port = 0;
                return 0;
            }
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
        {
            if (cmd.GetArguments()[i] == "include")
            {
                DBIDArray ids;
                split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                m_slave_client.SetIncludeDBs(ids);
            }
            else if (cmd.GetArguments()[i] == "exclude")
            {
                DBIDArray ids;
                split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                m_slave_client.SetExcludeDBs(ids);
            }
        }
        m_cfg.master_host = host;
        m_cfg.master_port = port;
        m_slave_client.ConnectMaster(host, port);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::ReplConf(ArdbConnContext& ctx, RedisCommandFrame& cmd)
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
                Address* addr = const_cast<Address*>(ctx.conn->GetRemoteAddress());
                if (InstanceOf<SocketHostAddress>(addr).OK)
                {
                    SocketHostAddress* tmp = (SocketHostAddress*) addr;
                    const SocketHostAddress& master_addr = m_slave_client.GetMasterAddress();
                    if (master_addr.GetHost() == tmp->GetHost() && master_addr.GetPort() == port)
                    {
                        ERROR_LOG("Can NOT accept this slave connection from master[%s:%u]",
                                master_addr.GetHost().c_str(), master_addr.GetPort());
                        fill_error_reply(ctx.reply, "ERR Reject connection as slave from master instance.");
                        return -1;
                    }
                }
                m_master_serv.AddSlavePort(ctx.conn, port);
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "ack"))
            {

            }
        }
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::Sync(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (!m_repl_backlog.IsInited())
        {
            fill_error_reply(ctx.reply, "ERR Ardb instance's replication log not enabled, can NOT serve as master.");
            return -1;
        }
        m_master_serv.AddSlave(ctx.conn, cmd);
        return 0;
    }

    /*
     *
     */
    int ArdbServer::PSync(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (!m_repl_backlog.IsInited())
        {
            fill_error_reply(ctx.reply,
                    "ERR Ardb instance's replication backlog not enabled, can NOT serve as master.");
            return -1;
        }
        m_master_serv.AddSlave(ctx.conn, cmd);
        return 0;
    }

    struct IdleConn
    {
            uint32 conn_id;
            uint64 ts;
    };

    struct IdleConnManager: public Runnable
    {
            int32 timer_id;
            LRUCache<uint32, IdleConn> lru;
            ChannelService* serv;
            uint32 maxIdleTime;
            IdleConnManager() :
                    timer_id(-1), serv(NULL), maxIdleTime(10000000)
            {
            }
            void Run()
            {
                LRUCache<uint32, IdleConn>::CacheEntry conn;
                uint64 now = get_current_epoch_millis();
                while (lru.PeekFront(conn) && (now - conn.second.ts >= maxIdleTime))
                {
                    Channel* ch = serv->GetChannel(conn.second.conn_id);
                    if (NULL != ch)
                    {
                        DEBUG_LOG("Closed timeout connection:%d.", ch->GetID());
                        ch->Close();
                    }
                    lru.PopFront();
                }
            }
    };

    void ArdbServer::TouchIdleConn(Channel* ch)
    {
        static ThreadLocal<IdleConnManager> kLocalIdleConns;
        uint64 now = get_current_epoch_millis();
        IdleConn conn;
        conn.conn_id = ch->GetID();
        conn.ts = now;

        IdleConnManager& m = kLocalIdleConns.GetValue();
        if (m.timer_id == -1)
        {
            m.serv = &(ch->GetService());
            m.maxIdleTime = m_cfg.timeout * 1000;
            m.lru.SetMaxCacheSize(m_cfg.max_clients);
            m.timer_id = ch->GetService().GetTimer().Schedule(&m, 1, 5, SECONDS);
        }
        LRUCache<uint32, IdleConn>::CacheEntry tmp;
        m.lru.Insert(conn.conn_id, conn, tmp);
    }

    void ClientConnHolder::ChangeCurrentDB(Channel* conn, const DBID& dbid)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        m_conn_table[conn->GetID()].currentDB = dbid;
    }

    void ClientConnHolder::TouchConn(Channel* conn, const std::string& currentCmd)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        ArdbConncetionTable::iterator found = m_conn_table.find(conn->GetID());
        if (found != m_conn_table.end())
        {
            found->second.lastCmd = currentCmd;
            found->second.lastTs = time(NULL);
        }
        else
        {
            ArdbConncetion c;
            c.conn = conn;
            c.birth = time(NULL);
            c.lastTs = time(NULL);
            c.lastCmd = currentCmd;
            const Address* remote = conn->GetRemoteAddress();
            if (InstanceOf<SocketUnixAddress>(remote).OK)
            {
                const SocketUnixAddress* addr = (const SocketUnixAddress*) remote;
                c.addr = addr->GetPath();
            }
            else if (InstanceOf<SocketHostAddress>(remote).OK)
            {
                const SocketHostAddress* addr = (const SocketHostAddress*) remote;
                char tmp[256];
                sprintf(tmp, "%s:%u", addr->GetHost().c_str(), addr->GetPort());
                c.addr = tmp;
            }
            m_conn_table.insert(ArdbConncetionTable::value_type(conn->GetID(), c));
        }
    }

    Channel* ClientConnHolder::GetConn(const std::string& addr)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        ArdbConncetionTable::iterator it = m_conn_table.begin();
        while (it != m_conn_table.end())
        {
            if (it->second.addr == addr)
            {
                return it->second.conn;
            }
            it++;
        }
        return NULL;
    }
    void ClientConnHolder::SetName(Channel* conn, const std::string& name)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        m_conn_table[conn->GetID()].name = name;
    }
    const std::string& ClientConnHolder::GetName(Channel* conn)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        return m_conn_table[conn->GetID()].name;
    }
    void ClientConnHolder::List(RedisReply& reply)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        reply.type = REDIS_REPLY_STRING;
        std::stringstream ss(std::stringstream::in | std::stringstream::out);
        ArdbConncetionTable::iterator it = m_conn_table.begin();
        time_t now = time(NULL);
        while (it != m_conn_table.end())
        {
            ArdbConncetion& c = it->second;
            ss << "addr=" << c.addr << " fd=" << c.conn->GetReadFD();
            ss << " name=" << c.name << " age=" << now - c.birth;
            ss << " idle=" << now - c.lastTs << " db=" << c.currentDB;
            ss << " cmd=" << c.lastCmd << "\r\n";
            it++;
        }
        reply.str = ss.str();
    }

}


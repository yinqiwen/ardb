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

#include "slave.hpp"
#include "ardb_server.hpp"
#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define ARDB_SLAVE_SYNC_STATE_MMAP_FILE_SIZE 512

namespace ardb
{
    Slave::Slave(ArdbServer* serv) :
            m_serv(serv), m_client(NULL), m_slave_state(
            SLAVE_STATE_CLOSED), m_cron_inited(false), m_ping_recved_time(0), m_server_type(
            ARDB_DB_SERVER_TYPE), m_server_support_psync(false), m_actx(
            NULL), m_rdb(NULL), m_backlog(serv->m_repl_backlog), m_routine_ts(0)
    {
    }

    bool Slave::Init()
    {
        InitCron();
        return 0;
    }

    ArdbConnContext* Slave::GetArdbConnContext()
    {
        if (NULL == m_actx)
        {
            NEW(m_actx, ArdbConnContext);
            if (m_backlog.GetCurrentDBID() != ARDB_GLOBAL_DB)
            {
                m_actx->currentDB = m_backlog.GetCurrentDBID();
            }
        }
        return m_actx;
    }

    RedisDumpFile* Slave::GetNewRedisDumpFile()
    {
        DELETE(m_rdb);
        std::string dump_file_path = m_serv->m_cfg.home + "/repl";
        char tmp[dump_file_path.size() + 100];
        uint32 now = time(NULL);
        sprintf(tmp, "%s/temp-%u-%u.rdb", dump_file_path.c_str(), getpid(), now);
        NEW(m_rdb, RedisDumpFile(m_serv->m_db, tmp));
        INFO_LOG("[Slave]Create redis dump file:%s", tmp);
        return m_rdb;
    }

    void Slave::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        DEBUG_LOG("Master conn connected.");
        Buffer info;
        info.Printf("info Server\r\n");
        ctx.GetChannel()->Write(info);
        m_slave_state = SLAVE_STATE_WAITING_INFO_REPLY;
        m_ping_recved_time = time(NULL);
    }

    void Slave::HandleRedisCommand(Channel* ch, RedisCommandFrame& cmd)
    {
        int flag = ARDB_PROCESS_REPL_WRITE;
        if (m_slave_state == SLAVE_STATE_SYNCED || m_slave_state == SLAVE_STATE_LOADING_DUMP_DATA)
        {
            flag |= ARDB_PROCESS_FORCE_REPLICATION;
        }
        else
        {
            flag |= ARDB_PROCESS_WITHOUT_REPLICATION;
        }
        DEBUG_LOG("Recv master cmd %s at %lld at flag:%d at state:%d", cmd.ToString().c_str(),
                m_backlog.GetReplEndOffset(), flag, m_slave_state);
        if (m_slave_state == SLAVE_STATE_WAITING_PSYNC_REPLY && m_server_type == ARDB_DB_SERVER_TYPE)
        {
            if (!strcasecmp(cmd.GetCommand().c_str(), "FULLSYNCED"))
            {
                uint64 offset, cksm;
                string_touint64(cmd.GetArguments()[0], offset);
                string_touint64(cmd.GetArguments()[1], cksm);
                m_backlog.SetChecksum(cksm);
                ASSERT(offset == m_backlog.GetReplEndOffset());
                //m_backlog.SetReplOffset(offset);
                m_slave_state = SLAVE_STATE_SYNCED;
                //Disconnect all slaves when all data resynced
                m_serv->m_master_serv.DisconnectAllSlaves();
                return;
            }
        }

        if (!strcasecmp(cmd.GetCommand().c_str(), "PING"))
        {
            m_ping_recved_time = time(NULL);
        }
        else if (!strcasecmp(cmd.GetCommand().c_str(), "SELECT"))
        {
            DBID id = 0;
            string_touint32(cmd.GetArguments()[0], id);
            m_backlog.SetCurrentDBID(id);
        }
        GetArdbConnContext();
        m_actx->is_slave_conn = true;
        m_actx->conn = ch;

        if (strcasecmp(cmd.GetCommand().c_str(), "SELECT") && strcasecmp(cmd.GetCommand().c_str(), "__SET__")
                && strcasecmp(cmd.GetCommand().c_str(), "__DEL__"))
        {
            if (!m_include_dbs.empty() && m_include_dbs.count(m_actx->currentDB) == 0)
            {
                flag |= ARDB_PROCESS_FEED_REPLICATION_ONLY;
            }
            if (!m_exclude_dbs.empty() && m_exclude_dbs.count(m_actx->currentDB) > 0)
            {
                flag |= ARDB_PROCESS_FEED_REPLICATION_ONLY;
            }
        }
        m_serv->ProcessRedisCommand(*m_actx, cmd, flag);
    }
    void Slave::Routine()
    {
        uint32 now = time(NULL);
        if (m_slave_state == SLAVE_STATE_SYNCED)
        {
            if (m_ping_recved_time > 0 && now - m_ping_recved_time >= m_serv->m_cfg.repl_timeout)
            {
                if (m_slave_state == SLAVE_STATE_SYNCED)
                {
                    INFO_LOG("now = %u, ping_recved_time=%u", now, m_ping_recved_time);
                    Timeout();
                    return;
                }
            }
        }
        if (m_slave_state == SLAVE_STATE_SYNCED || m_slave_state == SLAVE_STATE_LOADING_DUMP_DATA)
        {
            if (m_server_support_psync)
            {
                Buffer ack;
                ack.Printf("REPLCONF ACK %lld\r\n", m_backlog.GetReplEndOffset());
                m_client->Write(ack);
            }
        }
        m_routine_ts = now;
    }

    void Slave::LoadRDBRoutine(void* cb)
    {
        Slave* slave = (Slave*) cb;
        if (NULL != slave->m_client)
        {
            slave->m_client->GetService().Continue();
        }
        if (time(NULL) - slave->m_routine_ts > 10)
        {
            slave->Routine();
        }
    }
    void Slave::HandleRedisReply(Channel* ch, RedisReply& reply)
    {
        switch (m_slave_state)
        {
            case SLAVE_STATE_WAITING_INFO_REPLY:
            {
                if (reply.type == REDIS_REPLY_ERROR)
                {
                    ERROR_LOG("Recv info reply error:%s", reply.str.c_str());
                    Close();
                    return;
                }
                const char* redis_ver_key = "redis_version:";
                if (reply.str.find(redis_ver_key) != std::string::npos)
                {
                    m_server_type = REDIS_DB_SERVER_TYPE;
                    size_t start = reply.str.find(redis_ver_key);
                    size_t end = reply.str.find("\n", start);
                    std::string v = reply.str.substr(start + strlen(redis_ver_key),
                            end - start - strlen(redis_ver_key));
                    v = trim_string(v);
                    m_server_support_psync = (compare_version<3>(v, "2.7.0") >= 0);
                    INFO_LOG("[Slave]Remote master is a Redis %s instance, support partial sync:%u", v.c_str(),
                            m_server_support_psync);

                }
                else
                {
                    INFO_LOG("[Slave]Remote master is an Ardb instance.");
                    m_server_type = ARDB_DB_SERVER_TYPE;
                    m_server_support_psync = true;
                }
                Buffer replconf;
                replconf.Printf("replconf listening-port %u\r\n", m_serv->GetServerConfig().listen_port);
                ch->Write(replconf);
                m_slave_state = SLAVE_STATE_WAITING_REPLCONF_REPLY;
                break;
            }
            case SLAVE_STATE_WAITING_REPLCONF_REPLY:
            {
                if (reply.type == REDIS_REPLY_ERROR)
                {
                    ERROR_LOG("Recv replconf reply error:%s", reply.str.c_str());
                    ch->Close();
                    return;
                }
                if (m_server_support_psync)
                {
                    Buffer sync;
                    if (m_server_type == ARDB_DB_SERVER_TYPE)
                    {
                        std::string includes = "*";
                        std::string excludes = "-";
                        if (!m_include_dbs.empty())
                        {
                            includes = string_join_container(m_include_dbs, "|");
                        }
                        if (!m_exclude_dbs.empty())
                        {
                            excludes = string_join_container(m_exclude_dbs, "|");
                        }
                        sync.Printf("apsync %s %lld cksm %llu include %s exclude %s\r\n", m_backlog.GetServerKey(),
                                m_backlog.GetReplEndOffset(), m_backlog.GetChecksum(), includes.c_str(),
                                excludes.c_str());
                    }
                    else
                    {
                        sync.Printf("psync %s %lld\r\n", m_backlog.GetServerKey(), m_backlog.GetReplEndOffset());
                        INFO_LOG("Send PSYNC %s %lld", m_backlog.GetServerKey(), m_backlog.GetReplEndOffset());
                    }
                    ch->Write(sync);
                    m_slave_state = SLAVE_STATE_WAITING_PSYNC_REPLY;
                }
                else
                {
                    Buffer sync;
                    sync.Printf("sync\r\n");
                    ch->Write(sync);
                    m_slave_state = SLAVE_STATE_SYNING_DUMP_DATA;
                    m_decoder.SwitchToDumpFileDecoder();
                }
                break;
            }
            case SLAVE_STATE_WAITING_PSYNC_REPLY:
            {
                if (reply.type != REDIS_REPLY_STATUS)
                {
                    ERROR_LOG("Recv psync reply error:%s", reply.str.c_str());
                    ch->Close();
                    return;
                }
                INFO_LOG("Recv psync reply:%s", reply.str.c_str());
                std::vector<std::string> ss = split_string(reply.str, " ");
                if (!strcasecmp(ss[0].c_str(), "FULLRESYNC"))
                {
                    int64 offset;
                    if (!string_toint64(ss[2], offset))
                    {
                        ERROR_LOG("Invalid psync offset:%s", ss[2].c_str());
                        ch->Close();
                        return;
                    }
                    /*
                     * Delete all data before receiving resyncing data
                     */
                    if (m_serv->m_cfg.slave_cleardb_before_fullresync)
                    {
                        m_serv->m_db->FlushAll();
                    }
                    m_backlog.SetServerkey(ss[1]);
                    m_backlog.SetReplOffset(offset);
                    if (m_server_type == ARDB_DB_SERVER_TYPE)
                    {
                        //Do NOT change state, since master would send  "FULLSYNCED" after all data synced
                        m_decoder.SwitchToCommandDecoder();
                        return;
                    }
                }
                else if (!strcasecmp(ss[0].c_str(), "CONTINUE"))
                {
                    m_decoder.SwitchToCommandDecoder();
                    m_slave_state = SLAVE_STATE_SYNCED;
                    break;
                }
                else
                {
                    ERROR_LOG("Invalid psync status:%s", reply.str.c_str());
                    ch->Close();
                    return;
                }
                m_slave_state = SLAVE_STATE_SYNING_DUMP_DATA;
                m_decoder.SwitchToDumpFileDecoder();
                break;
            }
            default:
            {
                ERROR_LOG("Slave client is in invalid state:%d", m_slave_state);
                Close();
                break;
            }
        }
    }

    void Slave::HandleRedisDumpChunk(Channel* ch, RedisDumpFileChunk& chunk)
    {
        if (m_slave_state != SLAVE_STATE_SYNING_DUMP_DATA)
        {
            ERROR_LOG("Invalid state:%u to handler redis dump file chunk.", m_slave_state);
            ch->Close();
            return;
        }
        if (chunk.IsFirstChunk())
        {
            GetNewRedisDumpFile();
        }
        if (!chunk.chunk.empty())
        {
            m_rdb->Write(chunk.chunk.c_str(), chunk.chunk.size());
        }
        if (chunk.IsLastChunk())
        {
            m_rdb->Flush();
            m_decoder.SwitchToCommandDecoder();
            m_slave_state = SLAVE_STATE_LOADING_DUMP_DATA;
            if (m_serv->m_cfg.slave_cleardb_before_fullresync && !m_server_support_psync)
            {
                m_serv->m_db->FlushAll();
            }
            INFO_LOG("Start loading RDB dump file.");
            if (NULL != m_client)
            {
                m_client->DetachFD();
            }
            m_rdb->Load(Slave::LoadRDBRoutine, this);
            if (NULL != m_client)
            {
                m_client->AttachFD();
            }
            m_slave_state = SLAVE_STATE_SYNCED;
            m_rdb->Remove();
            DELETE(m_rdb);

            //Disconnect all slaves when all data resynced
            m_serv->m_master_serv.DisconnectAllSlaves();
        }
    }

    void Slave::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e)
    {
        if (e.GetMessage()->IsReply())
        {
            HandleRedisReply(ctx.GetChannel(), e.GetMessage()->reply);
        }
        else if (e.GetMessage()->IsCommand())
        {
            HandleRedisCommand(ctx.GetChannel(), e.GetMessage()->command);
        }
        else
        {
            HandleRedisDumpChunk(ctx.GetChannel(), e.GetMessage()->chunk);
        }
    }

    void Slave::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        m_client = NULL;
        m_slave_state = 0;
        DELETE(m_actx);
        m_slave_state = SLAVE_STATE_CLOSED;
        //reconnect master after 1000ms
        struct ReconnectTask: public Runnable
        {
                Slave& sc;
                ReconnectTask(Slave& ssc) :
                        sc(ssc)
                {
                }
                void Run()
                {
                    if (NULL == sc.m_client && !sc.m_master_addr.GetHost().empty())
                    {
                        sc.ConnectMaster(sc.m_master_addr.GetHost(), sc.m_master_addr.GetPort());
                    }
                }
        };
        m_serv->GetTimer().ScheduleHeapTask(new ReconnectTask(*this), 1000, -1, MILLIS);
    }

    void Slave::Timeout()
    {
        WARN_LOG("Master connection timeout.");
        Close();
    }

    void Slave::InitCron()
    {
        if (!m_cron_inited)
        {
            m_cron_inited = true;
            struct RoutineTask: public Runnable
            {
                    Slave* c;
                    RoutineTask(Slave* cc) :
                            c(cc)
                    {
                    }
                    void Run()
                    {
                        c->Routine();
                    }
            };
            m_serv->GetTimer().ScheduleHeapTask(new RoutineTask(this), 1, 1, SECONDS);
        }
    }

    int Slave::ConnectMaster(const std::string& host, uint32 port)
    {
        SocketHostAddress addr(host, port);
        if (m_master_addr == addr && NULL != m_client)
        {
            return 0;
        }
        m_master_addr = addr;
        Close();
        m_client = m_serv->m_service->NewClientSocketChannel();

        m_decoder.Clear();
        m_client->GetPipeline().AddLast("decoder", &m_decoder);
        m_client->GetPipeline().AddLast("encoder", &m_encoder);
        m_client->GetPipeline().AddLast("handler", this);
        m_decoder.SwitchToReplyDecoder();
        m_slave_state = SLAVE_STATE_CONNECTING;
        m_client->Connect(&m_master_addr);
        DEBUG_LOG("[Slave]Connecting master %s:%u", host.c_str(), port);
        return 0;
    }

    void Slave::Stop()
    {
        SocketHostAddress empty;
        m_master_addr = empty;
        m_slave_state = 0;
        Close();
    }
    void Slave::Close()
    {
        if (NULL != m_client)
        {
            m_client->Close();
            m_client = NULL;
        }
    }

    bool Slave::IsConnected()
    {
        return m_slave_state != SLAVE_STATE_CLOSED && m_slave_state != SLAVE_STATE_CONNECTING;
    }
    bool Slave::IsSynced()
    {
        return m_slave_state != SLAVE_STATE_SYNCED;
    }

    void Slave::SetIncludeDBs(const DBIDArray& dbs)
    {
        convert_vector_to_set(dbs, m_include_dbs);
    }
    void Slave::SetExcludeDBs(const DBIDArray& dbs)
    {
        convert_vector_to_set(dbs, m_exclude_dbs);
    }
}


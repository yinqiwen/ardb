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
#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include "ardb.hpp"

#define ARDB_SLAVE_SYNC_STATE_MMAP_FILE_SIZE 512

namespace ardb
{
    Slave::Slave(Ardb* serv) :
            m_serv(serv), m_client(NULL), m_slave_state(
            SLAVE_STATE_CLOSED), m_cron_inited(false), m_cmd_recved_time(0), m_master_link_down_time(0), m_server_type(
            ARDB_DB_SERVER_TYPE), m_server_support_psync(false), m_actx(
            NULL), m_rdb(NULL), m_backlog(serv->m_repl_backlog), m_routine_ts(0), m_cached_master_repl_offset(0), m_cached_master_repl_cksm(
                    0), m_lastinteraction(0)
    {
    }

    bool Slave::Init()
    {
        InitCron();
        return 0;
    }

    Context* Slave::GetArdbConnContext()
    {
        if (NULL == m_actx)
        {
            NEW(m_actx, Context);
            if (m_backlog.GetCurrentDBID() != ARDB_GLOBAL_DB)
            {
                m_actx->currentDB = m_backlog.GetCurrentDBID();
            }
        }
        return m_actx;
    }

    DataDumpFile* Slave::GetNewDumpFile()
    {
        DELETE(m_rdb);
        std::string dump_file_path = m_serv->m_cfg.home + "/repl";
        char tmp[dump_file_path.size() + 100];
        uint32 now = time(NULL);
        sprintf(tmp, "%s/temp-%u-%u.rdb", dump_file_path.c_str(), getpid(), now);
        if (m_server_type == ARDB_DB_SERVER_TYPE)
        {
            NEW(m_rdb, ArdbDumpFile);
        }
        else
        {
            NEW(m_rdb, RedisDumpFile);
        }
        m_rdb->Init(m_serv);
        m_rdb->OpenWriteFile(tmp);
        INFO_LOG("[Slave]Create dump file:%s, master is redis:%d", tmp, m_server_type != ARDB_DB_SERVER_TYPE);
        return m_rdb;
    }

    void Slave::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        DEBUG_LOG("Master conn connected.");
        Buffer info;
        info.Printf("info Server\r\n");
        ctx.GetChannel()->Write(info);
        m_slave_state = SLAVE_STATE_WAITING_INFO_REPLY;
        m_lastinteraction = m_cmd_recved_time = time(NULL);
        m_master_link_down_time = 0;
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
        m_cmd_recved_time = time(NULL);
        if (!strcasecmp(cmd.GetCommand().c_str(), "SELECT"))
        {
            DBID id = 0;
            string_touint32(cmd.GetArguments()[0], id);
            m_backlog.SetCurrentDBID(id);
        }
        GetArdbConnContext();
        m_actx->identity = CONTEXT_SLAVE_CONNECTION;
        m_actx->client = NULL;
        m_actx->server_address = MASTER_SERVER_ADDRESS_NAME;
        if (0 != strcasecmp(cmd.GetCommand().c_str(), "SELECT"))
        {
            if (!SupportDBID(m_actx->currentDB))
            {
                flag |= ARDB_PROCESS_FEED_REPLICATION_ONLY;
            }
        }
        m_serv->Call(*m_actx, cmd, flag);
    }
    void Slave::Routine()
    {
        uint32 now = time(NULL);
        if (m_slave_state == SLAVE_STATE_SYNCED)
        {
            if (m_cmd_recved_time > 0 && now - m_cmd_recved_time >= m_serv->m_cfg.repl_timeout)
            {
                if (m_slave_state == SLAVE_STATE_SYNCED)
                {
                    WARN_LOG("now = %u, ping_recved_time=%u", now, m_cmd_recved_time);
                    Timeout();
                    return;
                }
            }
        }
        if (m_slave_state == SLAVE_STATE_SYNCED || m_slave_state == SLAVE_STATE_LOADING_DUMP_DATA)
        {
            if (m_server_support_psync && NULL != m_client)
            {
                Buffer ack;
                ack.Printf("REPLCONF ACK %lld\r\n", m_backlog.GetReplEndOffset());
                m_client->Write(ack);
            }
        }
        m_routine_ts = now;
    }

    int Slave::LoadRDBRoutine(void* cb)
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
        return 0;
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
                //std::vector<std::string> ss = split_string(m_serv->GetServerConfig().listen_addresses[0], ":");
                replconf.Printf("replconf listening-port %u\r\n", m_serv->GetConfig().PrimayPort());
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
                        sync.Printf("apsync %s %lld cksm %llu\r\n", m_backlog.GetServerKey(),
                                m_backlog.GetReplEndOffset(), m_backlog.GetChecksum());
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
                        Context tmp;
                        m_serv->FlushAllData(tmp);
                    }
                    m_cached_master_runid = ss[1];
                    m_cached_master_repl_offset = offset;
                    /*
                     * if remote master is ardb, there would be a cksm part
                     */
                    if (ss.size() > 3)
                    {
                        uint64 cksm;
                        if (!string_touint64(ss[3], cksm))
                        {
                            ERROR_LOG("Invalid psync cksm:%s", ss[3].c_str());
                            ch->Close();
                            return;
                        }
                        m_cached_master_repl_cksm = cksm;
                    }
                    m_slave_state = SLAVE_STATE_SYNING_DUMP_DATA;
                    m_decoder.SwitchToDumpFileDecoder();
                    break;
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
            GetNewDumpFile()->SetExpectedDataSize(chunk.len);
        }
        if (!chunk.chunk.empty())
        {
            m_rdb->Write(chunk.chunk.c_str(), chunk.chunk.size());
        }
        if (chunk.IsLastChunk())
        {
            m_rdb->Flush();
            m_rdb->Rename();
            m_decoder.SwitchToCommandDecoder();
            m_slave_state = SLAVE_STATE_LOADING_DUMP_DATA;
            if (m_serv->m_cfg.slave_cleardb_before_fullresync && !m_server_support_psync)
            {
                Context tmp;
                m_serv->FlushAllData(tmp);
            }
            if(!m_server_support_psync)
            {
                m_backlog.SetCurrentDBID(0);
            }
            INFO_LOG("Start loading RDB dump file.");
            if (NULL != m_client)
            {
                /*
                 * Disable read on connection with master while loading data from dump file
                 */
                m_client->DetachFD();
            }
            int ret = m_rdb->Load(CONTEXT_DUMP_SYNC_LOADING, m_rdb->GetPath(), Slave::LoadRDBRoutine, this);
            if (NULL != m_client)
            {
                /*
                 * resume read
                 */
                m_client->AttachFD();
            }
            if(0 != ret)
            {
                m_client->Close();
                WARN_LOG("Failed to load RDB dump file.");
                return;
            }

            SwitchSyncedState();
            m_rdb->Close();
            //DELETE(m_rdb);
            //Disconnect all slaves when all data resynced
            m_serv->m_master.DisconnectAllSlaves();
        }
    }

    void Slave::SwitchSyncedState()
    {
        m_cmd_recved_time = time(NULL);
        m_slave_state = SLAVE_STATE_SYNCED;
        if(!m_cached_master_runid.empty())
        {
            m_backlog.SetServerkey(m_cached_master_runid);
            m_backlog.SetReplOffset(m_cached_master_repl_offset);
            m_backlog.SetChecksum(m_cached_master_repl_cksm);
        }
        m_backlog.Persist();
    }

    void Slave::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e)
    {
        m_lastinteraction = time(NULL);
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
        INFO_LOG("[Slave]Replication connection closed.");
        m_lastinteraction = m_master_link_down_time = time(NULL);
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
        m_client = m_serv->GetChannelService().NewClientSocketChannel();

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
        return m_slave_state == SLAVE_STATE_SYNCED;
    }

    bool Slave::IsSyncing()
    {
        return m_slave_state == SLAVE_STATE_SYNING_DUMP_DATA || m_slave_state == SLAVE_STATE_LOADING_DUMP_DATA;
    }
    uint32 Slave::GetState()
    {
        return m_slave_state;
    }

    void Slave::SetIncludeDBs(const DBIDArray& dbs)
    {
        convert_vector_to_set(dbs, m_include_dbs);
    }
    void Slave::SetExcludeDBs(const DBIDArray& dbs)
    {
        convert_vector_to_set(dbs, m_exclude_dbs);
    }
    bool Slave::SupportDBID(DBID id)
    {
        if (m_include_dbs.empty() && m_exclude_dbs.empty())
        {
            return true;
        }
        if (m_exclude_dbs.count(id) > 0)
        {
            return false;
        }
        if (m_include_dbs.count(id) == 0)
        {
            return false;
        }
        return true;
    }
    uint32 Slave::GetMasterLinkDownTime()
    {
        return m_master_link_down_time;
    }
    time_t Slave::GetMasterLastinteractionTime()
    {
        return m_lastinteraction;
    }
    int64 Slave::SyncLeftBytes()
    {
        if (NULL != m_rdb)
        {
            return m_rdb->DumpLeftDataSize();
        }
        return 0;
    }
    int64 Slave::LoadingLeftBytes()
    {
        if (NULL != m_rdb)
        {
            return m_rdb->ProcessLeftDataSize();
        }
        return 0;
    }
}


/*
 * master.cpp
 *
 *  Created on: 2013-08-22
 *      Author: yinqiwen
 */
#include "master.hpp"
#include "rdb.hpp"
#include "ardb.hpp"
#include <fcntl.h>
#include <sys/stat.h>

#define REPL_INSTRUCTION_ADD_SLAVE 0
#define REPL_INSTRUCTION_FEED_CMD 1
#define REPL_INSTRUCTION_DELETE_SLAVES 2

#define SOFT_SIGNAL_REPL_INSTRUCTION 1

#define MAX_SEND_CACHE_SIZE 4096

namespace ardb
{
    struct RedisReplCommand
    {
            RedisCommandFrame cmd;
            DBID dbid;
            RedisReplCommand() :
                    dbid(0)
            {
            }
    };

    static Master* g_master = NULL;

    /*
     * Master
     */
    Master::Master() :
            m_dumping_rdb(false), m_dumping_ardb(false), m_dump_rdb_offset(-1), m_dump_ardb_offset(-1), m_repl_no_slaves_since(
                    0), m_backlog_enable(true)
    {
        g_master = this;
    }

    int Master::Init()
    {
        Start();
        return 0;
    }

    void Master::Run()
    {
        struct HeartbeatTask: public Runnable
        {
                Master* serv;
                HeartbeatTask(Master* s) :
                        serv(s)
                {
                }
                void Run()
                {
                    serv->OnHeartbeat();
                }
        };

        m_channel_service.GetTimer().ScheduleHeapTask(new HeartbeatTask(this), g_db->GetConfig().repl_ping_slave_period,
                g_db->GetConfig().repl_ping_slave_period, SECONDS);
        m_channel_service.Start();
    }

    void Master::OnHeartbeat()
    {
        //Just process instructions  since the soft signal may loss
        if (!g_db->GetConfig().master_host.empty())
        {
            //let master feed 'ping' command
            return;
        }
        if (m_slave_table.empty())
        {
            if (0 == m_repl_no_slaves_since)
            {
                m_repl_no_slaves_since = time(NULL);
            }
            else if (m_backlog_enable)
            {
                if (g_db->GetConfig().repl_backlog_time_limit > 0)
                {
                    time_t now = time(NULL);
                    if (now - m_repl_no_slaves_since >= g_db->GetConfig().repl_backlog_time_limit)
                    {
                        m_backlog_enable = false;
                    }
                }
            }
            else
            {
                return;
            }
        }

        if (!m_slave_table.empty())
        {
            m_repl_no_slaves_since = 0;
            m_backlog_enable = true;

            RedisCommandFrame ping;
            ping.SetCommand("ping");
            ping.SetType(REDIS_CMD_PING);
            WriteSlaves(0, ping);
            DEBUG_LOG("Ping slaves.");
        }
    }

    void Master::WriteCmdToSlaves(const RedisCommandFrame& cmd)
    {
        DEBUG_LOG("[Master]Write command:%s to slave.", cmd.ToString().c_str());
        Buffer buffer(256);
        RedisCommandEncoder::Encode(buffer, cmd);
        g_db->m_repl_backlog.Feed(buffer);

        std::vector<Channel*> closing_slaves;
        SlaveConnTable::iterator it = m_slave_table.begin();
        for (; it != m_slave_table.end(); it++)
        {
            if (it->second->state == SLAVE_STATE_SYNCED)
            {
                buffer.SetReadIndex(0);
                if (!it->second->conn->Write(buffer))
                {
                    ERROR_LOG("Failed to write command to slave, we'll close it later.");
                    it->second->state = SLAVE_STATE_CLOSING;
                    closing_slaves.push_back(it->second->conn);
                }
            }
        }
        for (uint32 i = 0; i < closing_slaves.size(); i++)
        {
            closing_slaves[i]->Close();
        }
    }

    void Master::WriteSlaves(DBID db, const RedisCommandFrame& cmd)
    {
        switch (cmd.GetType())
        {
            case REDIS_CMD_SELECT:
            {
                DBID id = 0;
                string_touint32(cmd.GetArguments()[0], id);
                g_db->m_repl_backlog.SetCurrentDBID(id);
                break;
            }
            case REDIS_CMD_PING:
            {
                break;
            }
            default:
            {
                if (g_db->m_repl_backlog.GetCurrentDBID() != db)
                {
                    if (!g_db->GetConfig().master_host.empty())
                    {
                        ERROR_LOG(
                                "Can NOT happen since slave instance can NOT generate select command for cmd:%s to switch DB from %u to %u",
                                cmd.GetCommand().c_str(), g_db->m_repl_backlog.GetCurrentDBID(), db);
                    }
                    else
                    {
                        RedisCommandFrame select;
                        select.SetFullCommand("select %u", db);
                        select.SetType(REDIS_CMD_SELECT);
                        g_db->m_repl_backlog.SetCurrentDBID(db);
                        WriteCmdToSlaves(select);
                    }
                }
                break;
            }
        }
        WriteCmdToSlaves(cmd);
    }

    static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
    {
        Master* master = g_master;
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisCommandEncoder);
        pipeline->AddLast("handler", master);
    }

    static void slave_pipeline_finallize(ChannelPipeline* pipeline, void* data)
    {
        ChannelHandler* handler = pipeline->Get("decoder");
        DELETE(handler);
        handler = pipeline->Get("encoder");
        DELETE(handler);
    }

    int Master::DumpRDBRoutine(void* cb)
    {
        Master* m = (Master*) cb;
        SlaveConnTable::iterator fit = m->m_slave_table.begin();
        int waiting_dump_slave_count = 0;
        while (fit != m->m_slave_table.end())
        {
            SlaveConnection* slave = fit->second;
            if (slave->state == SLAVE_STATE_WAITING_DUMP_DATA)
            {
                Buffer newline;
                newline.Write("\n", 1);
                slave->conn->Write(newline);
                waiting_dump_slave_count++;
            }
            fit++;
        }
        m->m_channel_service.Continue();
        //no waiting slave, just abort dump process
        return waiting_dump_slave_count > 0 ? 0 : -1;
    }

    void Master::FullResyncArdbSlave(SlaveConnection& slave)
    {
        std::string dump_file_path = g_db->GetConfig().repl_data_dir + "/dump.ardb";
        slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
        if (m_dumping_ardb)
        {
            slave.sync_offset = m_dump_ardb_offset;
            return;
        }
        INFO_LOG("[Master]Start dump ardb data to file:%s", dump_file_path.c_str());
        m_dumping_ardb = true;
        m_dump_ardb_offset = g_db->m_repl_backlog.GetReplEndOffset();
        slave.repl_dbid_before_sync = g_db->m_repl_backlog.GetCurrentDBID();
        slave.repl_chsm_before_sync = g_db->m_repl_backlog.GetChecksum();
        slave.sync_offset = m_dump_rdb_offset;

        //force master generate 'select' later
        g_db->m_repl_backlog.SetCurrentDBID(ARDB_GLOBAL_DB);
        ArdbDumpFile rdb;
        rdb.Init(g_db);
        int ret = rdb.Save(dump_file_path, DumpRDBRoutine, this);
        if (0 == ret)
        {
            INFO_LOG("[REPL]Saved ardb dump file:%s", dump_file_path.c_str());
            OnArdbDumpComplete();
        }
        else
        {
            WARN_LOG("Failed to dump file because all slave closed.");
            m_dumping_ardb = false;
            m_dump_ardb_offset = -1;
        }
    }

    void Master::FullResyncRedisSlave(SlaveConnection& slave)
    {
        std::string dump_file_path = g_db->GetConfig().repl_data_dir + "/dump.rdb";
        slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
        if (m_dumping_rdb)
        {
            slave.sync_offset = m_dump_rdb_offset;
            return;
        }
        INFO_LOG("[Master]Start dump data to file:%s", dump_file_path.c_str());
        m_dumping_rdb = true;
        m_dump_rdb_offset = g_db->m_repl_backlog.GetReplEndOffset();
        slave.sync_offset = m_dump_rdb_offset;

        //force master generate 'select' later
        g_db->m_repl_backlog.SetCurrentDBID(ARDB_GLOBAL_DB);
        RedisDumpFile rdb;
        rdb.Init(g_db);
        int ret = rdb.Save(dump_file_path, DumpRDBRoutine, this);
        if (0 == ret)
        {
            INFO_LOG("[REPL]Saved rdb dump file:%s", dump_file_path.c_str());
            OnRedisDumpComplete();
        }
        else
        {
            WARN_LOG("Failed to dump file because all slave closed.");
            m_dumping_rdb = false;
            m_dump_rdb_offset = -1;
        }
    }

    void Master::OnRedisDumpComplete()
    {
        SlaveConnTable::iterator it = m_slave_table.begin();
        while (it != m_slave_table.end())
        {
            if (it->second->state == SLAVE_STATE_WAITING_DUMP_DATA && it->second->isRedisSlave)
            {
                SendRedisDumpToSlave(*(it->second));
            }
            it++;
        }
        m_dumping_rdb = false;
    }
    void Master::OnArdbDumpComplete()
    {
        SlaveConnTable::iterator it = m_slave_table.begin();
        while (it != m_slave_table.end())
        {
            if (it->second->state == SLAVE_STATE_WAITING_DUMP_DATA && !it->second->isRedisSlave)
            {
                SendArdbDumpToSlave(*(it->second));
            }
            it++;
        }
        m_dumping_ardb = false;
    }

    void Master::OnDumpFileSendComplete(void* data)
    {
        std::pair<void*, void*>* p = (std::pair<void*, void*>*) data;
        Master* master = (Master*) p->first;
        SlaveConnection* slave = (SlaveConnection*) p->second;
        DELETE(p);
        close(slave->repldbfd);
        slave->repldbfd = -1;
        INFO_LOG("Send complete.");
        master->SendCacheToSlave(*slave);
    }

    void Master::OnDumpFileSendFailure(void* data)
    {
        std::pair<void*, void*>* p = (std::pair<void*, void*>*) data;
        //Master* master = (Master*) p->first;
        SlaveConnection* slave = (SlaveConnection*) p->second;
        DELETE(p);
        close(slave->repldbfd);
        slave->repldbfd = -1;
    }

    void Master::SendCacheToSlave(SlaveConnection& slave)
    {
        slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
        slave.conn->EnableWriting();
        INFO_LOG("[Master]Slave request offset: %lld", slave.sync_offset);
    }

    void Master::SendRedisDumpToSlave(SlaveConnection& slave)
    {
        INFO_LOG("[REPL]Send dump file to slave");
        slave.state = SLAVE_STATE_SYNING_DUMP_DATA;
        std::string dump_file_path = g_db->GetConfig().repl_data_dir + "/dump.rdb";
        SendFileSetting setting;
        setting.fd = open(dump_file_path.c_str(), O_RDONLY);
        if (-1 == setting.fd)
        {
            int err = errno;
            ERROR_LOG("Failed to open file:%s for reason:%s", dump_file_path.c_str(), strerror(err));
            slave.conn->Close();
            return;
        }
        setting.file_offset = 0;
        struct stat st;
        fstat(setting.fd, &st);
        Buffer header;
        header.Printf("$%llu\r\n", st.st_size);
        slave.conn->Write(header);

        setting.file_rest_len = st.st_size;
        setting.on_complete = OnDumpFileSendComplete;
        setting.on_failure = OnDumpFileSendFailure;
        std::pair<void*, void*>* cb = new std::pair<void*, void*>;
        cb->first = this;
        cb->second = &slave;
        setting.data = cb;
        slave.repldbfd = setting.fd;
        slave.conn->SendFile(setting);
    }

    void Master::SendArdbDumpToSlave(SlaveConnection& slave)
    {
        INFO_LOG("[REPL]Send ardb dump file to slave");
        slave.state = SLAVE_STATE_SYNING_DUMP_DATA;
        std::string dump_file_path = g_db->GetConfig().repl_data_dir + "/dump.ardb";
        SendFileSetting setting;
        setting.fd = open(dump_file_path.c_str(), O_RDONLY);
        if (-1 == setting.fd)
        {
            int err = errno;
            ERROR_LOG("Failed to open file:%s for reason:%s", dump_file_path.c_str(), strerror(err));
            slave.conn->Close();
            return;
        }
        setting.file_offset = 0;
        struct stat st;
        fstat(setting.fd, &st);
        Buffer header;
        header.Printf("$%llu\r\n", st.st_size);
        slave.conn->Write(header);

        setting.file_rest_len = st.st_size;
        setting.on_complete = OnDumpFileSendComplete;
        setting.on_failure = OnDumpFileSendFailure;
        std::pair<void*, void*>* cb = new std::pair<void*, void*>;
        cb->first = this;
        cb->second = &slave;
        setting.data = cb;
        slave.repldbfd = setting.fd;
        slave.conn->SendFile(setting);
    }

    void Master::SyncSlave(SlaveConnection& slave)
    {
        if (slave.server_key.empty())
        {
            //redis 2.6/2.4
            slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
        }
        else
        {
            Buffer msg;
            if (g_db->m_repl_backlog.IsValidOffset(slave.server_key, slave.sync_offset))
            {
                msg.Printf("+CONTINUE\r\n");
                slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
            }
            else
            {
                //FULLRESYNC
                if ((slave.isRedisSlave && !m_dumping_rdb) || (!slave.isRedisSlave && !m_dumping_ardb))
                {
                    slave.syncing_from = g_db->m_repl_backlog.GetCurrentDBID();
                }
                uint64 offset = g_db->m_repl_backlog.GetReplEndOffset();
                if (slave.syncing_from != ARDB_GLOBAL_DB && slave.isRedisSlave)
                {
                    RedisCommandFrame select;
                    select.SetFullCommand("select %u", slave.syncing_from);
                    Buffer buf;
                    RedisCommandEncoder::Encode(buf, select);
                    offset -= buf.ReadableBytes();
                }
                msg.Printf("+FULLRESYNC %s %lld\r\n", g_db->m_repl_backlog.GetServerKey(), offset);
                slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
            }
            slave.conn->Write(msg);
        }
        if (slave.state == SLAVE_STATE_WAITING_DUMP_DATA)
        {
            if (slave.isRedisSlave)
            {
                FullResyncRedisSlave(slave);
            }
            else
            {
                FullResyncArdbSlave(slave);
            }
        }
        else
        {
            SendCacheToSlave(slave);
        }
    }

    void Master::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveConnTable::iterator found = m_slave_table.find(conn_id);
        if (found != m_slave_table.end())
        {
            WARN_LOG("Slave %s closed.", found->second->GetAddress().c_str());
            m_slave_table.erase(found);
        }
    }
    void Master::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveConnTable::iterator found = m_slave_table.find(conn_id);
        if (found != m_slave_table.end())
        {
            SlaveConnection* slave = found->second;
            if (slave->state == SLAVE_STATE_SYNING_CACHE_DATA)
            {
                DEBUG_LOG("[Master]Slave sync from %lld to %llu at state:%u", slave->sync_offset,
                        g_db->m_repl_backlog.GetReplEndOffset(), slave->state);
                if ((uint64) slave->sync_offset == g_db->m_repl_backlog.GetReplEndOffset())
                {
                    slave->state = SLAVE_STATE_SYNCED;
                    ChannelOptions options;
                    options.auto_disable_writing = true;
                    slave->conn->Configure(options);
                    DEBUG_LOG("[Master]Slave synced.");
                }
                else if ((uint64) slave->sync_offset < g_db->m_repl_backlog.GetReplEndOffset())
                {
                    if (!g_db->m_repl_backlog.IsValidOffset(slave->sync_offset))
                    {
                        WARN_LOG("[Master]Replication buffer overflow while syncing slave.");
                        slave->conn->Close();
                    }
                    else
                    {
                        size_t len = g_db->m_repl_backlog.WriteChannel(slave->conn, slave->sync_offset,
                        MAX_SEND_CACHE_SIZE);
                        slave->sync_offset += len;
                        slave->conn->EnableWriting();
                    }
                }
                else
                {
                    ERROR_LOG("[REPL]Invalid slave's sync offset:%lld", slave->sync_offset);
                    slave->conn->Close();
                }
            }
        }
        else
        {
            WARN_LOG("[Master]No slave found for:%u", conn_id);
        }
    }
    void Master::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
    {
        RedisCommandFrame* cmd = e.GetMessage();
        DEBUG_LOG("Master recv cmd from slave:%s", cmd->ToString().c_str());
        if (!strcasecmp(cmd->GetCommand().c_str(), "replconf"))
        {
            if (cmd->GetArguments().size() == 2 && !strcasecmp(cmd->GetArguments()[0].c_str(), "ack"))
            {
                int64 offset;
                if (string_toint64(cmd->GetArguments()[1], offset))
                {
                    SlaveConnTable::iterator found = m_slave_table.find(ctx.GetChannel()->GetID());
                    if (found != m_slave_table.end())
                    {
                        SlaveConnection* slave = found->second;
                        slave->acktime = time(NULL);
                        slave->sync_offset = offset;
                    }
                }
            }
        }
    }

    void Master::OnAddSlave(Channel* ch, void* data)
    {
        SlaveConnection* conn = (SlaveConnection*) data;
        g_master->m_channel_service.AttachChannel(conn->conn, true);
        conn->state = SLAVE_STATE_CONNECTED;
        g_master->m_slave_table[conn->conn->GetID()] = conn;
        conn->conn->ClearPipeline();

        ChannelOptions options;
        options.auto_disable_writing = false;
        conn->conn->Configure(options);
        conn->conn->SetChannelPipelineInitializor(slave_pipeline_init, NULL);
        conn->conn->SetChannelPipelineFinalizer(slave_pipeline_finallize, NULL);
        g_master->SyncSlave(*conn);
    }

    void Master::AddSlave(Channel* slave, RedisCommandFrame& cmd)
    {
        DEBUG_LOG("Recv sync command:%s", cmd.ToString().c_str());
        slave->Flush();
        slave->GetWritableOptions().max_write_buffer_size =
                (int32) (g_db->GetConfig().slave_client_output_buffer_limit);
        SlaveConnection& conn = GetSlaveConn(slave);
        if (!strcasecmp(cmd.GetCommand().c_str(), "sync"))
        {
            //Redis 2.6/2.4 send 'sync'
            conn.isRedisSlave = true;
            conn.sync_offset = -1;
        }
        else
        {
            conn.server_key = cmd.GetArguments()[0];
            const std::string& offset_str = cmd.GetArguments()[1];
            if (!string_toint64(offset_str, conn.sync_offset))
            {
                ERROR_LOG("Invalid offset argument:%s", offset_str.c_str());
                slave->Close();
                return;
            }
            //Redis 2.8+ send psync, Ardb send apsync
            if (!strcasecmp(cmd.GetCommand().c_str(), "psync"))
            {
                conn.isRedisSlave = true;
            }
            else
            {
                conn.isRedisSlave = false;
//                for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
//                {
//                    if (cmd.GetArguments()[i] == "include")
//                    {
//                        DBIDArray ids;
//                        split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
//                        convert_vector_to_set(ids, conn.include_dbs);
//                    }
//                    else if (cmd.GetArguments()[i] == "exclude")
//                    {
//                        DBIDArray ids;
//                        split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
//                        convert_vector_to_set(ids, conn.exclude_dbs);
//                    }
//                    else if (cmd.GetArguments()[i] == "cksm")
//                    {
//
//                    }
//                }
            }
        }
        slave->GetService().DetachChannel(slave, true);
        m_channel_service.AsyncIO(0, OnAddSlave, &conn);
    }

    void Master::OnFeedSlave(Channel* ch, void* data)
    {
        RedisReplCommand* cmd = (RedisReplCommand*) data;
        g_master->WriteSlaves(cmd->dbid, cmd->cmd);
        DELETE(cmd);
    }

    void Master::FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd)
    {
        if (!m_backlog_enable || !g_db->m_repl_backlog.IsInited())
        {
            DEBUG_LOG("Backlog is no enabled.");
            return;
        }
        RedisReplCommand* newcmd = new RedisReplCommand;
        newcmd->cmd = cmd;
        newcmd->dbid = dbid;
        m_channel_service.AsyncIO(0, OnFeedSlave, newcmd);
    }

    void Master::OnDisconnectAllSlaves(Channel*, void*)
    {
        SlaveConnTable tmp = g_master->m_slave_table;
        SlaveConnTable::iterator it = tmp.begin();
        while (it != tmp.end())
        {
            it->second->conn->Close();
            it++;
        }
    }

    void Master::DisconnectAllSlaves()
    {
        INFO_LOG("[REPL]Disconnect all slaves.");
        m_channel_service.AsyncIO(0, OnDisconnectAllSlaves, NULL);
    }

    void Master::Stop()
    {
        m_channel_service.Stop();
        m_channel_service.Wakeup();
        while (GetState() == RUNNING)
        {
            m_channel_service.Stop();
            Thread::Sleep(10);
        }
    }

    size_t Master::ConnectedSlaves()
    {
        return m_slave_table.size();
    }

    static void destroy_slave_conn(void* s)
    {
        delete (SlaveConnection*) s;
    }
    SlaveConnection& Master::GetSlaveConn(Channel* slave)
    {
        if (slave->Attachment() == NULL)
        {
            SlaveConnection* s = new SlaveConnection;
            s->conn = slave;
            slave->Attach(s, destroy_slave_conn);
        }
        SlaveConnection* slave_conn = (SlaveConnection*) slave->Attachment();
        return *slave_conn;
    }

    std::string SlaveConnection::GetAddress()
    {
        std::string address;
        Address* remote = const_cast<Address*>(conn->GetRemoteAddress());
        if (InstanceOf<SocketHostAddress>(remote).OK)
        {
            SocketHostAddress* un = (SocketHostAddress*) remote;
            address = "ip=";
            address += un->GetHost();
            address += ",port=";
            address += stringfromll(port);
        }
        return address;
    }

    void Master::PrintSlaves(std::string& str)
    {
        uint32 i = 0;
        char buffer[1024];
        SlaveConnTable::iterator it = m_slave_table.begin();
        while (it != m_slave_table.end())
        {
            const char* state = "ok";
            SlaveConnection* slave = it->second;
            switch (slave->state)
            {
                case SLAVE_STATE_WAITING_DUMP_DATA:
                {
                    state = "wait_bgsave";
                    break;
                }
                case SLAVE_STATE_SYNING_DUMP_DATA:
                {
                    state = "send_bulk";
                    break;
                }
                case SLAVE_STATE_SYNING_CACHE_DATA:
                case SLAVE_STATE_SYNCED:
                {
                    state = "online";
                    break;
                }
                default:
                {
                    state = "invalid state";
                    break;
                }
            }

            uint32 lag = time(NULL) - slave->acktime;
            sprintf(buffer, "slave%u:%s,state=%s,"
                    "offset=%" PRId64",lag=%u\r\n", i, slave->GetAddress().c_str(), state, slave->sync_offset, lag);
            it++;
            i++;
            str.append(buffer);
        }
    }

    void Master::AddSlavePort(Channel* slave, uint32 port)
    {
        GetSlaveConn(slave).port = port;
    }

    Master::~Master()
    {
        //DELETE(m_thread);
    }
}


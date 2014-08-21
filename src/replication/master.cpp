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
            m_dumping_db(false), m_dumpdb_offset(-1), m_repl_no_slaves_since(0), m_backlog_enable(true)
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
        SlaveConnTable::iterator it = m_slave_table.begin();
        for (; it != m_slave_table.end(); it++)
        {
            if (it->second->state == SLAVE_STATE_SYNCED)
            {
                buffer.SetReadIndex(0);
                it->second->conn->Write(buffer);
            }
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

    static void DumpRDBRoutine(void* cb)
    {
        SlaveConnection* slave = (SlaveConnection*)cb;
        Buffer newline;
        newline.Write("\n", 1);
        slave->conn->Write(newline);
        ChannelService& serv =  slave->conn->GetService();
        serv.Continue();
    }

    void Master::FullResyncArdbSlave(SlaveConnection& slave)
    {
        /*
         * Save dbid first. After full resynced, the first syncing
         * command MUST be SELECT to make sure that slave/master have
         * the same db syncing from cache.
         */

        DBID currentDB = g_db->m_repl_backlog.GetCurrentDBID();
        uint64 cksm = g_db->m_repl_backlog.GetChecksum();
        slave.sync_offset = g_db->m_repl_backlog.GetReplEndOffset();

        uint64 count;
        if (slave.include_dbs.empty() && slave.exclude_dbs.empty())
        {
            KeyObject k;
            k.db = 0;
            k.type = KEY_META;
            Iterator* iter = g_db->IteratorKeyValue(k, false);
            if (NULL != iter)
            {
                while (iter->Valid())
                {
                    ArgumentArray args;
                    args.push_back("__SET__");
                    args.push_back(std::string(iter->Key().data(), iter->Key().size()));
                    args.push_back(std::string(iter->Value().data(), iter->Value().size()));
                    RedisCommandFrame cmd(args);
                    slave.conn->Write(cmd);
                    count++;
                    if (count % 10000 == 0)
                    {
                        slave.conn->GetService().Continue();
                    }
                    iter->Next();
                }
                DELETE(iter);
            }
        }
        else
        {
            DBIDSet visit_dbs = slave.include_dbs;
            if (slave.include_dbs.empty())
            {
                g_db->GetAllDBIDSet(visit_dbs);
            }
            if (!slave.exclude_dbs.empty())
            {
                DBIDSet::iterator it = slave.exclude_dbs.begin();
                while (it != slave.exclude_dbs.end())
                {
                    visit_dbs.erase(*it);
                    it++;
                }
            }
            DBIDSet::iterator it = visit_dbs.begin();
            while (it != visit_dbs.end())
            {
                KeyObject k;
                k.db = *it;
                k.type = KEY_META;
                Iterator* iter = g_db->IteratorKeyValue(k, false);
                if (NULL != iter)
                {
                    while (iter->Valid())
                    {
                        KeyObject kk;
                        if (decode_key(iter->Key(), kk))
                        {
                            if (kk.db != k.db)
                            {
                                break;
                            }
                        }
                        ArgumentArray args;
                        args.push_back("__SET__");
                        args.push_back(std::string(iter->Key().data(), iter->Key().size()));
                        args.push_back(std::string(iter->Value().data(), iter->Value().size()));
                        RedisCommandFrame cmd(args);
                        slave.conn->Write(cmd);
                        count++;
                        if (count % 10000 == 0)
                        {
                            slave.conn->GetService().Continue();
                        }
                        iter->Next();
                    }
                    DELETE(iter);
                }
                it++;
            }
        }
        if (currentDB != ARDB_GLOBAL_DB)
        {
            Buffer select;
            select.Printf("select %u\r\n", currentDB);
            slave.conn->Write(select);
        }
        Buffer msg;
        msg.Printf("FULLSYNCED %llu %llu\r\n", slave.sync_offset, cksm);
        slave.conn->Write(msg);
        SendCacheToSlave(slave);
    }

    void Master::FullResyncRedisSlave(SlaveConnection& slave)
    {
        std::string dump_file_path = g_db->GetConfig().repl_data_dir + "/dump.rdb";
        slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
        if (m_dumping_db)
        {
            slave.sync_offset = m_dumpdb_offset;
            return;
        }
        INFO_LOG("[Master]Start dump data to file:%s", dump_file_path.c_str());
        m_dumping_db = true;
        m_dumpdb_offset = g_db->m_repl_backlog.GetReplEndOffset();
        slave.sync_offset = m_dumpdb_offset;

        RedisDumpFile rdb;
        rdb.Init(g_db);
        rdb.Save(dump_file_path, DumpRDBRoutine, &slave);
        INFO_LOG("[REPL]Saved rdb dump file:%s", dump_file_path.c_str());
        OnDumpComplete();
    }

    void Master::OnDumpComplete()
    {
        SlaveConnTable::iterator it = m_slave_table.begin();
        while (it != m_slave_table.end())
        {
            if (it->second->state == SLAVE_STATE_WAITING_DUMP_DATA)
            {
                SendDumpToSlave(*(it->second));
            }
            it++;
        }
        m_dumping_db = false;
    }

    static void OnDumpFileSendComplete(void* data)
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

    static void OnDumpFileSendFailure(void* data)
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
        if (slave.isRedisSlave && ARDB_GLOBAL_DB != slave.syncing_from)
        {
            //need send 'select <currentdb>' first
            RedisCommandFrame select;
            select.SetFullCommand("select %u", slave.syncing_from);
            slave.conn->Write(select);
            slave.syncing_from = ARDB_GLOBAL_DB;
        }
        slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
        slave.conn->EnableWriting();
        INFO_LOG("[Master]Slave request offset: %lld", slave.sync_offset);
    }

    void Master::SendDumpToSlave(SlaveConnection& slave)
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
                if (!m_dumping_db)
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
            DELETE(found->second);
            m_slave_table.erase(found);
        }
        LockGuard<ThreadMutex> guard(m_port_table_mutex);
        m_slave_port_table.erase(conn_id);
    }
    void Master::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveConnTable::iterator found = m_slave_table.find(conn_id);
        if (found != m_slave_table.end())
        {
            SlaveConnection* slave = found->second;
            DEBUG_LOG("[Master]Slave sync from %lld to %llu at state:%u", slave->sync_offset,
                    g_db->m_repl_backlog.GetReplEndOffset(), slave->state);
            if (slave->state == SLAVE_STATE_SYNING_CACHE_DATA)
            {
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
        {
            LockGuard<ThreadMutex> guard(g_master->m_port_table_mutex);
            conn->port = g_master->m_slave_port_table[conn->conn->GetID()];
        }

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
        SlaveConnection* conn = NULL;
        NEW(conn, SlaveConnection);
        conn->conn = slave;
        if (!strcasecmp(cmd.GetCommand().c_str(), "sync"))
        {
            //Redis 2.6/2.4 send 'sync'
            conn->isRedisSlave = true;
            conn->sync_offset = -1;
        }
        else
        {
            conn->server_key = cmd.GetArguments()[0];
            const std::string& offset_str = cmd.GetArguments()[1];
            if (!string_toint64(offset_str, conn->sync_offset))
            {
                ERROR_LOG("Invalid offset argument:%s", offset_str.c_str());
                slave->Close();
                DELETE(conn);
                return;
            }
            //Redis 2.8+ send psync, Ardb send apsync
            if (!strcasecmp(cmd.GetCommand().c_str(), "psync"))
            {
                conn->isRedisSlave = true;
            }
            else
            {
                conn->isRedisSlave = false;
                for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
                {
                    if (cmd.GetArguments()[i] == "include")
                    {
                        DBIDArray ids;
                        split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                        convert_vector_to_set(ids, conn->include_dbs);
                    }
                    else if (cmd.GetArguments()[i] == "exclude")
                    {
                        DBIDArray ids;
                        split_uint32_array(cmd.GetArguments()[i + 1], "|", ids);
                        convert_vector_to_set(ids, conn->exclude_dbs);
                    }
                    else if (cmd.GetArguments()[i] == "cksm")
                    {

                    }
                }
            }
        }
        slave->GetService().DetachChannel(slave, true);
        m_channel_service.AsyncIO(0, OnAddSlave, conn);
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
            Address* remote = const_cast<Address*>(slave->conn->GetRemoteAddress());
            std::string address;
            if (InstanceOf<SocketUnixAddress>(remote).OK)
            {
                SocketUnixAddress* un = (SocketUnixAddress*) remote;
                address = "unix_socket=";
                address += un->GetPath();

            }
            else if (InstanceOf<SocketHostAddress>(remote).OK)
            {
                SocketHostAddress* un = (SocketHostAddress*) remote;
                address = "ip=";
                address += un->GetHost();
                address += ",port=";
                address += stringfromll(slave->port);
            }
            uint32 lag = time(NULL) - slave->acktime;
            sprintf(buffer, "slave%u:%s,state=%s,"
                    "offset=%" PRId64",lag=%u\r\n", i, address.c_str(), state, slave->sync_offset, lag);
            it++;
            i++;
            str.append(buffer);
        }
    }

    void Master::AddSlavePort(Channel* slave, uint32 port)
    {
        LockGuard<ThreadMutex> guard(m_port_table_mutex);
        m_slave_port_table[slave->GetID()] = port;
    }

    Master::~Master()
    {
        //DELETE(m_thread);
    }
}


/*
 * master.cpp
 *
 *  Created on: 2013-08-22
 *      Author: yinqiwen
 */
#include "master.hpp"
#include "rdb.hpp"
#include "ardb_server.hpp"
#include <fcntl.h>
#include <sys/stat.h>

#define REPL_INSTRUCTION_ADD_SLAVE 0
#define REPL_INSTRUCTION_FEED_CMD 1
#define REPL_INSTRUCTION_DELETE_SLAVES 2

#define SOFT_SIGNAL_REPL_INSTRUCTION 1

#define MAX_SEND_CACHE_SIZE 4096

namespace ardb
{
    /*
     * Master
     */
    Master::Master(ArdbServer* server) :
            m_server(server), m_notify_channel(NULL), m_backlog(server->m_repl_backlog), m_dumping_db(false), m_dumpdb_offset(-1), m_thread(NULL), m_thread_running(false)
    {

    }

    int Master::Init()
    {
        m_notify_channel = m_channel_service.NewSoftSignalChannel();
        ChannelOptions options;
        options.max_write_buffer_size = 0; //disable write buffer for thread-safe reason
        m_notify_channel->Configure(options);
        m_notify_channel->Register(SOFT_SIGNAL_REPL_INSTRUCTION, this);
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

        m_channel_service.GetTimer().ScheduleHeapTask(new HeartbeatTask(this), m_server->m_cfg.repl_ping_slave_period, m_server->m_cfg.repl_ping_slave_period, SECONDS);

        m_thread = new Thread(this);
        m_thread_running = true;
        m_thread->Start();
        return 0;
    }

    void Master::Run()
    {
        m_channel_service.Start();
        m_thread_running = false;
    }

    void Master::OnHeartbeat()
    {
        //Just process instructions  since the soft signal may loss
        OnInstructions();
        if (!m_server->m_cfg.master_host.empty())
        {
            //let master feed 'ping' command
            return;
        }
        if (!m_slave_table.empty())
        {
            RedisCommandFrame ping("ping");
            ping.SetType(REDIS_CMD_PING);
            //m_backlog.Feed(ping, 0);
            WriteSlaves(0, ping);
            DEBUG_LOG("Ping slaves.");
        }
    }

    void Master::WriteCmdToSlaves(RedisCommandFrame& cmd)
    {
        Buffer buffer(256);
        RedisCommandEncoder::Encode(buffer, cmd);
        m_backlog.Feed(buffer);
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

    void Master::WriteSlaves(const DBID& dbid, RedisCommandFrame& cmd)
    {
        //DEBUG_LOG("WriteSlaves cmd:%s %u", cmd.ToString().c_str(), cmd.GetType());
        switch (cmd.GetType())
        {
            case REDIS_CMD_SELECT:
            {
                DBID id = 0;
                string_touint32(cmd.GetArguments()[0], id);
                m_backlog.SetCurrentDBID(id);
                break;
            }
            case REDIS_CMD_PING:
            {
                break;
            }
            default:
            {
                if (m_backlog.GetCurrentDBID() != dbid)
                {
                    if (!m_server->m_cfg.master_host.empty())
                    {
                        ERROR_LOG("Can NOT happen since slave instance can NOT generate select command for cmd:%s to switch DB from %u to %u", cmd.GetCommand().c_str(), m_backlog.GetCurrentDBID(), dbid);
                    } else
                    {
                        RedisCommandFrame select("select %u", dbid);
                        select.SetType(REDIS_CMD_SELECT);
                        m_backlog.SetCurrentDBID(dbid);
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
        Master* master = (Master*) data;
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

    void Master::OnInstructions()
    {
        ReplicationInstruction instruction;
        while (m_inst_queue.Pop(instruction))
        {
            switch (instruction.type)
            {
                case REPL_INSTRUCTION_ADD_SLAVE:
                {
                    SlaveConnection* conn = (SlaveConnection*) instruction.ptr;
                    m_channel_service.AttachChannel(conn->conn, true);
                    conn->state = SLAVE_STATE_CONNECTED;
                    m_slave_table[conn->conn->GetID()] = conn;
                    conn->conn->ClearPipeline();
                    {
                        LockGuard<ThreadMutex> guard(m_port_table_mutex);
                        conn->port = m_slave_port_table[conn->conn->GetID()];
                    }

                    ChannelOptions options;
                    options.auto_disable_writing = false;
                    conn->conn->Configure(options);
                    conn->conn->SetChannelPipelineInitializor(slave_pipeline_init, this);
                    conn->conn->SetChannelPipelineFinalizer(slave_pipeline_finallize, NULL);
                    SyncSlave(*conn);
                    break;
                }
                case REPL_INSTRUCTION_FEED_CMD:
                {
                    RedisCommandWithDBID* cmd = (RedisCommandWithDBID*) instruction.ptr;
                    //m_backlog.Feed(cmd->second, cmd->first);
                    WriteSlaves(cmd->first, cmd->second);
                    DELETE(cmd);
                    break;
                }
                case REPL_INSTRUCTION_DELETE_SLAVES:
                {
                    SlaveConnTable tmp = m_slave_table;
                    SlaveConnTable::iterator it = tmp.begin();
                    while (it != tmp.end())
                    {
                        it->second->conn->Close();
                        it++;
                    }
                    break;
                }
                default:
                {
                    break;
                }
            }
        }
    }
    static void DumpRDBRoutine(void* cb)
    {
        ChannelService* serv = (ChannelService*) cb;
        serv->Continue();
    }

    void Master::FullResyncArdbSlave(SlaveConnection& slave)
    {
        /*
         * Save dbid first. After full resynced, the first syncing
         * command MUST be SELECT to make sure that slave/master have
         * the same db syncing from cache.
         */

        DBID currentDB = m_backlog.GetCurrentDBID();
        slave.sync_offset = m_backlog.GetReplEndOffset();
        struct DBVisitor: public RawValueVisitor
        {
                SlaveConnection& conn;
                uint64 count;
                DBVisitor(SlaveConnection& c) :
                        conn(c), count(0)
                {
                }
                int OnRawKeyValue(const Slice& key, const Slice& value)
                {
                    ArgumentArray args;
                    args.push_back("__SET__");
                    args.push_back(std::string(key.data(), key.size()));
                    args.push_back(std::string(value.data(), value.size()));
                    RedisCommandFrame cmd(args);
                    conn.conn->Write(cmd);
                    count++;
                    if (count % 10000 == 0)
                    {
                        conn.conn->GetService().Continue();
                    }
                    return 0;
                }
        } visitor(slave);

        if (slave.syncdbs.empty())
        {
            m_server->m_db->VisitAllDB(&visitor);
        } else
        {
            DBIDSet::iterator it = slave.syncdbs.begin();
            while (it != slave.syncdbs.end())
            {
                m_server->m_db->VisitDB(*it, &visitor);
                it++;
            }
            m_server->m_db->VisitDB(ARDB_GLOBAL_DB, &visitor);
        }
        if (currentDB != ARDB_GLOBAL_DB)
        {
            Buffer select;
            select.Printf("select %u\r\n", currentDB);
            slave.conn->Write(select);
        }
        Buffer msg;
        msg.Printf("FULLSYNCED\r\n");
        slave.conn->Write(msg);
        SendCacheToSlave(slave);
    }

    void Master::FullResyncRedisSlave(SlaveConnection& slave)
    {
        std::string dump_file_path = m_server->m_cfg.repl_data_dir + "/dump.rdb";
        slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
        if (m_dumping_db)
        {
            slave.sync_offset = m_dumpdb_offset;
            return;
        }
        INFO_LOG("[Master]Start dump data to file:%s", dump_file_path.c_str());
        m_dumping_db = true;
        m_dumpdb_offset = m_backlog.GetReplEndOffset();
        slave.sync_offset = m_dumpdb_offset;

        RedisDumpFile rdb(m_server->m_db, dump_file_path);
        rdb.Dump(DumpRDBRoutine, &m_channel_service);
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
            RedisCommandFrame select("select %u", slave.syncing_from);
            slave.conn->Write(select);
            slave.syncing_from = ARDB_GLOBAL_DB;
        }
        slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
        slave.conn->EnableWriting();
        INFO_LOG("[PSYNC]Slave request offset: %lld", slave.sync_offset);
    }

    void Master::SendDumpToSlave(SlaveConnection& slave)
    {
        INFO_LOG("[REPL]Send dump file to slave");
        slave.state = SLAVE_STATE_SYNING_DUMP_DATA;
        std::string dump_file_path = m_server->m_cfg.repl_data_dir + "/dump.rdb";
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
        } else
        {
            Buffer msg;
            if (m_backlog.IsValidOffset(slave.server_key, slave.sync_offset))
            {
                msg.Printf("+CONTINUE\r\n");
                slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
            } else
            {
                //FULLRESYNC
                if (!m_dumping_db)
                {
                    slave.syncing_from = m_backlog.GetCurrentDBID();
                }
                uint64 offset = m_backlog.GetReplEndOffset() - 1;
                if (slave.syncing_from != ARDB_GLOBAL_DB && slave.isRedisSlave)
                {
                    RedisCommandFrame select("select %u", slave.syncing_from);
                    Buffer buf;
                    RedisCommandEncoder::Encode(buf, select);
                    offset -= buf.ReadableBytes();
                }
                msg.Printf("+FULLRESYNC %s %lld\r\n", m_backlog.GetServerKey().c_str(), offset);
                slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
            }
            slave.conn->Write(msg);
        }
        if (slave.state == SLAVE_STATE_WAITING_DUMP_DATA)
        {
            if (slave.isRedisSlave)
            {
                FullResyncRedisSlave(slave);
            } else
            {
                FullResyncArdbSlave(slave);
            }
        } else
        {
            SendCacheToSlave(slave);
        }
    }

    void Master::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
    {
        ASSERT(soft_signo == SOFT_SIGNAL_REPL_INSTRUCTION);
        OnInstructions();
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
            if (slave->state == SLAVE_STATE_SYNING_CACHE_DATA)
            {
                if ((uint64) slave->sync_offset >= m_backlog.GetReplEndOffset())
                {
                    slave->state = SLAVE_STATE_SYNCED;
                    ChannelOptions options;
                    options.auto_disable_writing = true;
                    slave->conn->Configure(options);
                } else
                {
                    if (!m_backlog.IsValidOffset(slave->sync_offset))
                    {
                        slave->conn->Close();
                    } else
                    {
                        size_t len = m_backlog.WriteChannel(slave->conn, slave->sync_offset, MAX_SEND_CACHE_SIZE);
                        slave->sync_offset += len;
                        slave->conn->EnableWriting();
                    }
                }
            }
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

    void Master::OfferReplInstruction(ReplicationInstruction& inst)
    {
        m_inst_queue.Push(inst);
        if (NULL != m_notify_channel)
        {
            /*
             * m_notify_channel use pipes to notify master thread, and for pipes
             * POSIX.1-2001 says that write(2)s of less than PIPE_BUF(512) bytes must be atomic,
             * in most time is's safe here.
             *
             */
            m_notify_channel->FireSoftSignal(SOFT_SIGNAL_REPL_INSTRUCTION, 0);
        } else
        {
            DEBUG_LOG("NULL singnal channel:%p %p", m_notify_channel, this);
        }
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
        } else
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
            //Redis 2.8+ send psync, Ardb send psync2
            if (!strcasecmp(cmd.GetCommand().c_str(), "psync"))
            {
                conn->isRedisSlave = true;
            } else
            {
                conn->isRedisSlave = false;
                if (cmd.GetArguments().size() >= 3)
                {
                    std::vector<std::string> ss = split_string(cmd.GetArguments()[2], "|");
                    for (uint32 i = 0; i < ss.size(); i++)
                    {
                        DBID id;
                        if (string_touint32(ss[i], id))
                        {
                            conn->syncdbs.insert(id);
                        }
                    }
                }
            }
        }
        m_server->m_service->DetachChannel(slave, true);
        ReplicationInstruction inst(conn, REPL_INSTRUCTION_ADD_SLAVE);
        OfferReplInstruction(inst);
    }

    void Master::FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd)
    {
        RedisCommandWithDBID* newcmd = new RedisCommandWithDBID(dbid, cmd);
        ReplicationInstruction inst(newcmd, REPL_INSTRUCTION_FEED_CMD);
        OfferReplInstruction(inst);
    }

    void Master::DisconnectAllSlaves()
    {
        INFO_LOG("[REPL]Disconnect all slaves.");
        ReplicationInstruction inst(NULL, REPL_INSTRUCTION_DELETE_SLAVES);
        OfferReplInstruction(inst);
    }

    void Master::Stop()
    {
        m_channel_service.Stop();
        while (m_thread_running)
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

            } else if (InstanceOf<SocketHostAddress>(remote).OK)
            {
                SocketHostAddress* un = (SocketHostAddress*) remote;
                address = "ip=";
                address += un->GetHost();
                address += ",port=";
                address += stringfromll(slave->port);
            }
            uint32 lag = time(NULL) - slave->acktime;
            sprintf(buffer, "slave%u:%s,state=%s,"
                    "offset=%"PRId64",lag=%u\r\n", i, address.c_str(), state, slave->sync_offset, lag);
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


/*
 * master.cpp
 *
 *  Created on: 2013-08-22
 *      Author: yinqiwen
 */
#include "master.hpp"
#include <fcntl.h>
#include <sys/stat.h>
#include "repl.hpp"

#define MAX_SEND_CACHE_SIZE 8192

namespace comms
{
    enum SyncState
    {
        SYNC_STATE_INVALID, SYNC_STATE_START, SYNC_STATE_WAITING_SNAPSHOT, SYNC_STATE_SYNCING_SNAPSHOT,
        //SYNC_STATE_SYNCING_WAL,
        SYNC_STATE_SYNCED,
    };

    struct SlaveConn
    {
            Channel* conn;
            std::string server_key;
            int64 sync_offset;
            int64 ack_offset;
            uint64 sync_cksm;
            uint32 acktime;
            uint32 port;
            int repldbfd;
            bool isRedisSlave;
            SyncState state;
            SlaveConn() :
                    conn(NULL), sync_offset(0), ack_offset(0), sync_cksm(0), acktime(0), port(0), repldbfd(-1), isRedisSlave(
                            false), state(SYNC_STATE_INVALID)
            {
            }
            std::string GetAddress()
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
            ~SlaveConn()
            {
            }
    };

    Master::Master() :
            m_repl_noslaves_since(0)
    {
    }

    int Master::Init()
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
        struct RoutineTask: public Runnable
        {
                Master* serv;
                RoutineTask(Master* s) :
                        serv(s)
                {
                }
                void Run()
                {
                    serv->ClearNilSlave();
                }
        };

        g_repl->GetTimer().ScheduleHeapTask(new HeartbeatTask(this), g_db->GetConfig().repl_ping_slave_period,
                g_db->GetConfig().repl_ping_slave_period, SECONDS);
        g_repl->GetTimer().ScheduleHeapTask(new RoutineTask(this), 1, 1, SECONDS);
        return 0;
    }

    void Master::ClearNilSlave()
    {
        SlaveConnTable::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            if (it->second == NULL)
            {
                it = m_slaves.erase(it);
                continue;
            }
            it++;
        }
    }

    void Master::OnHeartbeat()
    {
        //Just process instructions  since the soft signal may loss
        if (!g_db->GetConfig().master_host.empty())
        {
            //let master feed 'ping' command
            return;
        }
        if (m_slaves.empty())
        {
            if (0 == m_repl_noslaves_since)
            {
                m_repl_noslaves_since = time(NULL);
            }
            else
            {
                return;
            }
        }

        if (!m_slaves.empty())
        {
            m_repl_noslaves_since = 0;
            Buffer ping;
            ping.Printf("ping\r\n");
            g_repl->WriteWAL(ping);
            SyncWAL();
            DEBUG_LOG("Ping slaves.");
        }
    }

    static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
    {
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisCommandEncoder);
        pipeline->AddLast("handler", &(g_repl->GetMaster()));
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
        SlaveConnTable::iterator fit = m->m_slaves.begin();
        int waiting_dump_slave_count = 0;
        while (fit != m->m_slaves.end())
        {
            SlaveConn* slave = fit->second;
            if (NULL != slave && slave->state == SYNC_STATE_WAITING_SNAPSHOT)
            {
                Buffer newline;
                newline.Write("\n", 1);
                slave->conn->Write(newline);
                waiting_dump_slave_count++;
            }
            fit++;
        }
        g_repl->GetIOServ().Continue();
        //no waiting slave, just abort dump process
        return waiting_dump_slave_count > 0 ? 0 : -1;
    }

    void Master::FullResyncSlaves(bool is_redis_type)
    {
        SlaveConnTable::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            if (it->second != NULL && it->second->state == SYNC_STATE_WAITING_SNAPSHOT)
            {
                if (is_redis_type == it->second->isRedisSlave)
                {
                    SendSnapshotToSlave(it->second);
                }
            }
            it++;
        }
    }

    static void OnSnapshotFileSendComplete(void* data)
    {
        SlaveConn* slave = (SlaveConn*) data;
        close(slave->repldbfd);
        slave->repldbfd = -1;
        slave->state = SYNC_STATE_SYNCED;
    }

    static void OnSnapshotFileSendFailure(void* data)
    {
        SlaveConn* slave = (SlaveConn*) data;
        close(slave->repldbfd);
        slave->repldbfd = -1;
    }

    void Master::SendSnapshotToSlave(SlaveConn* slave)
    {
        slave->state = SYNC_STATE_SYNCING_SNAPSHOT;
        //FULLRESYNC
        Buffer msg;
        SnapshotType type = slave->isRedisSlave ? REDIS_SNAPSHOT : MMKV_SNAPSHOT;
        slave->sync_offset = Snapshot::SnapshotOffset(type);
        slave->sync_cksm = Snapshot::SnapshotCksm(type);
        if (slave->isRedisSlave)
        {
            msg.Printf("+FULLRESYNC %s %lld\r\n", g_repl->GetServerKey(), slave->sync_offset);
        }
        else
        {
            msg.Printf("+FULLRESYNC %s %lld %llu\r\n", g_repl->GetServerKey(), slave->sync_offset, slave->sync_cksm);
        }
        slave->conn->Write(msg);
        std::string dump_file_path = Snapshot::DefaultPath(type);
        SendFileSetting setting;
        setting.fd = open(dump_file_path.c_str(), O_RDONLY);
        if (-1 == setting.fd)
        {
            int err = errno;
            ERROR_LOG("Failed to open file:%s for reason:%s", dump_file_path.c_str(), strerror(err));
            slave->conn->Close();
            return;
        }
        setting.file_offset = 0;
        struct stat st;
        fstat(setting.fd, &st);
        Buffer header;
        header.Printf("$%llu\r\n", st.st_size);
        slave->conn->Write(header);

        setting.file_rest_len = st.st_size;
        setting.on_complete = OnSnapshotFileSendComplete;
        setting.on_failure = OnSnapshotFileSendFailure;
        setting.data = slave;
        slave->repldbfd = setting.fd;
        slave->conn->SendFile(setting);
    }

    static int send_wal_toslave(const void* log, size_t loglen, void* data)
    {
        SlaveConn* slave = (SlaveConn*) data;
        Buffer msg((char*) log, 0, loglen);
        slave->conn->Write(msg);
        slave->sync_offset += loglen;
        //INFO_LOG("####%d %d %d", loglen, slave->sync_offset, g_repl->WALEndOffset());
        if (slave->sync_offset == g_repl->WALEndOffset())
        {
            slave->conn->GetWritableOptions().auto_disable_writing = true;
        }
        else
        {
            slave->conn->GetWritableOptions().auto_disable_writing = false;
        }
        slave->conn->EnableWriting();
        return 0;
    }

    void Master::SyncWAL(SlaveConn* slave)
    {
        if (slave->sync_offset < g_repl->WALStartOffset() || slave->sync_offset > g_repl->WALEndOffset())
        {
            slave->conn->Close();
            ERROR_LOG("Slave synced offset:%llu is invalid in offset range[%llu-%llu] for wal.", slave->sync_offset,
                    g_repl->WALStartOffset(), g_repl->WALEndOffset());
            return;
        }
        if (slave->sync_offset < g_repl->WALEndOffset())
        {
            swal_replay(g_repl->GetWAL(), slave->sync_offset, MAX_SEND_CACHE_SIZE, send_wal_toslave, slave);
        }
    }

    void Master::SyncWAL()
    {
        SlaveConnTable::iterator it = m_slaves.begin();
        bool no_lag_slave = true;
        while (it != m_slaves.end())
        {
            if (it->second != NULL)
            {
                if (it->second->state == SYNC_STATE_SYNCED)
                {
                    if (it->second->sync_offset != g_repl->WALStartOffset())
                    {
                        SyncWAL(it->second);
                        no_lag_slave = false;
                    }
                }
                else
                {
                    no_lag_slave = false;
                }

            }
            it++;
        }
        if (no_lag_slave)
        {
            swal_clear_replay_cache(g_repl->GetWAL());
        }
    }

    int Master::CreateSnapshot(bool is_redis_type)
    {
        Snapshot rdb;
        int ret = rdb.Save(false, is_redis_type ? REDIS_SNAPSHOT : MMKV_SNAPSHOT, DumpRDBRoutine, this);

        if (0 == ret)
        {
            INFO_LOG("[Master]Saved snapshot file.");
            FullResyncSlaves(is_redis_type);
        }
        else if (ERR_SNAPSHOT_SAVING == ret)
        {
            return 0;
        }
        else
        {
            WARN_LOG("Failed to create snapshot file.");
            SlaveConnTable::iterator it = m_slaves.begin();
            while (it != m_slaves.end())
            {
                if (it->second != NULL && it->second->state == SYNC_STATE_WAITING_SNAPSHOT)
                {
                    it->second->conn->Close();
                }
                it++;
            }
        }
        return 0;
    }

    void Master::SyncSlave(SlaveConn* slave)
    {
        if (!g_db->GetConfig().master_host.empty())
        {
            if (!g_repl->GetSlave().IsSynced())
            {
                //just close slave connection if current instance not synced with remote master
                slave->conn->Close();
                return;
            }
        }

        if (slave->server_key.empty())
        {
            //redis 2.6/2.4
            slave->state = SYNC_STATE_SYNCING_SNAPSHOT;
        }
        else
        {
            Buffer msg;
            bool fullsync = true;
            if (slave->server_key == g_repl->GetServerKey())
            {
                if (!g_repl->IsValidOffsetCksm(slave->sync_offset, slave->sync_cksm))
                {
                    fullsync = true;
                }
                else
                {
                    fullsync = false;
                }
            }
            if (!fullsync)
            {
                msg.Printf("+CONTINUE\r\n");
                slave->conn->Write(msg);
                slave->state = SYNC_STATE_SYNCED;
                SyncWAL();
            }
            else
            {
                WARN_LOG(
                        "Create snapshot for full resync for slave runid:%s offset:%llu cksm:%llu, while current WAL runid:%s offset:%llu cksm:%llu",
                        slave->server_key.c_str(), slave->sync_offset, slave->sync_cksm, g_repl->GetServerKey(),
                        g_repl->WALEndOffset(), g_repl->WALCksm());
                slave->state = SYNC_STATE_WAITING_SNAPSHOT;
                CreateSnapshot(slave->isRedisSlave);
            }
        }
    }

    void Master::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveConnTable::iterator found = m_slaves.find(conn_id);
        if (found != m_slaves.end())
        {
            WARN_LOG("Slave %s closed.", found->second->GetAddress().c_str());
            found->second = NULL;
        }
    }
    void Master::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveConnTable::iterator found = m_slaves.find(conn_id);
        if (found != m_slaves.end())
        {
            SlaveConn* slave = found->second;
            if (NULL != slave && slave->state == SYNC_STATE_SYNCED)
            {
                DEBUG_LOG("[Master]Slave sync from %lld to %llu at state:%u", slave->sync_offset,
                        g_repl->WALEndOffset(), slave->state);
                SyncWAL(slave);
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
                    SlaveConnTable::iterator found = m_slaves.find(ctx.GetChannel()->GetID());
                    if (found != m_slaves.end())
                    {
                        SlaveConn* slave = found->second;
                        if (NULL != slave)
                        {
                            slave->acktime = time(NULL);
                            slave->ack_offset = offset;
                        }
                    }
                }
            }
        }
    }

    static void OnAddSlave(Channel* ch, void* data)
    {
        SlaveConn* conn = (SlaveConn*) data;
        g_repl->GetMaster().AddSlave(conn);
    }

    static void destroy_slave_conn(void* s)
    {
        delete (SlaveConn*) s;
    }
    static SlaveConn& GetSlaveConn(Channel* slave)
    {
        if (slave->Attachment() == NULL)
        {
            SlaveConn* s = new SlaveConn;
            s->conn = slave;
            slave->Attach(s, destroy_slave_conn);
        }
        SlaveConn* slave_conn = (SlaveConn*) slave->Attachment();
        return *slave_conn;
    }

    void Master::AddSlave(SlaveConn* slave)
    {
        DEBUG_LOG("Add slave %s", slave->GetAddress().c_str());
        g_repl->GetIOServ().AttachChannel(slave->conn, true);

        slave->state = SYNC_STATE_START;
        m_slaves[slave->conn->GetID()] = slave;
        slave->conn->ClearPipeline();

        if (g_db->GetConfig().repl_disable_tcp_nodelay)
        {
            ChannelOptions newoptions = slave->conn->GetOptions();
            newoptions.tcp_nodelay = false;
            slave->conn->Configure(newoptions);
        }
        slave->conn->SetChannelPipelineInitializor(slave_pipeline_init, NULL);
        slave->conn->SetChannelPipelineFinalizer(slave_pipeline_finallize, NULL);
        slave->conn->GetWritableOptions().auto_disable_writing = false;
        SyncSlave(slave);
    }

    void Master::AddSlave(Channel* slave, RedisCommandFrame& cmd)
    {
        INFO_LOG("[Master]Recv sync command:%s", cmd.ToString().c_str());
        slave->Flush();
        SlaveConn& conn = GetSlaveConn(slave);
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
            conn.isRedisSlave = true;
            for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
            {
                if (cmd.GetArguments()[i] == "cksm")
                {
                    conn.isRedisSlave = false;
                    if (!string_touint64(cmd.GetArguments()[i + 1], conn.sync_cksm))
                    {
                        ERROR_LOG("Invalid checksum argument:%s", cmd.GetArguments()[i + 1].c_str());
                        slave->Close();
                        return;
                    }
                }
            }
        }
        slave->GetService().DetachChannel(slave, true);
        if (g_repl->GetIOServ().IsInLoopThread())
        {
            AddSlave(&conn);
        }
        else
        {
            g_repl->GetIOServ().AsyncIO(0, OnAddSlave, &conn);
        }
    }

    void Master::DisconnectAllSlaves()
    {
        SlaveConnTable::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveConn* slave = it->second;
            if (NULL != slave)
            {
                slave->conn->Close();
            }
            it++;
        }
    }

    size_t Master::ConnectedSlaves()
    {
        return m_slaves.size();
    }

    void Master::PrintSlaves(std::string& str)
    {
        uint32 i = 0;
        char buffer[1024];
        SlaveConnTable::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            const char* state = "ok";
            SlaveConn* slave = it->second;
            if (NULL == slave)
            {
                it++;
                continue;
            }
            switch (slave->state)
            {
                case SYNC_STATE_WAITING_SNAPSHOT:
                {
                    state = "wait_bgsave";
                    break;
                }
                case SYNC_STATE_SYNCING_SNAPSHOT:
                {
                    state = "send_bulk";
                    break;
                }
                case SYNC_STATE_SYNCED:
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
                    "offset=%" PRId64",lag=%u, o_buffer_size:%u, o_buffer_capacity:%u\r\n", i,
                    slave->GetAddress().c_str(), state, slave->sync_offset, lag, slave->conn->WritableBytes(),
                    slave->conn->GetOutputBuffer().Capacity());
            it++;
            i++;
            str.append(buffer);
        }
    }

    void Master::SetSlavePort(Channel* slave, uint32 port)
    {
        GetSlaveConn(slave).port = port;
    }

    Master::~Master()
    {
    }
}


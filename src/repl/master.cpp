/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include "repl.hpp"
#include <fcntl.h>
#include <sys/stat.h>
#include "db/db.hpp"
#include "util/file_helper.hpp"

#define MAX_SEND_CACHE_SIZE 8192

OP_NAMESPACE_BEGIN
    enum SyncState
    {
        SYNC_STATE_INVALID, SYNC_STATE_START, SYNC_STATE_WAITING_SNAPSHOT, SYNC_STATE_SYNCING_SNAPSHOT,
        //SYNC_STATE_SYNCING_WAL,
        SYNC_STATE_SYNCED,
    };

    struct SlaveSyncContext
    {
            Snapshot* snapshot;
            Channel* conn;
            std::deque<std::string> sync_backup_fs;
            std::string repl_key;
            std::string engine;
            int64 sync_offset;
            int64 ack_offset;
            uint64 sync_cksm;
            uint32 acktime;
            uint32 port;
            bool isRedisSlave;
            uint8 state;
            SlaveSyncContext() :
                    snapshot(NULL), conn(NULL), sync_offset(0), ack_offset(0), sync_cksm(0), acktime(0), port(0), isRedisSlave(false), state(SYNC_STATE_INVALID)
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
            ~SlaveSyncContext()
            {
            }
    };

    Master::Master() :
            m_repl_noslaves_since(0), m_repl_nolag_since(0), m_repl_good_slaves_count(0), m_slaves_count(0), m_sync_full_count(0), m_sync_partial_ok_count(0), m_sync_partial_err_count(
                    0)
    {
    }

    int Master::Init()
    {
        return 0;
    }

    static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
    {
        pipeline->AddLast("decoder", new RedisCommandDecoder(false));
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

    static void snapshot_dump_success(Channel* ch, void* data)
    {
        Snapshot* snapshot = (Snapshot*) data;
        g_repl->GetMaster().FullResyncSlaves(snapshot);
        INFO_LOG("Snapshot dump success");
    }
    static void snapshot_dump_fail(Channel* ch, void* data)
    {
        Snapshot* snapshot = (Snapshot*) data;
        g_repl->GetMaster().CloseSlaveBySnapshot(snapshot);
    }

    static int snapshot_dump_routine(SnapshotState state, Snapshot* snapshot, void* cb)
    {
        //INFO_LOG("Snapshot dump state:%d %d", state,snapshot->GetType());
        if (state == DUMP_SUCCESS)
        {
            g_repl->GetIOService().AsyncIO(0, snapshot_dump_success, snapshot);
        }
        else if (state == DUMP_FAIL)
        {
            g_repl->GetIOService().AsyncIO(0, snapshot_dump_fail, snapshot);
        }
        else if (state == DUMPING)
        {
            //g_repl->GetIOService().Continue();
        }
        return 0;
    }

    int Master::Routine()
    {
        m_repl_good_slaves_count = 0;
        if (m_slaves.empty())
        {
            if (0 == m_repl_noslaves_since)
            {
                m_repl_noslaves_since = time(NULL);
            }
            return 0;
        }
        m_repl_noslaves_since = 0;
        std::vector<SlaveSyncContext*> to_close;
        SlaveSyncContextSet::iterator fit = m_slaves.begin();
        bool wal_ping_saved = false;
        time_t now = time(NULL);
        while (fit != m_slaves.end())
        {
            SlaveSyncContext* slave = *fit;
            if (slave->state == SYNC_STATE_WAITING_SNAPSHOT)
            {
                Buffer newline;
                newline.Write("\n", 1);
                slave->conn->Write(newline);
            }
            if (slave->state == SYNC_STATE_SYNCED)
            {
                //only master instance can generate ping command to slaves
                if (!wal_ping_saved && g_db->GetConf().master_host.empty())
                {
                    wal_ping_saved = true;
                    Buffer ping;
                    ping.Printf("*1\r\n$4\r\nping\r\n");
                    g_repl->GetReplLog().WriteWAL(ping, true);
                }
                if (slave->sync_offset < g_repl->GetReplLog().WALEndOffset())
                {
                    SyncWAL(slave);
                }

                time_t lag = now - slave->acktime;
                if (lag <= g_db->GetConf().repl_min_slaves_max_lag)
                {
                    m_repl_good_slaves_count++;
                }
                if (lag > g_db->GetConf().repl_timeout)
                {
                    WARN_LOG("Disconnecting timeout slave:%s", slave->GetAddress().c_str());
                    to_close.push_back(slave);
                }
            }
            fit++;
        }

        for (size_t i = 0; i < to_close.size(); i++)
        {
            CloseSlave(to_close[i]);
        }
        return 0;
    }

    void Master::FullResyncSlaves(Snapshot* snapshot)
    {
        std::vector<SlaveSyncContext*> to_close;
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveSyncContext* slave = *it;
            if (slave != NULL && slave->snapshot == snapshot)
            {
                int err = SendSnapshotToSlave(slave);
                if (0 != err)
                {
                    to_close.push_back(slave);
                }
            }
            it++;
        }
        for (size_t i = 0; i < to_close.size(); i++)
        {
            CloseSlave(to_close[i]);
        }
    }

    bool Master::IsAllSlaveSyncingCache()
    {
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveSyncContext* slave = *it;
            if (slave == NULL || slave->state != SYNC_STATE_SYNCED)
            {
                return false;
            }
            if (slave->sync_offset < g_repl->GetReplLog().WALEndOffset() - g_db->GetConf().repl_backlog_cache_size / 2)
            {
                return false;
            }
            it++;
        }
        return true;
    }

    void Master::CloseSlaveBySnapshot(Snapshot* snapshot)
    {
        std::vector<SlaveSyncContext*> to_close;
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveSyncContext* slave = *it;
            if (slave != NULL && slave->snapshot == snapshot)
            {
                to_close.push_back(slave);
            }
            it++;
        }

        for (size_t i = 0; i < to_close.size(); i++)
        {
            CloseSlave(to_close[i]);
        }
    }

    void Master::CloseSlave(SlaveSyncContext* slave)
    {
        if (NULL != slave && NULL != slave->conn)
        {
            slave->conn->Close();
        }
    }

    static void OnSnapshotBackupSendFailure(void* data)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) data;
        slave->snapshot = NULL;
        WARN_LOG("Send backup to slave:%s failed.", slave->GetAddress().c_str());
    }

    void Master::OnSnapshotBackupSendComplete(void* data)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) data;
        std::string fs = slave->sync_backup_fs.front();
        slave->sync_backup_fs.pop_front();
        INFO_LOG("Send backup:%s to slave:%s success.", fs.c_str(), slave->GetAddress().c_str());
        g_repl->GetMaster().SendBackupToSlave(slave);
    }

    int Master::SendBackupToSlave(SlaveSyncContext* slave)
    {
        if (!slave->sync_backup_fs.empty())
        {
            std::string fs =  slave->sync_backup_fs.front();
            std::string fs_path = slave->snapshot->GetPath() + "/" + fs;
            SendFileSetting setting;
            setting.fd = open(fs_path.c_str(), O_RDONLY);
            if (-1 == setting.fd)
            {
                int err = errno;
                ERROR_LOG("Failed to open file:%s for reason:%s", fs_path.c_str(), strerror(err));
                return -1;
            }
            setting.file_offset = 0;
            struct stat st;
            fstat(setting.fd, &st);
            Buffer header;
            BufferHelper::WriteVarString(header, fs);
            BufferHelper::WriteFixInt64(header, (int64_t) (st.st_size));
            slave->conn->Write(header);

            setting.file_rest_len = st.st_size;
            setting.on_complete = OnSnapshotBackupSendComplete;
            setting.on_failure = OnSnapshotBackupSendFailure;
            setting.data = slave;
            slave->conn->SendFile(setting);
        }
        else
        {
            slave->state = SYNC_STATE_SYNCED;
            SyncWAL(slave);
        }
        return 0;
    }

    static void OnSnapshotFileSendComplete(void* data)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) data;
        slave->snapshot = NULL;
        slave->state = SYNC_STATE_SYNCED;
        INFO_LOG("Send snapshot to slave:%s success.", slave->GetAddress().c_str());
        g_repl->GetMaster().SyncWAL(slave);
    }

    static void OnSnapshotFileSendFailure(void* data)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) data;
        slave->snapshot = NULL;
        WARN_LOG("Send snapshot to slave:%s failed.", slave->GetAddress().c_str());
    }

    int Master::SendSnapshotToSlave(SlaveSyncContext* slave)
    {
        slave->state = SYNC_STATE_SYNCING_SNAPSHOT;
        std::string dump_file_path = slave->snapshot->GetPath();
        if (slave->snapshot->GetType() == BACKUP_DUMP)
        {
            //send dir
            slave->sync_backup_fs.clear();
            list_allfiles(dump_file_path, slave->sync_backup_fs);
            Buffer header;
            int64_t filenum = slave->sync_backup_fs.size();
            header.Printf("#");  //start char
            BufferHelper::WriteFixInt64(header, filenum);
            slave->conn->Write(header);
            return SendBackupToSlave(slave);
        }

        SendFileSetting setting;
        setting.fd = open(dump_file_path.c_str(), O_RDONLY);
        if (-1 == setting.fd)
        {
            int err = errno;
            ERROR_LOG("Failed to open file:%s for reason:%s", dump_file_path.c_str(), strerror(err));
            return -1;
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
        slave->conn->SendFile(setting);
        return 0;
    }

    static size_t send_wal_toslave(const void* log, size_t loglen, void* data)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) data;
        slave->conn->GetOutputBuffer().Write(log, loglen);
        slave->sync_offset += loglen;
        if (slave->sync_offset == g_repl->GetReplLog().WALEndOffset(false))
        {
            slave->conn->GetWritableOptions().auto_disable_writing = true;
        }
        else
        {
            slave->conn->GetWritableOptions().auto_disable_writing = false;
        }
        slave->conn->EnableWriting();
        return loglen;
    }

    void Master::SyncWAL(SlaveSyncContext* slave)
    {
        if (slave->sync_offset < g_repl->GetReplLog().WALStartOffset() || slave->sync_offset > g_repl->GetReplLog().WALEndOffset())
        {
            WARN_LOG("Slave synced offset:%llu is invalid in offset range[%llu-%llu] for wal.", slave->sync_offset, g_repl->GetReplLog().WALStartOffset(),
                    g_repl->GetReplLog().WALEndOffset());
            slave->conn->Close();
            return;
        }
        /*
         * It's too busy to replay wal log if more than 10MB data in slave output buffer
         */
        if (slave->conn->WritableBytes() >= g_db->GetConf().slave_client_output_buffer_limit)
        {
            return;
        }
        if (slave->sync_offset < g_repl->GetReplLog().WALEndOffset())
        {
            g_repl->GetReplLog().Replay(slave->sync_offset, MAX_SEND_CACHE_SIZE, send_wal_toslave, slave);
        }
    }

    void Master::SyncWAL()
    {
        std::vector<SlaveSyncContext*> sync_slaves;
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveSyncContext* slave = *it;
            if (slave != NULL && SYNC_STATE_SYNCED == slave->state)
            {
                sync_slaves.push_back(slave);
            }
            it++;
        }

        /*
         * 'm_slaves' may be modified in iterator syncing, use local vector to store the slaves
         */
        for (size_t i = 0; i < sync_slaves.size(); i++)
        {
            SlaveSyncContext* slave = sync_slaves[i];
            SyncWAL(slave);
        }
    }

    void Master::SyncSlave(SlaveSyncContext* slave)
    {
        if (!g_db->GetConf().master_host.empty())
        {
            if (!g_repl->GetSlave().IsSynced())
            {
                //just close slave connection if current instance not synced with remote master
                WARN_LOG("Close connected slaves which is not synced.");
                slave->conn->Close();
                return;
            }
        }
        /*
         * init acktime  as current time for slave
         */
        slave->acktime = time(NULL);
        if (slave->repl_key.empty())
        {
            //redis 2.6/2.4
            slave->state = SYNC_STATE_SYNCING_SNAPSHOT;
        }
        else
        {
            Buffer msg;
            bool fullsync = true;
            if (slave->repl_key == g_repl->GetReplLog().GetReplKey())
            {
                if (!g_repl->GetReplLog().IsValidOffsetCksm(slave->sync_offset, slave->sync_cksm))
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
                INFO_LOG("[Master]Send +CONTINUE to slave %s.", slave->GetAddress().c_str());
                SyncWAL(slave);
            }
            else
            {
                if (slave->repl_key != "?")
                {
                    m_sync_partial_err_count++;
                }

                WARN_LOG("Create snapshot for full resync for slave replid:%s offset:%llu cksm:%llu, while current WAL runid:%s offset:%llu cksm:%llu",
                        slave->repl_key.c_str(), slave->sync_offset, slave->sync_cksm, g_repl->GetReplLog().GetReplKey().c_str(),
                        g_repl->GetReplLog().WALEndOffset(), g_repl->GetReplLog().WALCksm());
                slave->state = SYNC_STATE_WAITING_SNAPSHOT;
                SnapshotType snapshot_type = slave->isRedisSlave ? REDIS_DUMP : ARDB_DUMP;
                if (slave->engine == g_engine_name && g_engine->GetFeatureSet().support_backup)
                {
                    snapshot_type = BACKUP_DUMP;
                }
                slave->snapshot = g_snapshot_manager->GetSyncSnapshot(snapshot_type, snapshot_dump_routine, this);
                if (NULL != slave->snapshot)
                {
                    //FULLRESYNC
                    /* We are going to accumulate the incremental changes for this
                     * slave as well.Clear current namespace in order to force to re-emit
                     * a SLEECT statement in the replication stream. */
                    g_repl->GetReplLog().ClearCurrentNamespace();
                    Buffer msg;
                    slave->sync_offset = slave->snapshot->CachedReplOffset();
                    slave->sync_cksm = slave->snapshot->CachedReplCksm();
                    if (slave->isRedisSlave)
                    {
                        msg.Printf("+FULLRESYNC %s %lld\r\n", g_repl->GetReplLog().GetReplKey().c_str(), slave->sync_offset);
                    }
                    else
                    {
                        msg.Printf("+%s %s %lld %llu\r\n", BACKUP_DUMP == snapshot_type ? "FULLBACKUP" : "FULLRESYNC",
                                g_repl->GetReplLog().GetReplKey().c_str(), slave->sync_offset, slave->sync_cksm);
                    }
                    slave->conn->Write(msg);
                    m_sync_full_count++;
                    if (slave->snapshot->IsReady())
                    {
                        FullResyncSlaves(slave->snapshot);
                    }
                }
                else
                {
                    ERROR_LOG("Failed to get sync snapshot for slave:%s", slave->GetAddress().c_str());
                    CloseSlave(slave);
                }
            }
        }
    }

    void Master::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        SlaveSyncContext* slave = (SlaveSyncContext*) (ctx.GetChannel()->Attachment());
        if (NULL != slave)
        {
            WARN_LOG("Slave %s closed.", slave->GetAddress().c_str());
            m_slaves.erase(slave);
            m_slaves_count = m_slaves.size();
        }
    }
    void Master::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        uint32 conn_id = ctx.GetChannel()->GetID();
        SlaveSyncContext* slave = (SlaveSyncContext*) (ctx.GetChannel()->Attachment());
        if (NULL != slave)
        {
            if (slave->state == SYNC_STATE_SYNCED)
            {
                DEBUG_LOG("[Master]Slave sync from %lld to %llu at state:%u", slave->sync_offset, g_repl->GetReplLog().WALEndOffset(), slave->state);
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
        SlaveSyncContext* slave = (SlaveSyncContext*) (ctx.GetChannel()->Attachment());
        if (cmd->IsEmpty())
        {
            if (NULL != slave)
            {
                /*
                 * redis slave would send '\n' as empty command to keep connection alive
                 */
                slave->acktime = time(NULL);
            }
            return;
        }
        DEBUG_LOG("Master recv cmd from slave:%s", cmd->ToString().c_str());
        if (!strcasecmp(cmd->GetCommand().c_str(), "replconf"))
        {
            if (cmd->GetArguments().size() == 2 && !strcasecmp(cmd->GetArguments()[0].c_str(), "ack"))
            {
                int64 offset;
                if (string_toint64(cmd->GetArguments()[1], offset))
                {
                    if (NULL != slave)
                    {
                        slave->acktime = time(NULL);
                        slave->ack_offset = offset;
                    }
                }
            }
        }
    }

    static void OnAddSlave(Channel* ch, void* data)
    {
        SlaveSyncContext* conn = (SlaveSyncContext*) data;
        g_repl->GetMaster().AddSlave(conn);
    }

    static void destroy_slave_conn(void* s)
    {
        delete (SlaveSyncContext*) s;
    }
    static SlaveSyncContext& getSlaveContext(Channel* slave)
    {
        if (slave->Attachment() == NULL)
        {
            SlaveSyncContext* s = new SlaveSyncContext;
            s->conn = slave;
            slave->Attach(s, destroy_slave_conn);
        }
        SlaveSyncContext* slave_conn = (SlaveSyncContext*) slave->Attachment();
        return *slave_conn;
    }

    void Master::AddSlave(SlaveSyncContext* slave)
    {
        INFO_LOG("Accept slave %s", slave->GetAddress().c_str());
        g_repl->GetIOService().AttachChannel(slave->conn, true);
        slave->state = SYNC_STATE_START;
        slave->conn->ClearPipeline();
        if (g_db->GetConf().repl_disable_tcp_nodelay)
        {
            ChannelOptions newoptions = slave->conn->GetOptions();
            newoptions.tcp_nodelay = false;
            slave->conn->Configure(newoptions);
        }
        slave->conn->SetChannelPipelineInitializor(slave_pipeline_init, NULL);
        slave->conn->SetChannelPipelineFinalizer(slave_pipeline_finallize, NULL);
        slave->conn->GetWritableOptions().auto_disable_writing = false;

        m_slaves.insert(slave);
        m_slaves_count = m_slaves.size();
        SyncSlave(slave);
    }

    void Master::AddSlave(Channel* slave, RedisCommandFrame& cmd)
    {
        INFO_LOG("[Master]Recv sync command:%s", cmd.ToString().c_str());
        slave->Flush();
        SlaveSyncContext& ctx = getSlaveContext(slave);
        if (cmd.GetType() == REDIS_CMD_SYNC)
        {
            //Redis 2.6/2.4 send 'sync'
            ctx.isRedisSlave = true;
            ctx.sync_offset = -1;
        }
        else
        {
            ctx.repl_key = cmd.GetArguments()[0];
            const std::string& offset_str = cmd.GetArguments()[1];
            if (!string_toint64(offset_str, ctx.sync_offset))
            {
                ERROR_LOG("Invalid offset argument:%s", offset_str.c_str());
                slave->Close();
                return;
            }
            ctx.isRedisSlave = true;
            for (uint32 i = 2; i < cmd.GetArguments().size(); i += 2)
            {
                if (cmd.GetArguments()[i] == "cksm")
                {
                    ctx.isRedisSlave = false;
                    if (!string_touint64(cmd.GetArguments()[i + 1], ctx.sync_cksm))
                    {
                        ERROR_LOG("Invalid checksum argument:%s", cmd.GetArguments()[i + 1].c_str());
                        slave->Close();
                        return;
                    }
                }
                else if (cmd.GetArguments()[i] == "engine")
                {
                    ctx.engine = cmd.GetArguments()[i + 1];
                }
            }
            if (ctx.isRedisSlave)
            {
                ctx.sync_offset--;
            }
        }
        slave->GetService().DetachChannel(slave, true);
        if (g_repl->GetIOService().IsInLoopThread())
        {
            AddSlave(&ctx);
        }
        else
        {
            g_repl->GetIOService().AsyncIO(0, OnAddSlave, &ctx);
        }
    }

    void Master::DisconnectAllSlaves()
    {
        std::vector<SlaveSyncContext*> to_close;
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            SlaveSyncContext* slave = *it;
            if (NULL != slave)
            {
                to_close.push_back(slave);
            }
            it++;
        }
        if (!to_close.empty())
        {
            WARN_LOG("Disconnect all slaves.");
        }
        for (size_t i = 0; i < to_close.size(); i++)
        {
            CloseSlave(to_close[i]);
        }
    }

    size_t Master::ConnectedSlaves()
    {
        return m_slaves_count;
    }

    void Master::PrintSlaves(std::string& str)
    {
        uint32 i = 0;
        char buffer[1024];
        SlaveSyncContextSet::iterator it = m_slaves.begin();
        while (it != m_slaves.end())
        {
            const char* state = "ok";
            SlaveSyncContext* slave = *it;
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
                    "offset=%" PRId64 ",ack_offset=%" PRId64",lag=%u,o_buffer_size=%u,o_buffer_capacity=%u\r\n", i, slave->GetAddress().c_str(), state,
                    slave->sync_offset, slave->ack_offset, lag, slave->conn->WritableBytes(), slave->conn->GetOutputBuffer().Capacity());
            it++;
            i++;
            str.append(buffer);
        }
    }

    void Master::SetSlavePort(Channel* slave, uint32 port)
    {
        getSlaveContext(slave).port = port;
    }

    Master::~Master()
    {
    }
OP_NAMESPACE_END


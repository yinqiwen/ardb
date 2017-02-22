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

#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include "repl.hpp"
#include "redis/crc64.h"
#include "db/db.hpp"
#include "util/concurrent_queue.hpp"

#define MAX_REPLAY_CACHE_SIZE 1024*1024

OP_NAMESPACE_BEGIN

    static QPSTrack g_slave_sync_qps;

    enum SlaveState
    {
        SLAVE_STATE_INVALID = 0,
        SLAVE_STATE_CONNECTING,
        SLAVE_STATE_WAITING_AUTH_REPLY,
        SLAVE_STATE_WAITING_INFO_REPLY,
        SLAVE_STATE_WAITING_REPLCONF_REPLY,
        SLAVE_STATE_WAITING_PSYNC_REPLY,
        SLAVE_STATE_WAITING_SNAPSHOT,
        SLAVE_STATE_LOADING_SNAPSHOT,
        SLAVE_STATE_REPLAYING_WAL,
        SLAVE_STATE_SYNCED,
    };
    static const char* state2String(int state)
    {
        switch (state)
        {
            case SLAVE_STATE_CONNECTING:
            {
                return "CONNECTING";
            }
            case SLAVE_STATE_WAITING_AUTH_REPLY:
            {
                return "WAITING_AUTH_REPLY";
            }
            case SLAVE_STATE_WAITING_INFO_REPLY:
            {
                return "WAITING_INFO_REPLY";
            }
            case SLAVE_STATE_WAITING_REPLCONF_REPLY:
            {
                return "WAITING_REPLCONF_REPLY";
            }
            case SLAVE_STATE_WAITING_PSYNC_REPLY:
            {
                return "WAITING_PSYNC_REPLY";
            }
            case SLAVE_STATE_WAITING_SNAPSHOT:
            {
                return "WAITING_SNAPSHOT";
            }
            case SLAVE_STATE_LOADING_SNAPSHOT:
            {
                return "LOADING_SNAPSHOT";
            }
            case SLAVE_STATE_REPLAYING_WAL:
            {
                return "REPLAYING_WAL";
            }
            case SLAVE_STATE_SYNCED:
            {
                return "SYNCED";
            }
            default:
            {
                return "INVALID_STATE";
            }
        }
    }

    void SlaveContext::ResetCallFlags()
    {
        ctx.ClearFlags();
        ctx.flags.no_wal = 1;
        ctx.flags.no_fill_reply = 1;
        ctx.flags.slave = 1;
    }

    void SlaveContext::UpdateSyncOffsetCksm(const Buffer& buffer)
    {
        sync_repl_offset += buffer.ReadableBytes();
        sync_repl_cksm = crc64(sync_repl_cksm, (const unsigned char *) (buffer.GetRawReadBuffer()), buffer.ReadableBytes());
    }
    void SlaveContext::Clear()
    {
        server_is_redis = false;
        server_support_psync = false;
        state = 0;
        cached_master_runid.clear();
        cached_master_repl_offset = 0;
        cached_master_repl_cksm = 0;
        sync_repl_offset = 0;
        sync_repl_cksm = 0;
        snapshot_path.clear();
        if(snapshot.IsReady())
        {
            snapshot.Close();
        }
        else
        {
            snapshot.Remove();
        }
        snapshot.SetRoutineCallback(NULL, NULL);
    }

    Slave::Slave() :
            m_client(NULL), m_clientid(0)
    {
    	m_ctx.ctx.client = &m_client_ctx;
    }

    int Slave::Init()
    {
        g_slave_sync_qps.name = "slave_sync_total_commands_processed";
        g_slave_sync_qps.qpsName = "slave_sync_instantaneous_ops_per_sec";
        Statistics::GetSingleton().AddTrack(&g_slave_sync_qps);
        /*
         * use same thread pool size with network server
         */
        m_db_writer.Init(g_db->GetConf().thread_pool_size);
        return 0;
    }

    time_t Slave::GetMasterLastinteractionTime()
    {
        return m_ctx.master_last_interaction_time;
    }
    time_t Slave::GetMasterLinkDownTime()
    {
        return m_ctx.master_link_down_time;
    }

    static size_t slave_replay_wal(const void* log, size_t loglen, void* data)
    {
        return g_repl->GetSlave().ReplayWAL(log, loglen);
    }

    void Slave::ReplayWAL()
    {
        static bool replaying = false;
        /*
         * this method is not reentrant when replaying local wal
         */
        if (replaying)
        {
            return;
        }
        if (m_ctx.state != SLAVE_STATE_REPLAYING_WAL)
        {
            //can not replay wal in non synced state
            if (m_ctx.state != SLAVE_STATE_SYNCED && m_ctx.state != SLAVE_STATE_LOADING_SNAPSHOT && SLAVE_STATE_WAITING_SNAPSHOT != m_ctx.state)
            {
                WARN_LOG("Try to replay wal at state:%s", state2String(m_ctx.state));
            }
            return;
        }
        replaying = true;
        m_ctx.ResetCallFlags();
        m_db_writer.SetDefaulFlags(m_ctx.ctx.flags);
        INFO_LOG("Start to replay wal from sync offset:%lld & wal end offset:%llu", m_ctx.sync_repl_offset, g_repl->GetReplLog().WALEndOffset());
        int64_t replay_init_offset = m_ctx.sync_repl_offset;
        const int64_t replay_process_events_interval_bytes = 10 * 1024 * 1024;
        while (true)
        {
            if (m_ctx.sync_repl_offset < g_repl->GetReplLog().WALStartOffset() || m_ctx.sync_repl_offset > g_repl->GetReplLog().WALEndOffset())
            {
                ERROR_LOG("Failed to replay wal with sync_offset:%lld, wal_start_offset:%llu, wal_end_offset:%lld", m_ctx.sync_repl_offset,
                        g_repl->GetReplLog().WALStartOffset(), g_repl->GetReplLog().WALEndOffset());
                DoClose();
                /*
                 * Data lost when loading wal while wal, reset wal record, clear replication key
                 * Next time, when this slave connected master, it would do a full resync from master.
                 */
                g_repl->GetReplLog().SetReplKey("?");
                break;
            }

            int64_t before_replayed_bytes = m_ctx.sync_repl_offset - replay_init_offset;
            g_repl->GetReplLog().Replay(m_ctx.sync_repl_offset, MAX_REPLAY_CACHE_SIZE, slave_replay_wal, NULL);
            int64_t after_replayed_bytes = m_ctx.sync_repl_offset - replay_init_offset;
            if (after_replayed_bytes / replay_process_events_interval_bytes > before_replayed_bytes / replay_process_events_interval_bytes)
            {
                INFO_LOG("%lld bytes replayed from wal log, %llu bytes left.", after_replayed_bytes,
                        g_repl->GetReplLog().WALEndOffset() - m_ctx.sync_repl_offset);
            }
            if (m_ctx.sync_repl_offset == g_repl->GetReplLog().WALEndOffset())
            {
                m_ctx.state = SLAVE_STATE_SYNCED;
                if (!g_repl->GetReplLog().CurrentNamespace().empty())
                {
                    m_db_writer.SetNamespace(m_ctx.ctx, g_repl->GetReplLog().CurrentNamespace());
                    //m_ctx.ctx.ns.SetString(g_repl->GetReplLog().CurrentNamespace(), false);
                }
                break;
            }
            g_repl->GetIOService().Continue();
        }
        replaying = false;
        INFO_LOG("Complete replay wal with sync offset:%lld & wal end offset:%llu", m_ctx.sync_repl_offset, g_repl->GetReplLog().WALEndOffset());
    }

    size_t Slave::ReplayWAL(const void* log, size_t loglen)
    {
        Buffer logbuf((char*) log, 0, loglen);
        while (logbuf.Readable() && m_ctx.state == SLAVE_STATE_REPLAYING_WAL)
        {
            RedisCommandFrame msg;
            size_t rest = logbuf.ReadableBytes();
            if (!RedisCommandDecoder::Decode(NULL, logbuf, msg))
            {
                break;
            }
            if (msg.GetRawProtocolData().Readable())
            {
                m_ctx.UpdateSyncOffsetCksm(msg.GetRawProtocolData());
                m_db_writer.Put(m_ctx.ctx, msg);
            }
            else
            {
                ERROR_LOG("Invalid msg with empty protocol data:%s", msg.GetCommand().c_str());
                break;
            }
        }
        return loglen - logbuf.ReadableBytes();
    }

    void Slave::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        DEBUG_LOG("Master conn connected.");
        m_ctx.master_last_interaction_time = m_ctx.cmd_recved_time = time(NULL);
        m_ctx.master_link_down_time = 0;
        m_client_ctx.last_interaction_ustime = m_ctx.cmd_recved_time;
        m_client_ctx.uptime = m_ctx.cmd_recved_time;
        m_client_ctx.clientid.id = ctx.GetChannel()->GetID();
        m_client_ctx.clientid.ctx = &m_ctx.ctx;
        m_client_ctx.client = ctx.GetChannel();
        m_db_writer.SetMasterClient(m_ctx.ctx);
        if (!g_db->GetConf().masterauth.empty())
        {
            Buffer auth;
            auth.Printf("auth %s\r\n", g_db->GetConf().masterauth.c_str());
            /*
             * always set state before write command, since 'close' callback may invoked in write function
             */
            m_ctx.state = SLAVE_STATE_WAITING_AUTH_REPLY;
            ctx.GetChannel()->Write(auth);
            return;
        }
        InfoMaster();
    }

    void Slave::HandleRedisCommand(Channel* ch, RedisCommandFrame& cmd)
    {
        m_ctx.cmd_recved_time = time(NULL);
        int len = g_repl->GetReplLog().DirectWriteWAL(cmd);
        DEBUG_LOG("Recv master inline:%d cmd %s with type:%d at %lld %lld at state:%s", cmd.IsInLine(), cmd.ToString().c_str(), len, m_ctx.sync_repl_offset,
                g_repl->GetReplLog().WALEndOffset(), state2String(m_ctx.state));
        /*
         * Only execute command after slave fully synced from master
         */
        g_slave_sync_qps.IncMsgCount(1);
        if (SLAVE_STATE_SYNCED == m_ctx.state)
        {
            m_ctx.ResetCallFlags();
            /*
             * todo:
             * call db may block current sync thread, maybe use more threads to exec command better.
             * If use more threads to handle sync commands, there are several new problems:
             * 1. Keep the commands exec order for same key
             *    eg: lpush list a  & lpush list b must be executed in same thread  to keep exec order
             * 2. ?
             */
            //g_db->Call(m_ctx.ctx, cmd);
            m_db_writer.Put(m_ctx.ctx, cmd);
            m_ctx.UpdateSyncOffsetCksm(cmd.GetRawProtocolData());
        }
    }
    void Slave::Routine()
    {
        if (g_db->GetConf().master_host.empty())
        {
            return;
        }
        //ReplayWAL();
        uint32 now = time(NULL);
        if (NULL == m_client)
        {
            ConnectMaster();
            return;
        }
        if (m_ctx.state == SLAVE_STATE_SYNCED || m_ctx.state == SLAVE_STATE_LOADING_SNAPSHOT)
        {
            if (m_ctx.cmd_recved_time > 0 && now - m_ctx.cmd_recved_time >= g_db->GetConf().repl_timeout)
            {
                if (m_ctx.state == SLAVE_STATE_SYNCED)
                {
                    //WARN_LOG("now = %u, ping_recved_time=%u", now, m_ctx.cmd_recved_time);
                    Timeout();
                    return;
                }
            }
        }
        ReportACK();
        //m_routine_ts = now;
    }

    void Slave::ReportACK()
    {
        if (NULL == m_client)
        {
            return;
        }
        if (m_ctx.state == SLAVE_STATE_SYNCED || m_ctx.state == SLAVE_STATE_LOADING_SNAPSHOT)
        {
            if (m_ctx.server_support_psync)
            {
                Buffer buffer;
                RedisCommandFrame ack("REPLCONF");
                ack.AddArg("ACK");
                ack.AddArg(stringfromll(m_ctx.sync_repl_offset));
                RedisCommandEncoder::Encode(buffer, ack);
                m_client->Write(buffer);
            }
        }
        else
        {
            Buffer new_line;
            new_line.Write("\n", 1);
            m_client->Write(new_line);
        }
    }

    void Slave::InfoMaster()
    {
        if (NULL != m_client)
        {
            Buffer info;
            info.Printf("info Server\r\n");
            m_ctx.state = SLAVE_STATE_WAITING_INFO_REPLY;
            m_client->Write(info);
        }
    }

    static int LoadRDBRoutine(SnapshotState state, Snapshot* snapshot, void* cb)
    {
        g_repl->GetIOService().Continue();
        SlaveContext* slave = (SlaveContext*) cb;
        if (slave->state != SLAVE_STATE_LOADING_SNAPSHOT)
        {
            return -1;
        }
        if (DUMPING == state)
        {
            if (snapshot->CachedReplOffset() < g_repl->GetReplLog().WALStartOffset())
            {
                ERROR_LOG("Slave is too slow to load snapshot, while the wal log is full fill from loading snapshot[%llu, %llu].", snapshot->CachedReplOffset(),
                        g_repl->GetReplLog().WALStartOffset());
                return -1;
            }
        }
        return 0;
    }
    void Slave::HandleRedisReply(Channel* ch, RedisReply& reply)
    {
        switch (m_ctx.state)
        {
            case SLAVE_STATE_WAITING_AUTH_REPLY:
            {
                if (reply.type == REDIS_REPLY_ERROR)
                {
                    ERROR_LOG("Recv auth reply error:%s", reply.str.c_str());
                    Close();
                }
                InfoMaster();
                return;
            }
            case SLAVE_STATE_WAITING_INFO_REPLY:
            {
                if (reply.type == REDIS_REPLY_ERROR)
                {
                    ERROR_LOG("Recv info reply error:%s", reply.str.c_str());
                    Close();
                    return;
                }
                const char* redis_ver_key = "redis_version:";
                const char* ardb_ver_key = "ardb_version:";
                m_ctx.server_is_redis = reply.str.find(ardb_ver_key) == std::string::npos;
                if (m_ctx.server_is_redis)
                {
                    size_t start = reply.str.find(redis_ver_key);
                    size_t end = reply.str.find("\n", start);
                    std::string v = reply.str.substr(start + strlen(redis_ver_key), end - start - strlen(redis_ver_key));
                    v = trim_string(v);
                    m_ctx.server_support_psync = (compare_version<3>(v, "2.7.0") >= 0);
                    INFO_LOG("[Slave]Remote master is a Redis %s instance, support partial sync:%u", v.c_str(), m_ctx.server_support_psync);
                }
                else
                {
                    INFO_LOG("[Slave]Remote master is an ardb instance.");
                    m_ctx.server_support_psync = true;
                }
                Buffer replconf;
                replconf.Printf("replconf listening-port %u\r\n", g_db->GetConf().PrimaryPort());
                m_ctx.state = SLAVE_STATE_WAITING_REPLCONF_REPLY;
                ch->Write(replconf);
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
                if (m_ctx.server_support_psync)
                {
                    Buffer sync;
                    if (!m_ctx.server_is_redis)
                    {
                        sync.Printf("psync %s %lld cksm %llu engine %s\r\n",
                                g_repl->GetReplLog().IsReplKeySelfGen() ? "?" : g_repl->GetReplLog().GetReplKey().c_str(), g_repl->GetReplLog().WALEndOffset(),
                                g_repl->GetReplLog().WALCksm(), g_engine_name);
                    }
                    else
                    {
                        sync.Printf("psync %s %lld\r\n", g_repl->GetReplLog().IsReplKeySelfGen() ? "?" : g_repl->GetReplLog().GetReplKey().c_str(),
                                g_repl->GetReplLog().WALEndOffset());
                    }
                    INFO_LOG("Send %s", trim_string(sync.AsString()).c_str());
                    m_ctx.state = SLAVE_STATE_WAITING_PSYNC_REPLY;
                    ch->Write(sync);
                }
                else
                {
                    Buffer sync;
                    sync.Printf("sync\r\n");
                    m_ctx.state = SLAVE_STATE_WAITING_SNAPSHOT;
                    ch->Write(sync);
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
                if (!strcasecmp(ss[0].c_str(), "FULLRESYNC") || !strcasecmp(ss[0].c_str(), "FULLBACKUP"))
                {
                    int64 offset;
                    if (!string_toint64(ss[2], offset))
                    {
                        ERROR_LOG("Invalid psync offset:%s", ss[2].c_str());
                        ch->Close();
                        return;
                    }

                    m_ctx.cached_master_runid = ss[1];
                    m_ctx.cached_master_repl_offset = offset;
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
                        m_ctx.cached_master_repl_cksm = cksm;
                    }

                    m_ctx.state = SLAVE_STATE_WAITING_SNAPSHOT;
                    m_decoder.SwitchToDumpFileDecoder();
                    if (!strcasecmp(ss[0].c_str(), "FULLBACKUP"))
                    {
                        m_ctx.snapshot_path = Snapshot::GetSyncSnapshotPath(BACKUP_DUMP, m_ctx.cached_master_repl_offset, m_ctx.cached_master_repl_cksm);
                        std::string tmppath = m_ctx.snapshot_path + ".tmp";
                        make_dir(tmppath);
                        m_ctx.snapshot.SetFilePath(tmppath);
                        INFO_LOG("[Slave]Create sync backup path:%s", tmppath.c_str());
                        m_decoder.SwitchToBackupSyncDecoder(tmppath);
                    }
                    break;
                }
                else if (!strcasecmp(ss[0].c_str(), "CONTINUE"))
                {
                    m_decoder.SwitchToCommandDecoder();
                    m_ctx.ResetCallFlags();
                    m_db_writer.SetDefaulFlags(m_ctx.ctx.flags);
                    m_db_writer.SetNamespace(m_ctx.ctx, g_repl->GetReplLog().CurrentNamespace());
                    //m_ctx.ctx.ns.SetString(g_repl->GetReplLog().CurrentNamespace(), false);
                    m_ctx.sync_repl_offset = g_repl->GetReplLog().WALEndOffset();
                    m_ctx.state = SLAVE_STATE_SYNCED;
                    INFO_LOG("Slave recv continue from master with current namespace:%s", m_ctx.ctx.ns.AsString().c_str());
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
                ERROR_LOG("Slave client is in invalid state:%s", state2String(m_ctx.state));
                Close();
                break;
            }
        }
    }

    void Slave::LoadSyncedSnapshot()
    {
        m_ctx.snapshot.Close();
        if (0 != m_ctx.snapshot.Rename(m_ctx.snapshot_path))
        {
            if (NULL != m_client)
            {
                m_client->Close();
            }
            return;
        }
        m_ctx.snapshot.MarkDumpComplete();
        m_decoder.SwitchToCommandDecoder();
        m_ctx.state = SLAVE_STATE_LOADING_SNAPSHOT;
        /*
         * set server key to a random string first, if server restart when loading data, it would do a full resync again with another server key.
         * set wal offset&cksm to make sure that this slave could accept&save synced commands when loading snapshot file.
         */
        g_repl->GetReplLog().SetReplKey(random_hex_string(40));
        g_repl->GetReplLog().ResetWALOffsetCksm(m_ctx.cached_master_repl_offset, m_ctx.cached_master_repl_cksm);
        if (g_db->GetConf().slave_cleardb_before_fullresync)
        {
            g_db->FlushAll(m_ctx.ctx);
        }
        INFO_LOG("Start loading snapshot:%s", m_ctx.snapshot.GetPath().c_str());
        m_ctx.cmd_recved_time = time(NULL);
        int ret = m_ctx.snapshot.Reload(LoadRDBRoutine, &m_ctx);
        if (0 != ret)
        {
            if (NULL != m_client)
            {
                m_client->Close();
            }
            WARN_LOG("Failed to load snapshot:%s", m_ctx.snapshot.GetPath().c_str());
            return;
        }
        g_repl->GetReplLog().SetReplKey(m_ctx.cached_master_runid);
        m_ctx.sync_repl_offset = m_ctx.cached_master_repl_offset;
        m_ctx.sync_repl_cksm = m_ctx.cached_master_repl_cksm;
        m_ctx.state = SLAVE_STATE_REPLAYING_WAL;
        ReplayWAL();

        g_snapshot_manager->AddSnapshot(m_ctx.snapshot.GetPath());
    }

    void Slave::HandleBackupSync(Channel* ch, DirSyncStatus& status)
    {
        if (m_ctx.state != SLAVE_STATE_WAITING_SNAPSHOT)
        {
            ERROR_LOG("Invalid state:%s to handler backup sync status.", state2String(m_ctx.state));
            ch->Close();
            return;
        }
        if (status.IsItemSuccess())
        {
            INFO_LOG("Backup file:%s sync success.", status.path.c_str());
        }
        if (status.IsComplete())
        {
            if (status.IsSuccess())
            {
                INFO_LOG("Backup file:%s sync success.", status.path.c_str());
                INFO_LOG("Backup sync success.");
                LoadSyncedSnapshot();
            }
            else
            {
                ERROR_LOG("Failed to sync backup:%s with reason:%s", status.path.c_str(), status.reason.c_str());
                ch->Close();
                return;
            }
        }
    }

    void Slave::HandleRedisDumpChunk(Channel* ch, RedisDumpFileChunk& chunk)
    {
        if (m_ctx.state != SLAVE_STATE_WAITING_SNAPSHOT)
        {
            ERROR_LOG("Invalid state:%s to handler redis dump file chunk.", state2String(m_ctx.state));
            ch->Close();
            return;
        }
        if (chunk.IsFirstChunk())
        {
            m_ctx.snapshot.Close();
            m_ctx.snapshot_path = Snapshot::GetSyncSnapshotPath(m_ctx.server_is_redis ? REDIS_DUMP : ARDB_DUMP, m_ctx.cached_master_repl_offset, m_ctx.cached_master_repl_cksm);
            std::string tmppath = m_ctx.snapshot_path + ".tmp";
            m_ctx.snapshot.OpenWriteFile(tmppath);
            INFO_LOG("[Slave]Create dump file:%s, expected size:%lld, master is redis:%d", m_ctx.snapshot_path.c_str(), chunk.len, m_ctx.server_is_redis);
            m_ctx.snapshot.SetExpectedDataSize(chunk.len);
        }
        if (!chunk.chunk.empty())
        {
            int err = m_ctx.snapshot.Write(chunk.chunk.c_str(), chunk.chunk.size());
            //printf("####write err:%d with %d %d\n", err, chunk.chunk.size(), m_ctx.snapshot.DumpLeftDataSize());
        }
        if (chunk.IsLastChunk())
        {
            LoadSyncedSnapshot();
        }
    }

    void Slave::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e)
    {
        m_ctx.master_last_interaction_time = time(NULL);
        if (e.GetMessage()->IsReply())
        {
            HandleRedisReply(ctx.GetChannel(), e.GetMessage()->reply);
        }
        else if (e.GetMessage()->IsCommand())
        {
            HandleRedisCommand(ctx.GetChannel(), e.GetMessage()->command);
        }
        else if (e.GetMessage()->IsBackupSync())
        {
            HandleBackupSync(ctx.GetChannel(), e.GetMessage()->backup);
        }
        else
        {
            HandleRedisDumpChunk(ctx.GetChannel(), e.GetMessage()->chunk);
        }
    }

    void Slave::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        INFO_LOG("[Slave]Replication connection closed at state:%s", state2String(m_ctx.state));
        if (m_ctx.state == SLAVE_STATE_LOADING_SNAPSHOT)
        {
            WARN_LOG("Maybe remote master is too busy to accept too many write commands while slave is loading synced snapshot file.");
        }
        m_ctx.master_last_interaction_time = m_ctx.master_link_down_time = time(NULL);
        m_client = NULL;
        m_clientid = 0;
        m_db_writer.Clear();
        m_ctx.Clear();
        /*
         * Current instance is disconnect from remote master, can not accept slaves now.
         */
        g_repl->GetMaster().DisconnectAllSlaves();
    }

    void Slave::Timeout()
    {
        WARN_LOG("Master connection timeout.");
        Close();
    }

    int Slave::ConnectMaster()
    {
        if (g_db->GetConf().master_host.empty())
        {
            return -1;
        }
        SocketHostAddress addr(g_db->GetConf().master_host, g_db->GetConf().master_port);
        if (NULL != m_client)
        {
            return 0;
        }
        /*
         * Current instance is syncing data from remote master, can not accept slaves now.
         */
        g_repl->GetMaster().DisconnectAllSlaves();
        m_client = g_repl->GetIOService().NewClientSocketChannel();
        m_clientid = m_client->GetID();
        m_decoder.Clear();
        m_client->GetPipeline().AddLast("decoder", &m_decoder);
        m_client->GetPipeline().AddLast("encoder", &m_encoder);
        m_client->GetPipeline().AddLast("handler", this);
        m_decoder.SwitchToReplyDecoder();
        m_ctx.state = SLAVE_STATE_CONNECTING;
        m_client->Connect(&addr);
        INFO_LOG("[Slave]Connecting master %s:%u", g_db->GetConf().master_host.c_str(), g_db->GetConf().master_port);
        return 0;
    }
    int64 Slave::SyncOffset()
    {
        return m_ctx.sync_repl_offset;
    }
    int64 Slave::SyncQueueSize()
    {
        return m_db_writer.QueueSize();
    }
    int64 Slave::SyncLeftBytes()
    {
        return m_ctx.snapshot.DumpLeftDataSize();
    }
    int64 Slave::LoadLeftBytes()
    {
        if (m_ctx.state == SLAVE_STATE_REPLAYING_WAL)
        {
            return g_repl->GetReplLog().WALEndOffset() - m_ctx.sync_repl_offset;
        }
        return m_ctx.snapshot.ProcessLeftDataSize();
    }

    bool Slave::IsSynced()
    {
        return NULL != m_client && SLAVE_STATE_SYNCED == m_ctx.state;
    }

    bool Slave::IsLoading()
    {
        /*
         * if slave can serve stale data, 'SLAVE_STATE_REPLAYING_WAL' would not considered as 'loading' state.
         */
        return NULL != m_client
                && (SLAVE_STATE_LOADING_SNAPSHOT == m_ctx.state || (!g_db->GetConf().slave_serve_stale_data && SLAVE_STATE_REPLAYING_WAL == m_ctx.state));
    }

    bool Slave::IsConnected()
    {
        return NULL != m_client && (m_ctx.state >= SLAVE_STATE_WAITING_AUTH_REPLY);
    }
    bool Slave::IsSyncing()
    {
        return NULL != m_client && (SLAVE_STATE_WAITING_SNAPSHOT == m_ctx.state);
    }

    void Slave::Stop()
    {
        m_ctx.state = SLAVE_STATE_INVALID;
        m_db_writer.Stop();
        Close();
    }
    void Slave::DoClose()
    {
        if (NULL != m_client)
        {
            m_client->Close();
        }
    }
    static void async_close_callback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->Close();
        }
    }
    void Slave::Close()
    {
        if (m_clientid > 0)
        {
            //async close
            g_repl->GetIOService().AsyncIO(m_clientid, async_close_callback, NULL);
        }
    }

    void Slave::AsyncACKCallback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            g_repl->GetSlave().ReportACK();
        }
    }

    void Slave::SendACK()
    {
        if (m_clientid > 0)
        {
            g_repl->GetIOService().AsyncIO(m_clientid, AsyncACKCallback, NULL);
        }
    }
OP_NAMESPACE_END


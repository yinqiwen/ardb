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

#ifndef REPL_HPP_
#define REPL_HPP_

#include "common/common.hpp"
#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "thread/spin_rwlock.hpp"
#include "swal.h"
#include "context.hpp"
#include "db/db_utils.hpp"
#include <map>
#include "snapshot.hpp"

using namespace ardb::codec;

OP_NAMESPACE_BEGIN
    class Master;
    class Slave;
    class ReplicationService;
    class ReplicationBacklog
    {
        private:
            swal_t* m_wal;
            volatile uint32 m_wal_queue_size;
            //SpinRWLock m_repl_lock;
            void ReCreateWAL();
            static void WriteWALCallback(Channel*, void* data);
            int WriteWAL(const Data& ns, const Buffer& cmd);
            int WriteWAL(const Buffer& cmd, bool lock);
            int DirectWriteWAL(RedisCommandFrame& cmd);
            void FlushSyncWAL();
            friend class Master;
            friend class Slave;
            friend class ReplicationService;
        public:
            ReplicationBacklog();
            int Init();
            void Routine();
            std::string GetReplKey();
            bool IsReplKeySelfGen();
            void SetReplKey(const std::string& str);
            int WriteWAL(const Data& ns, RedisCommandFrame& cmd);
            void Replay(size_t offset, int64_t limit_len, swal_replay_logfunc func, void* data);
            bool IsValidOffsetCksm(int64_t offset, uint64_t cksm);
            uint64_t WALStartOffset(bool lock = true);
            uint64_t WALEndOffset(bool lock = true);
            uint64_t WALCksm(bool lock = true);
            std::string CurrentNamespace();
            void ClearCurrentNamespace();
            void SetCurrentNamespace(const std::string& ns);
            void ResetWALOffsetCksm(uint64_t offset, uint64_t cksm);
            void DebugDumpCache(const std::string& file);
            uint32 WALQueueSize() const
            {
                return m_wal_queue_size;
            }
            ~ReplicationBacklog();
    };

    struct SlaveContext
    {
            Context ctx;
            bool server_is_redis;
            bool server_support_psync;
            uint32_t state;
            std::string cached_master_runid;
            int64 cached_master_repl_offset;
            uint64 cached_master_repl_cksm;
            int64 sync_repl_offset;
            uint64 sync_repl_cksm;
            uint32 cmd_recved_time;
            uint32 master_link_down_time;
            time_t master_last_interaction_time;
            Snapshot snapshot;
            std::string snapshot_path;
            void UpdateSyncOffsetCksm(const Buffer& buffer);
            void Clear();
            void ResetCallFlags();
            SlaveContext() :
                    server_is_redis(false), server_support_psync(false), state(0), cached_master_repl_offset(0), cached_master_repl_cksm(0), sync_repl_offset(
                            0), sync_repl_cksm(0), cmd_recved_time(0), master_link_down_time(0), master_last_interaction_time(0)
            {
            }
    };

    class Slave: public ChannelUpstreamHandler<RedisMessage>
    {
        private:
            Channel* m_client;
            uint32 m_clientid;
            RedisMessageDecoder m_decoder;
            NullRedisReplyEncoder m_encoder;
            SlaveContext m_ctx;
            ClientContext m_client_ctx;
            DBWriter m_db_writer;
            void HandleRedisCommand(Channel* ch, RedisCommandFrame& cmd);
            void HandleRedisReply(Channel* ch, RedisReply& reply);
            void HandleRedisDumpChunk(Channel* ch, RedisDumpFileChunk& chunk);
            void HandleBackupSync(Channel* ch, DirSyncStatus& cmd);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e);
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void Timeout();

            void LoadSyncedSnapshot();
            void ReportACK();
            void InfoMaster();
            int ConnectMaster();
            void ReplayWAL();
            void DoClose();
            static void AsyncACKCallback(Channel* ch, void*);
        public:
            Slave();
            int Init();
            void Routine();
            void Close();
            void Stop();
            size_t ReplayWAL(const void* log, size_t loglen);
            time_t GetMasterLastinteractionTime();
            time_t GetMasterLinkDownTime();
            bool IsSynced();
            bool IsLoading();
            bool IsConnected();
            bool IsSyncing();
            int64 SyncLeftBytes();
            int64 LoadLeftBytes();
            int64 SyncOffset();
            int64 SyncQueueSize();
            void SendACK();

    };

    class SlaveSyncContext;
    typedef TreeSet<SlaveSyncContext*>::Type SlaveSyncContextSet;
    typedef TreeSet<Snapshot*>::Type DataDumpFileSet;
    class Master: public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            SlaveSyncContextSet m_slaves;
            DataDumpFileSet m_cached_snapshots;
            time_t m_repl_noslaves_since;
            time_t m_repl_nolag_since;
            uint32 m_repl_good_slaves_count;
            uint32 m_slaves_count;
            int64 m_sync_full_count;
            int64 m_sync_partial_ok_count;
            int64 m_sync_partial_err_count;

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);

            void SyncSlave(SlaveSyncContext* slave);
            int SendBackupToSlave(SlaveSyncContext* slave);
            int SendSnapshotToSlave(SlaveSyncContext* slave);
            bool IsAllSlaveSyncingCache();
            static void OnSnapshotBackupSendComplete(void* data);
            friend class ReplicationService;
        public:
            Master();
            int Init();
            int Routine();
            void FullResyncSlaves(Snapshot* snapshot);
            void CloseSlaveBySnapshot(Snapshot* snapshot);
            void CloseSlave(SlaveSyncContext* slave);

            void AddSlave(SlaveSyncContext* slave);
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void SetSlavePort(Channel* slave, uint32 port);
            void SyncWAL(SlaveSyncContext* slave);
            size_t ConnectedSlaves();
            int64 FullSyncCount()
            {
                return m_sync_full_count;
            }
            int64 ParitialSyncOKCount()
            {
                return m_sync_partial_ok_count;
            }
            int64 ParitialSyncErrCount()
            {
                return m_sync_partial_err_count;
            }
            void SyncWAL();
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
            uint32 GoodSlavesCount() const
            {
                return m_repl_good_slaves_count;
            }
            ~Master();
    };

    class ReplicationService: public Thread
    {
        private:
            ChannelService* io_serv_;
            Slave slave_;
            Master master_;
            ReplicationBacklog repl_backlog_;
            bool inited_;
            void Run();
            void Routine();
        public:
            ReplicationService();
            int Init();
            bool IsInited() const;
            ChannelService& GetIOService();
            ReplicationBacklog& GetReplLog();
            Master& GetMaster();
            Slave& GetSlave();
            void StopService();

    };
    extern ReplicationService* g_repl;
OP_NAMESPACE_END

#endif /* REPL_HPP_ */

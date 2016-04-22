/*
 * repl.hpp
 *
 *  Created on: 2013-08-29     Author: wqy
 */

#ifndef REPL_HPP_
#define REPL_HPP_

#include "common/common.hpp"
#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "swal.h"
#include "context.hpp"
#include "rdb.hpp"
#include <map>

using namespace ardb::codec;

OP_NAMESPACE_BEGIN
    class ReplicationBacklog
    {
        private:
            swal_t* m_wal;
            void ReCreateWAL();
            static void WriteWALCallback(Channel*, void* data);
            int WriteWAL(const Data& ns, const Buffer& cmd);
            int WriteWAL(const Buffer& cmd);
            void Routine();
            void FlushSyncWAL();
        public:
            ReplicationBacklog();
            int Init();
            void Routine();
            swal_t* GetWAL();
            const char* GetReplKey();
            void SetReplKey(const std::string& str);
            int WriteWAL(const Data& ns, RedisCommandFrame& cmd);
            bool IsValidOffsetCksm(int64_t offset, uint64_t cksm);
            uint64_t WALStartOffset();
            uint64_t WALEndOffset();
            uint64_t WALCksm();
            void ResetWALOffsetCksm(uint64_t offset, uint64_t cksm);
//            void ResetDataOffsetCksm(uint64_t offset, uint64_t cksm);
//            uint64_t DataOffset();
//            uint64_t DataCksm();
//            void UpdateDataOffsetCksm(const Buffer& data);
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
            Buffer replay_cumulate_buffer;
            Snapshot snapshot;
            void UpdateSyncOffsetCksm(const Buffer& buffer);
            void Clear()
            {
                server_is_redis = false;
                server_support_psync = false;
                state = 0;
                cached_master_runid.clear();
                cached_master_repl_offset = 0;
                cached_master_repl_cksm = 0;
                sync_repl_offset = 0;
                sync_repl_cksm = 0;
                snapshot.Close();
            }
            SlaveContext() :
                    server_is_redis(false), server_support_psync(false), state(0), cached_master_repl_offset(0), cached_master_repl_cksm(0), sync_repl_offset(
                            0), sync_repl_cksm(0), cmd_recved_time(0), master_link_down_time(0)
            {
            }
    };

    class Slave: public ChannelUpstreamHandler<RedisMessage>
    {
        private:
            Channel* m_client;
            RedisMessageDecoder m_decoder;
            NullRedisReplyEncoder m_encoder;
            SlaveContext m_ctx;
            //time_t m_routine_ts;
            time_t m_lastinteraction;

            void HandleRedisCommand(Channel* ch, RedisCommandFrame& cmd);
            void HandleRedisReply(Channel* ch, RedisReply& reply);
            void HandleRedisDumpChunk(Channel* ch, RedisDumpFileChunk& chunk);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e);
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void Timeout();

            void InfoMaster();
            int ConnectMaster();
            void ReplayWAL();
            void DoClose();
        public:
            Slave();
            int Init();
            void Routine();
            void Close();
            void Stop();
            void ReplayWAL(const void* log, size_t loglen);
            bool IsSynced();
            bool IsLoading();
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
            int PingSlaves();

            void ClearNilSlave();

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);

            void SyncWAL(SlaveSyncContext* slave);
            void SendSnapshotToSlave(SlaveSyncContext* slave);
            void AddSlave(SlaveSyncContext* slave);
        public:
            Master();
            int Init();
            void FullResyncSlaves(Snapshot* snapshot);
            void CloseSlaveBySnapshot(Snapshot* snapshot);
            void CloseSlave(SlaveSyncContext* slave);
            void SyncSlave(SlaveSyncContext* slave);
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void SetSlavePort(Channel* slave, uint32 port);
            size_t ConnectedSlaves();
            void SyncWAL();
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
            uint32 GoodSlavesCount()const
            {
                return m_repl_good_slaves_count;
            }
            ~Master();
    };

    class ReplicationService: public Thread
    {
        private:
            ChannelService m_io_serv;
            Slave m_slave;
            Master m_master;
            void Run();
        public:
            ReplicationService();
            int Init();
            ChannelService& GetIOService();
            ReplicationBacklog& GetReplLog();
            Master& GetMaster();
            Slave& GetSlave();

    };
    extern ReplicationService* g_repl;
OP_NAMESPACE_END

#endif /* REPL_HPP_ */

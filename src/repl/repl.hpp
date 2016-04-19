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
            const char* GetServerKey();
            void SetServerKey(const std::string& str);
            int WriteWAL(const Data& ns, RedisCommandFrame& cmd);
            bool IsValidOffsetCksm(int64_t offset, uint64_t cksm);
            uint64_t WALStartOffset();
            uint64_t WALEndOffset();
            uint64_t WALCksm();
            void ResetWALOffsetCksm(uint64_t offset, uint64_t cksm);

            void ResetDataOffsetCksm(uint64_t offset, uint64_t cksm);
            uint64_t DataOffset();
            uint64_t DataCksm();
            void UpdateDataOffsetCksm(const Buffer& data);
            ~ReplicationBacklog();
    };

    struct SlaveStatus
    {
            bool server_is_redis;
            bool server_support_psync;
            uint32_t state;
            std::string cached_master_runid;
            int64 cached_master_repl_offset;
            uint64 cached_master_repl_cksm;
            bool replaying_wal;
            Snapshot snapshot;
            void Clear()
            {
                server_is_redis = false;
                server_support_psync = false;
                state = 0;
                cached_master_runid.clear();
                cached_master_repl_offset = 0;
                cached_master_repl_cksm = 0;
                snapshot.Close();
            }
            SlaveStatus() :
                    server_is_redis(false), server_support_psync(false), state(0), cached_master_repl_offset(0), cached_master_repl_cksm(0), replaying_wal(
                            false)
            {
            }
    };

    class Slave: public ChannelUpstreamHandler<RedisMessage>
    {
        private:
            Channel* m_client;
            uint32 m_cmd_recved_time;
            uint32 m_master_link_down_time;
            RedisMessageDecoder m_decoder;
            NullRedisReplyEncoder m_encoder;
            SlaveStatus m_status;
            Context m_slave_ctx;
            time_t m_routine_ts;
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
        public:
            Slave();
            int Init();
            void Routine();
            void Close();
            void Stop();
            void ReplayWAL(const void* log, size_t loglen);
            bool IsSynced();
            const SlaveStatus& GetStatus() const
            {
                return m_status;
            }
    };

    class SlaveContext;
    typedef TreeSet<SlaveContext*>::Type SlaveContextSet;
    class Master: public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            SlaveContextSet m_slaves;
            time_t m_repl_noslaves_since;

            void OnHeartbeat();

            void SyncSlave(SlaveContext* slave);

            void ClearNilSlave();

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);

            void SyncWAL(SlaveContext* slave);
            void SendSnapshotToSlave(SlaveContext* slave);
            int CreateSnapshot(bool is_redis_type);
            static int DumpRDBRoutine(void* cb);
            void AddSlave(SlaveContext* slave);
        public:
            Master();
            int Init();
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void SetSlavePort(Channel* slave, uint32 port);
            size_t ConnectedSlaves();
            void SyncWAL();
            void FullResyncSlaves(bool is_redis_type);
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
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

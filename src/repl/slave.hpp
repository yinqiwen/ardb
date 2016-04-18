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

#ifndef SLAVE_CLIENT_HPP_
#define SLAVE_CLIENT_HPP_
#include "channel/all_includes.hpp"
#include "util/mmap.hpp"
#include "context.hpp"

using namespace ardb::codec;

#define MASTER_SERVER_ADDRESS_NAME "master"

OP_NAMESPACE_BEGIN
    class Comms;
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
                    server_is_redis(false), server_support_psync(false), state(0), cached_master_repl_offset(0), cached_master_repl_cksm(0), replaying_wal(false)
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
OP_NAMESPACE_END

#endif /* SLAVE_CLIENT_HPP_ */

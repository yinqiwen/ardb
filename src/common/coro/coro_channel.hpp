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

#ifndef COMMON_CORO_CORO_CHANNEL_HPP_
#define COMMON_CORO_CORO_CHANNEL_HPP_

#include "common.hpp"
#include "channel/all_includes.hpp"
#include "scheduler.hpp"
#include <stack>

using namespace ardb::codec;
OP_NAMESPACE_BEGIN

    class CoroChannel: public ChannelUpstreamHandler<Buffer>, public Runnable
    {
        protected:
            ChannelService* m_io_serv;
            Channel* m_ch;
            Coroutine* m_coro;
            Coroutine* m_waiting_coro;
            bool m_connect_success;
            Buffer* m_readed_content;
            bool m_timeout;
            int32 m_timeout_taskid;

            void SetConnectResult(bool v);
            void SetReadedContent(Buffer* buf)
            {
                m_readed_content = buf;
            }
            void Run();
            void WakeupCoro();
            int WaitCoro();

            void CreateTimeoutTask(int timeout);
            void CancelTimeoutTask();
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<Buffer>& e);
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
        public:
            CoroChannel(Channel* ch);
            void Init();
            void Close();
            bool IsTimeout();
            bool SyncConnect(Address* addr, int timeout);
            int SyncRead(Buffer& buffer, int timeout);
            int SyncWrite(Buffer& buffer, int timeout);
            ~CoroChannel();
    };
    class RedisCoroChannelHandler;
    class CoroRedisClient: public CoroChannel, public ChannelUpstreamHandler<RedisReply>
    {
        private:
            size_t m_expected_multi_reply_count;
            RedisReplyArray m_multi_replies;
            RedisReplyDecoder m_decoder;
            RedisCommandEncoder m_encoder;
            RedisReply m_error_reply;
            void SetReply(RedisReply* buf);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisReply>& e);
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void FillErrorReply();
            void Clear();
        public:
            CoroRedisClient(Channel* ch);
            void Init();
            RedisReply* SyncCall(RedisCommandFrame& cmd, int timeout);
            RedisReplyArray* SyncMultiCall(RedisCommandFrameArray& cmds, int timeout);
            ~CoroRedisClient();
    };

OP_NAMESPACE_END

#endif /* SRC_COMMON_CORO_CORO_CHANNEL_HPP_ */

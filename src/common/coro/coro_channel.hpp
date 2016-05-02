/*
 * coro_channel.hpp
 *
 *  Created on: 2016年4月30日
 *      Author: wangqiying
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
            Channel* m_ch;
            Coroutine* m_coro;
            bool m_connect_success;
            Buffer* m_readed_content;
            bool m_timeout;

            void SetConnectResult(bool v);
            void SetReadedContent(Buffer* buf)
            {
                m_readed_content = buf;
            }
            void Run();

            void CreateTimeoutTask(int timeout);
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

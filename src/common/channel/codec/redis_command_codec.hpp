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

#ifndef REDIS_FRAME_CODEC_HPP_
#define REDIS_FRAME_CODEC_HPP_

#include "channel/codec/stack_frame_decoder.hpp"
#include "redis_command.hpp"

namespace ardb
{
    namespace codec
    {
        class RedisMessageDecoder;
        class FastRedisCommandDecoder;
        class RedisCommandDecoder: public StackFrameDecoder<RedisCommandFrame>
        {
            protected:
                bool m_ignore_empty;
                static int ProcessInlineBuffer(Buffer& buffer, RedisCommandFrame& frame);
                static int ProcessMultibulkBuffer(Channel* ch, Buffer& buffer, RedisCommandFrame& frame);
                bool Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, RedisCommandFrame& msg);
                friend class RedisMessageDecoder;
                friend class FastRedisCommandDecoder;
            public:
                RedisCommandDecoder(bool ignore_empty = true):m_ignore_empty(ignore_empty)
                {
                }
                static bool Decode(Channel* ch, Buffer& buffer, RedisCommandFrame& msg);
        };

        class FastRedisCommandDecoder: public ChannelUpstreamHandler<Buffer>
        {
            protected:
                bool m_ignore_empty;
                int m_reqtype;
                int m_multibulklen; /* number of multi bulk arguments left to read */
                long m_bulklen; /* length of bulk argument in multi bulk request */
                RedisCommandFrame m_cmd;
                int m_argc;
                void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<Buffer>& e);
                void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
                {
                    ctx.SendUpstream(e);
                }
                void ExceptionCaught(ChannelHandlerContext& ctx, ExceptionEvent& e)
                {
                    ctx.SendUpstream(e);
                }
                int ProcessMultibulkBuffer(Buffer& buffer, std::string& err);
            public:
                FastRedisCommandDecoder(bool ignore_empty = true) :
                    m_ignore_empty(ignore_empty), m_reqtype(0), m_multibulklen(0), m_bulklen(-1), m_argc(0)
                {
                }
        };

        class RedisCommandEncoder: public ChannelDownstreamHandler<RedisCommandFrame>
        {
            private:
                //Buffer m_buffer;
                bool WriteRequested(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);
            public:
                static bool Encode(Buffer& buf, const RedisCommandFrame& cmd);
        };
    }
}

#endif /* REDIS_FRAME_DECODER_HPP_ */

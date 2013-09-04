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

/*
 * redis_message_decoder.hpp
 *
 *  Created on: 2013-9-4
 *      Author: wqy
 */

#ifndef REDIS_MESSAGE_DECODER_HPP_
#define REDIS_MESSAGE_DECODER_HPP_
#include "redis_command_codec.hpp"
#include "redis_reply_codec.hpp"
#include "redis_reply.hpp"
#include "redis_command.hpp"

namespace ardb
{
	namespace codec
	{
		struct RedisMessage
		{
				RedisReply reply;
				RedisCommandFrame command;
				bool isReply;
				RedisMessage() :
						isReply(false)
				{
				}
		};

		class RedisMessageDecoder: public StackFrameDecoder<RedisMessage>
		{
			protected:
				bool m_decode_reply;
				RedisCommandDecoder m_cmd_decoder;
				RedisReplyDecoder m_reply_decoder;
				bool Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, RedisMessage& msg)
				{
					msg.isReply = m_decode_reply;
					if (m_decode_reply)
					{
						return m_reply_decoder.Decode(ctx, channel, buffer, msg.reply);
					}
					return m_cmd_decoder.Decode(ctx, channel, buffer, msg.command);
				}
			public:
				RedisMessageDecoder(bool decode_reply = false, bool allow_chunk= false) :
						m_decode_reply(decode_reply),m_reply_decoder(allow_chunk)
				{
				}

				void SwitchToCommandDecoder()
				{
					m_decode_reply = false;
				}
				void SwitchToReplyDecoder()
				{
					m_decode_reply = true;
				}
		};
	}
}

#endif /* REDIS_MESSAGE_DECODER_HPP_ */

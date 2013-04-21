/*
 * InternalMessageDecoder.hpp
 *
 *  Created on: 2011-1-29
 *      Author: wqy
 */

#ifndef REDIS_REPLY_CODEC_HPP_
#define REDIS_REPLY_CODEC_HPP_
#include "channel/all_includes.hpp"
#include <deque>
#include <string>
#include "redis_reply.hpp"

namespace ardb
{
	namespace codec
	{
		class RedisReplyDecoder: public StackFrameDecoder<RedisReply>
		{
			protected:
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
				        Buffer& buffer, RedisReply& msg);
			public:
				RedisReplyDecoder()
				{
				}
		};

		class RedisReplyEncoder: public ChannelDownstreamHandler<RedisReply>
		{
			private:
				bool WriteRequested(ChannelHandlerContext& ctx,
				        MessageEvent<RedisReply>& e);
			public:
				static bool Encode(Buffer& buf, RedisReply& reply);
		};
	}
}

#endif /* INTERNALMESSAGEDECODER_HPP_ */

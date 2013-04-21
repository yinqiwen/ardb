/*
 * redis_frame_decoder.hpp
 *
 *  Created on: 2011-7-14
 *      Author: qiyingwang
 */

#ifndef REDIS_FRAME_CODEC_HPP_
#define REDIS_FRAME_CODEC_HPP_

#include "channel/all_includes.hpp"
#include "channel/codec/frame_decoder.hpp"
#include "redis_command.hpp"

namespace ardb
{
	namespace codec
	{
		class RedisCommandDecoder: public StackFrameDecoder<RedisCommandFrame>
		{
			protected:
				int ProcessInlineBuffer(ChannelHandlerContext& ctx,
				        Buffer& buffer, RedisCommandFrame& frame);
				int ProcessMultibulkBuffer(ChannelHandlerContext& ctx,
				        Buffer& buffer, RedisCommandFrame& frame);
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
				        Buffer& buffer, RedisCommandFrame& msg);
			public:
				RedisCommandDecoder()
				{
				}
		};

		class RedisCommandEncoder: public ChannelDownstreamHandler<
		        RedisCommandFrame>
		{
			private:
				bool WriteRequested(ChannelHandlerContext& ctx,
				        MessageEvent<RedisCommandFrame>& e);
			public:
				static bool Encode(Buffer& buf, RedisCommandFrame& cmd);
		};
	}
}

#endif /* REDIS_FRAME_DECODER_HPP_ */

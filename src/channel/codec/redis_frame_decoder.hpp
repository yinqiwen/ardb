/*
 * redis_frame_decoder.hpp
 *
 *  Created on: 2011-7-14
 *      Author: qiyingwang
 */

#ifndef REDIS_FRAME_DECODER_HPP_
#define REDIS_FRAME_DECODER_HPP_

#include "channel/all_includes.hpp"
#include "channel/codec/frame_decoder.hpp"
#include <deque>
#include <string>

namespace ardb
{
	namespace codec
	{
		class RedisFrameDecoder;
		typedef std::deque<std::string> ArgumentArray;
		class RedisCommandFrame
		{
			private:
				bool m_is_inline;
				bool m_cmd_seted;
				std::string m_cmd;
				ArgumentArray m_args;
				inline void FillNextArgument(Buffer& buf, size_t len){
					const char* str = buf.GetRawReadBuffer();
					buf.AdvanceReadIndex(len);
					if (m_cmd_seted)
					{
						m_args.push_back(std::string(str, len));
					} else
					{
						m_cmd.append(str, len);
						m_cmd_seted = true;
					}
				}
				friend class RedisFrameDecoder;
			public:
				RedisCommandFrame() :
						m_is_inline(false),m_cmd_seted(false)
				{
				}
				bool IsInLine()
				{
					return m_is_inline;
				}
				ArgumentArray& GetArguments()
				{
					return m_args;
				}
				std::string& GetCommand()
				{
					return m_cmd;
				}
				std::string* GetArgument(uint32 index);
				void Clear();
				~RedisCommandFrame();
		};

		//typedef std::list<std::string> RedisCommandFrame;
		class RedisFrameDecoder: public StackFrameDecoder<RedisCommandFrame>
		{
			protected:
				int ProcessInlineBuffer(ChannelHandlerContext& ctx,
						Buffer& buffer, RedisCommandFrame& frame);
				int ProcessMultibulkBuffer(ChannelHandlerContext& ctx,
						Buffer& buffer, RedisCommandFrame& frame);
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
						Buffer& buffer, RedisCommandFrame& msg);
			public:
				RedisFrameDecoder()
				{
				}

		};
	}
}

#endif /* REDIS_FRAME_DECODER_HPP_ */

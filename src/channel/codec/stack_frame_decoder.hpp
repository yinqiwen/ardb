/*
 * FrameDecoder.hpp
 *
 *  Created on: 2011-1-25
 *      Author: qiyingwang
 */

#ifndef NOVA_STACK_FRAMEDECODER_HPP_
#define NOVA_STACK_FRAMEDECODER_HPP_

#include "channel/channel_upstream_handler.hpp"
#include "util/buffer.hpp"

using ardb::Buffer;

namespace ardb
{
	namespace codec
	{

		/**
		 * 在基于流的协议中(如TCP), 消息发送过程中可能会发生组合，例如发送下面三个消息：
		 *
		 *  +-----+-----+-----+
		 *  | ABC | DEF | GHI |
		 *  +-----+-----+-----+
		 * 服务端可能收到的消息内容分段如下：
		 *  +----+-------+---+---+
		 *  | AB | CDEFG | H | I |
		 *  +----+-------+---+---+

		 * FrameDecoder 则帮助重新对收到的消息分段组合，使之和发送过程的消息保持一致
		 *  +-----+-----+-----+
		 *  | ABC | DEF | GHI |
		 *  +-----+-----+-----+
		 *
		 */
		template<typename T>
		class StackFrameDecoder: public ChannelUpstreamHandler<Buffer>
		{
			private:
				//bool m_unfold;
				Buffer m_cumulation;
				void CallDecode(ChannelHandlerContext& context,
						Channel* channel, Buffer& cumulation)
				{
					while (cumulation.Readable())
					{
						uint32 oldReadableBytes = cumulation.ReadableBytes();
						T msg;
						bool ret = Decode(context, channel, cumulation, msg);
						if (!ret)
						{
							if (oldReadableBytes == cumulation.ReadableBytes())
							{
								// Seems like more data is required.
								// Let us wait for the next notification.
								break;
							} else
							{
								// Previous data has been discarded.
								// Probably it is reading on.
								continue;
							}
						} else if (oldReadableBytes
								== cumulation.ReadableBytes())
						{
							//do sth.
							//throw new IllegalStateException(
							//        "decode() method must read at least one byte "
							//                + "if it returned a frame (caused by: "
							//                + getClass() + ")");

						}

						fire_message_received<T>(context, &msg, NULL);
					}
				}
				void Cleanup(ChannelHandlerContext& ctx, ChannelStateEvent& e)
				{
					if (!m_cumulation.Readable())
					{
						ctx.SendUpstream(e);
						return;
					}

					// Make sure all frames are read before notifying a closed channel.
					CallDecode(ctx, ctx.GetChannel(), m_cumulation);

					// Call decodeLast() finally.  Please note that decodeLast() is
					// called even if there's nothing more to read from the buffer to
					// notify a user that the connection was closed explicitly.
					T msg;

					bool ret = DecodeLast(ctx, ctx.GetChannel(), m_cumulation,
							msg);
					if (ret)
					{
						fire_message_received<T>(ctx, &msg, NULL);
					}
					ctx.SendUpstream(e);
				}
			protected:
				virtual bool Decode(ChannelHandlerContext& ctx,
						Channel* channel, Buffer& buffer, T& msg) = 0;
				virtual bool DecodeLast(ChannelHandlerContext& ctx,
						Channel* channel, Buffer& buffer, T& msg)
				{
					return Decode(ctx, channel, buffer, msg);
				}
			public:
				StackFrameDecoder()
				{
				}
				void MessageReceived(ChannelHandlerContext& ctx,
						MessageEvent<Buffer>& e)
				{
					Buffer* input = e.GetMessage();

					if (m_cumulation.Readable())
					{
						m_cumulation.DiscardReadedBytes();
						m_cumulation.Write(input, input->ReadableBytes());
						CallDecode(ctx, e.GetChannel(), m_cumulation);
					} else
					{
						CallDecode(ctx, e.GetChannel(), *input);
						if (input->Readable())
						{
							m_cumulation.Write(input, input->ReadableBytes());
						}
					}
				}
				void ChannelClosed(ChannelHandlerContext& ctx,
						ChannelStateEvent& e)
				{
					Cleanup(ctx, e);
				}
				void ExceptionCaught(ChannelHandlerContext& ctx,
						ExceptionEvent& e)
				{
					ctx.SendUpstream(e);
				}
				virtual ~StackFrameDecoder()
				{
				}
		};
	}
}

#endif /* FRAMEDECODER_HPP_ */

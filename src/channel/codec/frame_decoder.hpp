/*
 * FrameDecoder.hpp
 *
 *  Created on: 2011-1-25
 *      Author: qiyingwang
 */

#ifndef NOVA_FRAMEDECODER_HPP_
#define NOVA_FRAMEDECODER_HPP_

#include "channel/channel_upstream_handler.hpp"
#include "util/buffer.hpp"

using namespace ardb;
using ardb::Buffer;
namespace ardb
{
	namespace codec
	{
		template<typename T>
		struct FrameDecodeResult
		{
				T* msg;
				typename ardb::Type<T>::Destructor* destructor;
				FrameDecodeResult(T* obj = NULL,
						typename ardb::Type<T>::Destructor* d = NULL) :
						msg(obj), destructor(d)
				{
				}
		};

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
		class FrameDecoder: public ChannelUpstreamHandler<Buffer>
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
						FrameDecodeResult<T> frame = Decode(context, channel,
								cumulation);
						if (frame.msg == NULL)
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

						fire_message_received(context, frame.msg,
								frame.destructor);
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
					FrameDecodeResult<T> partialFrame = DecodeLast(ctx,
							ctx.GetChannel(), m_cumulation);
					if (partialFrame.msg != NULL)
					{
						fire_message_received(ctx, partialFrame.msg,
								partialFrame.destructor);
					}
					ctx.SendUpstream(e);
				}
			protected:
				virtual FrameDecodeResult<T> Decode(ChannelHandlerContext& ctx,
						Channel* channel, Buffer& buffer) = 0;
				virtual FrameDecodeResult<T> DecodeLast(
						ChannelHandlerContext& ctx, Channel* channel,
						Buffer& buffer)
				{
					return Decode(ctx, channel, buffer);
				}
			public:
				FrameDecoder()
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
				virtual ~FrameDecoder()
				{
				}
		};
	}
}

#endif /* FRAMEDECODER_HPP_ */

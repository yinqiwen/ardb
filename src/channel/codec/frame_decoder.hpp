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

#ifndef NOVA_FRAMEDECODER_HPP_
#define NOVA_FRAMEDECODER_HPP_

#include "channel/channel_upstream_handler.hpp"
#include "buffer/buffer.hpp"

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
		 * In a stream-based transport such as TCP/IP, packets can be fragmented and reassembled
		 * during transmission even in a LAN environment. For example, let us assume you have
		 * received three packets:
		 *
		 *  +-----+-----+-----+
		 *  | ABC | DEF | GHI |
		 *  +-----+-----+-----+
		 * because of the packet fragmentation, a server can receive them like the following:
		 *  +----+-------+---+---+
		 *  | AB | CDEFG | H | I |
		 *  +----+-------+---+---+

		 * FrameDecoder helps you defrag the received packets into one or more meaningful frames
		 * that could be easily understood by the application logic. In case of the example above,
		 * your FrameDecoder implementation could defrag the received packets like the following:
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

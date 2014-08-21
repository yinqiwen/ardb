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

#ifndef NOVA_CHANNELDOWNSTREAMHANDLER_HPP_
#define NOVA_CHANNELDOWNSTREAMHANDLER_HPP_
#include "channel/channel_handler.hpp"
#include "channel/channel_state_event.hpp"
#include "channel/exception_event.hpp"
#include "channel/message_event.hpp"
#include "channel/channel_handler_context.hpp"
namespace ardb
{

	/**
	 * downstream channel event's handler template
	 * @see channel_pipeline.hpp
	 */
	template<typename T>
	class ChannelDownstreamHandler: public AbstractChannelHandler<T>
	{
		protected:
			virtual void OpenRequested(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendDownstream(e);
			}
			virtual void BindRequested(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendDownstream(e);
			}
			virtual void CloseRequested(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendDownstream(e);
			}
			virtual void ConnectRequested(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendDownstream(e);
			}
			virtual bool WriteRequested(ChannelHandlerContext& ctx,
					MessageEvent<T>& e) = 0;
			bool CanHandleDownstream()
			{
				return true;
			}
		public:

			inline bool HandleStreamEvent(ChannelHandlerContext& ctx,
					MessageEvent<T>& e)
			{
				return WriteRequested(ctx, e);
			}

			bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ExceptionEvent& e)
			{
				return true;
			}
			inline bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				//ChannelStateEvent& stateEvent = (ChannelStateEvent&) e;
				switch (e.GetState())
				{
					case CLOSED:
					{
						CloseRequested(ctx, e);
						break;
					}
					case BOUND:
					{
						BindRequested(ctx, e);
						break;
					}
					case CONNECTED:
					{
						ConnectRequested(ctx, e);
						break;
					}
					default:
					{
						ctx.SendDownstream(e);
						break;
					}
				}
				return true;
			}
			virtual ~ChannelDownstreamHandler()
			{
			}
	};
}

#endif /* CHANNELDOWNSTREAMHANDLER_HPP_ */

/*
 * ChannelUpstreamHandler.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_CHANNELUPSTREAMHANDLER_HPP_
#define NOVA_CHANNELUPSTREAMHANDLER_HPP_
#include "channel/channel_handler.hpp"
#include "channel/channel_state_event.hpp"
#include "channel/exception_event.hpp"
#include "channel/message_event.hpp"
#include "channel/channel_handler_context.hpp"
namespace ardb
{
	/**
	 * 处理上行channel event的handler的模版基类
	 * @see channel_pipeline.hpp
	 */
	template<typename T>
	class ChannelUpstreamHandler: public AbstractChannelHandler<T>
	{
		protected:
			virtual void ChannelBound(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void ChannelConnected(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void ChannelOpen(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void ExceptionCaught(ChannelHandlerContext& ctx,
					ExceptionEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void WriteComplete(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				ctx.SendUpstream(e);
			}
			virtual void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<T>& e) = 0;
			bool CanHandleUpstream()
			{
				return true;
			}
		public:
			inline bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ChannelStateEvent& e)
			{
				switch (e.GetState())
				{
					case OPEN:
					{
						ChannelOpen(ctx, e);
						break;
					}
					case CLOSED:
					{
						ChannelClosed(ctx, e);
						break;
					}
					case BOUND:
					{
						ChannelBound(ctx, e);
						break;
					}
					case CONNECTED:
					{
						ChannelConnected(ctx, e);
						break;
					}
					case FLUSH:
					{
						WriteComplete(ctx, e);
						break;
					}
					default:
					{
						ctx.SendUpstream(e);
						break;
					}
				}
				return true;
			}

			inline bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ExceptionEvent& e)
			{
				ExceptionCaught(ctx, e);
				return true;
			}

			inline bool HandleStreamEvent(ChannelHandlerContext& ctx,
					MessageEvent<T>& e)
			{
				MessageReceived(ctx, e);
				return true;
			}

			virtual ~ChannelUpstreamHandler()
			{
			}
	};
}

#endif /* CHANNELUPSTREAMHANDLER_HPP_ */

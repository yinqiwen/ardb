/*
 * ChannelDownstreamHandler.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
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
	 * 处理下行channel event的handler的模版基类
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

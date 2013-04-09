/*
 * AllInclude.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_CHANNEL_ALLINCLUDE_HPP_
#define NOVA_CHANNEL_ALLINCLUDE_HPP_

#include "common.hpp"
#include "channel/channel.hpp"
#include "channel/channel_event.hpp"
#include "channel/channel_handler.hpp"
#include "channel/channel_pipeline.hpp"
#include "channel/channel_upstream_handler.hpp"
#include "channel/channel_downstream_handler.hpp"
#include "channel/channel_state_event.hpp"
#include "channel/message_event.hpp"
#include "channel/exception_event.hpp"
#include "channel/channel_helper.hpp"
#include "channel/channel_service.hpp"

#include "channel/socket/serversocket_channel.hpp"
#include "channel/socket/socket_channel.hpp"
#include "channel/socket/clientsocket_channel.hpp"
#include "channel/socket/datagram_channel.hpp"
#include "channel/timer/timer_channel.hpp"
#include "channel/signal/signal_channel.hpp"
#include "channel/signal/soft_signal_channel.hpp"

#include "channel/codec/frame_decoder.hpp"
#include "channel/codec/int_header_frame_decoder.hpp"
#include "channel/codec/delimiter_frame_decoder.hpp"
#include <errno.h>

namespace ardb
{

	template<typename T>
	bool ChannelPipeline::SendUpstream(ChannelHandlerContext* ctx, T& e)
	{
		RETURN_FALSE_IF_NULL(ctx);
		ChannelHandler* handler = ctx->GetHandler();
		return handler->HandleStreamEvent(*ctx, e);
	}

	template<typename T>
	bool ChannelPipeline::SendUpstream(ChannelHandlerContext* ctx,
			MessageEvent<T>& e)
	{
		RETURN_FALSE_IF_NULL(ctx);

		ChannelHandler* base = ctx->GetHandler();
		//ChannelUpstreamHandler<T> * handler = dynamic_cast<ChannelUpstreamHandler<T>*>(base);
		ChannelUpstreamHandler<T> * handler = NULL;
		if (ChannelHandlerHelper<T>::CanHandleUpMessageEvent(base))
		{
			handler = static_cast<ChannelUpstreamHandler<T>*>(base);
		}
		if (NULL == handler)
		{
			return ctx->SendUpstream(e);
		} else
		{
			return handler->HandleStreamEvent(*ctx, e);
		}
	}

	template<typename T>
	bool ChannelPipeline::SendDownstream(ChannelHandlerContext* ctx, T& e)
	{
		RETURN_FALSE_IF_NULL(ctx);
		ChannelHandler* handler = ctx->GetHandler();
		return handler->HandleStreamEvent(*ctx, e);
	}

	template<typename T>
	bool ChannelPipeline::SendDownstream(ChannelHandlerContext* ctx,
			MessageEvent<T>& e)
	{
		RETURN_FALSE_IF_NULL(ctx);

		ChannelHandler* base = ctx->GetHandler();
		//ChannelDownstreamHandler<T> * handler = dynamic_cast<ChannelDownstreamHandler<T>*>(base);
		ChannelDownstreamHandler<T> * handler = NULL;
		if (ChannelHandlerHelper<T>::CanHandleDownMessageEvent(base))
		{
			handler = static_cast<ChannelDownstreamHandler<T>*>(base);
		}
		if (NULL == handler)
		{
			return ctx->SendDownstream(e);
		} else
		{
			return handler->HandleStreamEvent(*ctx, e);
		}
	}

	template<typename T>
	bool ChannelPipeline::SendUpstream(T& event)
	{
		ChannelHandlerContext* ctx = GetActualUpstreamContext(m_head);
		RETURN_FALSE_IF_NULL(ctx);
		return SendUpstream(ctx, event);
	}

	template<typename T>
	bool ChannelPipeline::SendDownstream(T& event)
	{
		ChannelHandlerContext* ctx = GetActualDownstreamContext(m_tail);
		if (NULL == ctx)
		{
			return GetChannel()->GetService().EventSunk(this, event);
		}
		return SendDownstream(ctx, event);
	}

	template<typename T>
	bool ChannelHandlerContext::SendDownstream(T& e)
	{
		ChannelHandlerContext* prev = m_pipeline.GetActualDownstreamContext(
				m_prev);
		if (NULL == prev)
		{
			return m_pipeline.GetChannel()->GetService().EventSunk(&m_pipeline,
					e);
		} else
		{
			return m_pipeline.SendDownstream(prev, e);
		}
	}

	template<typename T>
	bool ChannelHandlerContext::SendUpstream(T& e)
	{
		ChannelHandlerContext* next = m_pipeline.GetActualUpstreamContext(
				m_next);
		if (NULL != next)
		{
			return m_pipeline.SendUpstream(next, e);
		} else
		{
			return false;
		}
	}
}

#endif /* ALLINCLUDE_HPP_ */

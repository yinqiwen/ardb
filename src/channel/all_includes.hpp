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
#include "channel/codec/redis_message_codec.hpp"
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
		ChannelUpstreamHandler<T> * handler = dynamic_cast<ChannelUpstreamHandler<T>*>(base);
		//ChannelUpstreamHandler<T> * handler = NULL;
		//if (ChannelHandlerHelper<T>::CanHandleUpMessageEvent(base))
		//{
		//	handler = static_cast<ChannelUpstreamHandler<T>*>(base);
		//}
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
		ChannelDownstreamHandler<T> * handler = dynamic_cast<ChannelDownstreamHandler<T>*>(base);
		//ChannelDownstreamHandler<T> * handler = NULL;
		//if (ChannelHandlerHelper<T>::CanHandleDownMessageEvent(base))
		//{
		//	handler = static_cast<ChannelDownstreamHandler<T>*>(base);
		//}
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

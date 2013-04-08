/*
 * ChannelHandlerContext.cpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */
#include "channel/all_includes.hpp"

using namespace rddb;

ChannelHandlerContext::ChannelHandlerContext(ChannelPipeline& pipeline,
        ChannelHandlerContext* prev, ChannelHandlerContext* next,
        const string& name, ChannelHandler* handler) :
	m_prev(prev), m_next(next), m_pipeline(pipeline), m_name(name), m_handler(
	        handler), m_canHandleUpstream(false), m_canHandleDownstream(false)
{
	m_canHandleUpstream = m_handler->CanHandleUpstream();
	m_canHandleDownstream = m_handler->CanHandleDownstream();
}

Channel* ChannelHandlerContext::GetChannel()
{
	return GetPipeline().GetChannel();
}

//bool ChannelHandlerContext::SendDownstream(ChannelEvent& e)
//{
//    ChannelHandlerContext* prev = m_pipeline.GetActualDownstreamContext(m_prev);
//    if (NULL == prev)
//    {
//        return m_pipeline.GetSink()->EventSunk(&m_pipeline, e);
//    }
//    else
//    {
//        return m_pipeline.SendDownstream(prev, e);
//    }
//}
//
//bool ChannelHandlerContext::SendUpstream(ChannelEvent& e)
//{
//    ChannelHandlerContext* next = m_pipeline.GetActualUpstreamContext(m_next);
//    if (NULL != next)
//    {
//        return m_pipeline.SendUpstream(next, e);
//    }
//    else
//    {
//    	return true;
//        //return m_pipeline.GetSink()->eventPop(&m_pipeline, e);
//    }
//}

Timer& ChannelHandlerContext::GetTimer()
{
	return m_pipeline.GetChannel()->GetService().GetTimer();
}

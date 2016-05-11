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

#include "channel/all_includes.hpp"

using namespace ardb;

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

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

#define CHECK_DUPLICATE_NAME(name)          \
	    do{                                 \
		     if (m_name2ctx.count(name) > 0)\
			   	return NULL;               \
		}while(0)

ChannelPipeline::ChannelPipeline() :
	m_channel(NULL), m_head(NULL), m_tail(NULL)
{
}

ChannelHandlerContext* ChannelPipeline::Init(const string& name,
        ChannelHandler* handler)
{
	ChannelHandlerContext* ctx = NULL;
	NEW(ctx, ChannelHandlerContext(*this, NULL, NULL, name, handler));
	if (NULL == ctx)
	{
		return NULL;
	}
	m_head = ctx;
	m_tail = ctx;
	m_name2ctx[name] = ctx;
	return ctx;
}

//ChannelPipeline& ChannelPipeline::operator=(const ChannelPipeline& pipeline)
//{
//	ChannelHandlerContext* ctx = pipeline.m_head;
//	while(NULL  != ctx)
//	{
//		AddLast(ctx->GetName(), ctx->GetHandler());
//		ctx = ctx->GetNext();
//	}
//	return *this;
//}

bool ChannelPipeline::CheckDuplicateName(const string& name)
{
	if (m_name2ctx.count(name) > 0)
	{
		//do sth;
		return false;
	}
	return true;
}

ChannelHandlerContext* ChannelPipeline::PriAddFirst(const string& name,
        ChannelHandler* handler)
{
	if (m_name2ctx.empty())
	{
		return Init(name, handler);
	}
	else
	{
		CHECK_DUPLICATE_NAME(name);
		ChannelHandlerContext* oldHead = m_head;
		ChannelHandlerContext* newHead = NULL;
		NEW(newHead, ChannelHandlerContext(*this,NULL, oldHead, name, handler));
		RETURN_NULL_IF_NULL(newHead);
		oldHead->SetPrev(newHead);
		m_head = newHead;
		m_name2ctx[name] = newHead;
		return newHead;
	}
}

ChannelHandlerContext* ChannelPipeline::PriAddLast(const string& name,
        ChannelHandler* handler)
{
	if (m_name2ctx.empty())
	{
		return Init(name, handler);
		//return true;
	}
	else
	{
		CHECK_DUPLICATE_NAME(name);
		ChannelHandlerContext* oldTail = m_tail;
		ChannelHandlerContext* newTail = NULL;
		NEW(newTail, ChannelHandlerContext(*this, oldTail, NULL, name, handler));
		RETURN_NULL_IF_NULL(newTail);
		oldTail->SetNext(newTail);
		m_tail = newTail;
		m_name2ctx[name] = newTail;
		return newTail;
	}
}

ChannelHandlerContext* ChannelPipeline::PriAddBefore(const string& baseName,
        const string& name, ChannelHandler* handler)
{
	ChannelHandlerContext* ctx = GetContext(baseName);
	RETURN_NULL_IF_NULL(ctx);
	if (ctx == m_head)
	{
		return PriAddFirst(name, handler);
	}
	else
	{
		CHECK_DUPLICATE_NAME(name);
		ChannelHandlerContext* newCtx = NULL;
		NEW(newCtx, ChannelHandlerContext(*this, ctx->GetPrev(), ctx, name, handler));
		RETURN_NULL_IF_NULL(newCtx);
		ctx->GetPrev()->SetNext(newCtx);
		ctx->SetPrev(newCtx);
		m_name2ctx[name] = newCtx;
		return newCtx;
	}
}

ChannelHandlerContext* ChannelPipeline::PriAddAfter(const string& baseName,
        const string& name, ChannelHandler* handler)
{
	ChannelHandlerContext* ctx = GetContext(baseName);
	RETURN_NULL_IF_NULL(ctx);
	if (ctx == m_tail)
	{
		return PriAddLast(name, handler);
	}
	else
	{
		CHECK_DUPLICATE_NAME(name);
		ChannelHandlerContext* newCtx = NULL;
		NEW(newCtx, ChannelHandlerContext(*this, ctx, ctx->GetNext(), name, handler));
		RETURN_NULL_IF_NULL(newCtx);
		ctx->GetNext()->SetPrev(newCtx);
		ctx->SetNext(newCtx);
		m_name2ctx[name] = newCtx;
		return newCtx;
	}
}

void ChannelPipeline::Clear()
{
	m_head = NULL;
	m_tail = NULL;
	ChannelHandlerContextTable::iterator it = m_name2ctx.begin();
	while (it != m_name2ctx.end())
	{
		ChannelHandlerContext* ctx = it->second;
		DELETE(ctx);
		it++;
	}
	m_name2ctx.clear();
}

ChannelHandler* ChannelPipeline::Remove(ChannelHandlerContext* ctx)
{
	RETURN_NULL_IF_NULL(ctx);
	ChannelHandler* handler = ctx->GetHandler();
	if (m_head == m_tail)
	{
		m_head = NULL;
		m_tail = NULL;
		m_name2ctx.clear();
		DELETE(ctx);
		return handler;
	}
	else if (ctx == m_head)
	{
		return RemoveFirst();
	}
	else if (ctx == m_tail)
	{
		return RemoveLast();
	}
	else
	{
		//callBeforeRemove(ctx);
		ChannelHandlerContext* prev = ctx->GetPrev();
		ChannelHandlerContext* next = ctx->GetNext();
		prev->SetNext(next);
		next->SetPrev(prev);
		m_name2ctx.erase(ctx->GetName());
		DELETE(ctx);
		//callAfterRemove(ctx);
		return handler;
	}
}

ChannelHandler* ChannelPipeline::Remove(const string& name)
{
	ChannelHandlerContext* ctx = GetContext(name);
	return Remove(ctx);
}

ChannelHandler* ChannelPipeline::RemoveFirst()
{
	ChannelHandlerContext* oldHead = m_head;
	RETURN_NULL_IF_NULL(oldHead);
	//callBeforeRemove(oldHead);
	ChannelHandler* handler = oldHead->GetHandler();
	if (NULL == oldHead->GetNext())
	{
		m_head = NULL;
		m_tail = NULL;
		m_name2ctx.clear();
	}
	else
	{
		oldHead->GetNext()->SetPrev(NULL);
		m_head = oldHead->GetNext();
		m_name2ctx.erase(oldHead->GetName());
	}
	DELETE(oldHead);
	//callAfterRemove(oldHead);
	return handler;
}

ChannelHandler* ChannelPipeline::RemoveLast()
{

	ChannelHandlerContext* oldTail = m_tail;
	RETURN_NULL_IF_NULL(m_tail);
	//callBeforeRemove(oldTail);
	ChannelHandler* handler = oldTail->GetHandler();
	if (NULL == oldTail->GetPrev())
	{
		m_head = NULL;
		m_tail = NULL;
		m_name2ctx.clear();
		DELETE(oldTail);
	}
	else
	{
		oldTail->GetPrev()->SetNext(NULL);
		m_tail = oldTail->GetPrev();
		m_name2ctx.erase(oldTail->GetName());
	}
	//callBeforeRemove(oldTail);
	DELETE(oldTail);
	return handler;
}

ChannelHandlerContext* ChannelPipeline::GetContext(const string& name)
{
	if (m_name2ctx.count(name) > 0)
	{
		return m_name2ctx[name];
	}
	return NULL;
}

ChannelHandlerContext* ChannelPipeline::GetActualUpstreamContext(
        ChannelHandlerContext* ctx)
{
	RETURN_NULL_IF_NULL(ctx);

	ChannelHandlerContext* realCtx = ctx;
	while (!realCtx->CanHandleUpstream())
	{
		realCtx = realCtx->GetNext();
		RETURN_NULL_IF_NULL(realCtx);
	}
	return realCtx;
}

ChannelHandlerContext* ChannelPipeline::GetActualDownstreamContext(
        ChannelHandlerContext* ctx)
{
	RETURN_NULL_IF_NULL(ctx);

	ChannelHandlerContext* realCtx = ctx;
	while (!realCtx->CanHandleDownstream())
	{
		realCtx = realCtx->GetPrev();
		RETURN_NULL_IF_NULL(realCtx);
	}
	return realCtx;
}

ChannelHandler* ChannelPipeline::Get(const string& name)
{
	ChannelHandlerContext* ctx = GetContext(name);
	RETURN_NULL_IF_NULL(ctx);
	return ctx->GetHandler();
}

void ChannelPipeline::GetAll(vector<ChannelHandler*> handlerlist)
{
	ChannelHandlerContext* ctx = m_head;
	while(NULL != ctx)
	{
		handlerlist.push_back(ctx->GetHandler());
		ctx = ctx->GetNext();
	}
}

ChannelPipeline::~ChannelPipeline()
{
	Clear();
}

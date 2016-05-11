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

#ifndef NOVA_CHANNELPIPELINE_HPP_
#define NOVA_CHANNELPIPELINE_HPP_
#include "common.hpp"
#include "channel/message_event.hpp"
#include "channel/channel_downstream_handler.hpp"
#include "channel/channel_upstream_handler.hpp"
#include "channel/channel_handler_context.hpp"
#include <map>
#include <vector>
#include <string>
using std::vector;
using std::string;
namespace ardb
{
	/**
	 *  +----------------------------------------+---------------+
	 *  |                  ChannelPipeline       |               |
	 *  |                                       \|/              |
	 *  |  +----------------------+  +-----------+------------+  |
	 *  |  | Upstream Handler  N  |  | Downstream Handler  1  |  |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |            /|\                         |               |
	 *  |             |                         \|/              |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |  | Upstream Handler N-1 |  | Downstream Handler  2  |  |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |            /|\                         .               |
	 *  |             .                          .               |
	 *  |     [ sendUpstream() ]        [ sendDownstream() ]     |
	 *  |     [ + INBOUND data ]        [ + OUTBOUND data  ]     |
	 *  |             .                          .               |
	 *  |             .                         \|/              |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |  | Upstream Handler  2  |  | Downstream Handler M-1 |  |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |            /|\                         |               |
	 *  |             |                         \|/              |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |  | Upstream Handler  1  |  | Downstream Handler  M  |  |
	 *  |  +----------+-----------+  +-----------+------------+  |
	 *  |            /|\                         |               |
	 *  +-------------+--------------------------+---------------+
	 *                |                         \|/
	 *  +-------------+--------------------------+---------------+
	 *  |             |                          |               |
	 *  |     [ Channel.Read() ]          [ Channel.Write() ]    |
	 *  |                                                        |
	 *  |                     Internal I/O                       |
	 *  +--------------------------------------------------------+
	 *
	 *  Example of register handlers to  pipline:
	 *   Channel* ch =  ...;
	 *   ChannelPipeline& p = ch->GetPipeline();
	 *   p.addLast("1", new UpstreamHandlerA());
	 *   p.addLast("2", new UpstreamHandlerB());
	 *   p.addLast("3", new DownstreamHandlerA());
	 *   p.addLast("4", new DownstreamHandlerB());
	 *   p.addLast("5", new UpstreamHandlerX());
	 */

	class Channel;
	class ChannelHandlerContext;
	class ChannelPipeline
	{
		private:
			Channel* m_channel;
			ChannelHandlerContext* m_head;
			ChannelHandlerContext* m_tail;
			typedef TreeMap<string, ChannelHandlerContext*>::Type ChannelHandlerContextTable;
			ChannelHandlerContextTable m_name2ctx;
			ChannelHandlerContext* Init(const string& name,
					ChannelHandler* handler);

			bool CheckDuplicateName(const string& name);
			ChannelHandler* Remove(ChannelHandlerContext* ctx);
			ChannelHandlerContext* GetActualUpstreamContext(
					ChannelHandlerContext* ctx);
			ChannelHandlerContext* GetActualDownstreamContext(
					ChannelHandlerContext* ctx);

			ChannelHandlerContext* PriAddFirst(const string& name,
					ChannelHandler* handler);
			ChannelHandlerContext* PriAddLast(const string& name,
					ChannelHandler* handler);
			ChannelHandlerContext* PriAddBefore(const string& baseName,
					const string& name, ChannelHandler* handler);
			ChannelHandlerContext* PriAddAfter(const string& baseName,
					const string& name, ChannelHandler* handler);

			template<typename T>
			bool SendUpstream(ChannelHandlerContext* ctx, T& e);
			template<typename T>
			bool SendUpstream(ChannelHandlerContext* ctx, MessageEvent<T>& e);
			template<typename T>
			bool SendDownstream(ChannelHandlerContext* ctx, T& e);
			template<typename T>
			bool SendDownstream(ChannelHandlerContext* ctx, MessageEvent<T>& e);
			friend class ChannelHandlerContext;
		public:
			ChannelPipeline();
			inline Channel* GetChannel()
			{
				return m_channel;
			}
			inline bool IsAttached()
			{
				return NULL != m_channel;
			}
			inline void Attach(Channel* channel)
			{
				assert(NULL != channel);
				m_channel = channel;
			}

			void Clear();

			ChannelHandler* Remove(ChannelHandler* handler);
			ChannelHandler* Remove(const string& name);
			ChannelHandler* RemoveFirst();
			ChannelHandler* RemoveLast();
			ChannelHandler* Get(const string& name);
			void GetAll(vector<ChannelHandler*> handlerlist);
			ChannelHandlerContext* GetContext(const string& name);

			template<typename T>
			bool SendUpstream(T& event);
			template<typename T>
			bool SendDownstream(T& event);
			template<typename T>
			ChannelHandlerContext* AddFirst(const string& name,
					AbstractChannelHandler<T>* handler)
			{
				//ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddFirst(name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddLast(const string& name,
					AbstractChannelHandler<T>* handler)
			{
				//ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddLast(name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddBefore(const string& baseName,
					const string& name, AbstractChannelHandler<T>* handler)
			{
				//ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddBefore(baseName, name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddAfter(const string& baseName,
					const string& name, AbstractChannelHandler<T>* handler)
			{
				//ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddAfter(baseName, name, handler);
			}
			~ChannelPipeline();
	};

	typedef void ChannelPipelineInitializer(ChannelPipeline* pipeline,
			void* data);
	typedef void ChannelPipelineFinalizer(ChannelPipeline* pipeline,
			void* data);

}
#endif /* CHANNELPIPELINE_HPP_ */


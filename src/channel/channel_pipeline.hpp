/*
 * ChannelPipeline.hpp
 *
 *  Created on: 2011-1-7
 *      Author: qiyingwang
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
using std::map;
using std::vector;
using std::string;
namespace rddb
{
	/**
	 *  ChannelPipeline用于管理注册在channle上的handler， handler之间关系， event流向如图
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
	 *  pipline注册handler如下所示:
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
			map<string, ChannelHandlerContext*> m_name2ctx;
			ChannelHandlerContext* Init(const string& name,
					ChannelHandler* handler);
			void Clear();
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
				ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddFirst(name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddLast(const string& name,
					AbstractChannelHandler<T>* handler)
			{
				ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddLast(name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddBefore(const string& baseName,
					const string& name, AbstractChannelHandler<T>* handler)
			{
				ChannelHandlerHelper < T > ::RegisterHandler(handler);
				return PriAddBefore(baseName, name, handler);
			}
			template<typename T>
			ChannelHandlerContext* AddAfter(const string& baseName,
					const string& name, AbstractChannelHandler<T>* handler)
			{
				ChannelHandlerHelper < T > ::RegisterHandler(handler);
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


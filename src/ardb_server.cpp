/*
 * ardb_server.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */
#include "ardb_server.hpp"

namespace ardb
{

	int ARDBServer::Ping()
	{

	}
	int ARDBServer::Echo(const std::string& message)
	{

	}
	int ARDBServer::Select(DBID id)
	{

	}

	void ARDBServer::ProcessRedisCommand(Channel* conn, RedisCommandFrame& cmd)
	{

	}

	static void ardb_pipeline_init(ChannelPipeline* pipeline, void* data)
	{
		ChannelUpstreamHandler<RedisCommandFrame>* handler =
		        (ChannelUpstreamHandler<RedisCommandFrame>*) data;
		pipeline->AddLast("decoder", new RedisFrameDecoder);
		pipeline->AddLast("handler", handler);
	}

	static void ardb_pipeline_finallize(ChannelPipeline* pipeline, void* data)
	{
		ChannelHandler* handler = pipeline->Get("decoder");
		DELETE(handler);
	}

	int ARDBServer::Start()
	{
		struct RedisRequestHandler: public ChannelUpstreamHandler<
		        RedisCommandFrame>
		{
				ARDBServer* server;
				void MessageReceived(ChannelHandlerContext& ctx,
				        MessageEvent<RedisCommandFrame>& e)
				{
					server->ProcessRedisCommand(ctx.GetChannel(),
					        *(e.GetMessage()));
				}
				RedisRequestHandler(ARDBServer* s) :
						server(s)
				{
				}
		};
		RedisRequestHandler handler(this);
		ServerSocketChannel* tcp_server = m_server->NewServerSocketChannel();
		ChannelOptions ops;
		ops.tcp_nodelay = true;
		SocketHostAddress address("0.0.0.0", 6379);
		tcp_server->Bind(&address);
		tcp_server->Configure(ops);
		tcp_server->SetChannelPipelineInitializor(ardb_pipeline_init, &handler);
		tcp_server->SetChannelPipelineFinalizer(ardb_pipeline_finallize, NULL);

		m_server->Start();
		return 0;
	}
}


/*
 * db_worker.cpp
 *
 *  Created on: 2013-5-15
 *      Author: wqy
 */
#include "db_worker.hpp"

namespace ardb
{
	DBWorker::DBWorker(ArdbServer* server) :
			m_server(server), m_worker_input(NULL), m_worker_output(NULL), m_server_input(
					NULL), m_server_output(NULL)
	{
		int pfd1[2], pfd2[2];
		pipe(pfd1);
		pipe(pfd2);
		m_server_output = m_server->m_service->NewPipeChannel(-1, pfd1[1]);
		m_worker_input = m_serv.NewPipeChannel(pfd1[0], -1);
		m_worker_output = m_serv.NewPipeChannel(-1, pfd2[1]);
		m_server_input = m_server->m_service->NewPipeChannel(pfd2[0], -1);

		m_server_output->GetPipeline().AddLast("encoder", &m_inst_encoder);
		m_worker_output->GetPipeline().AddLast("encoder", &m_reply_encoder);

		ChannelUpstreamHandler<DBOpInstruction>* h1 = this;
		m_worker_input->GetPipeline().AddLast("decoder", &m_inst_decoder);
		m_worker_input->GetPipeline().AddLast("handler", h1);

		ChannelUpstreamHandler<DBOpReply>* h2 = this;
		m_server_input->GetPipeline().AddLast("decoder", &m_reply_decoder);
		m_server_input->GetPipeline().AddLast("handler", h2);
	}

	void DBWorker::Run()
	{
		m_serv.Start(true);
	}

	void DBWorker::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<DBOpInstruction>& e)
	{
		DBOpInstruction* instruction = e.GetMessage();
		m_server->DoRedisCommand(*(instruction->ctx), instruction->setting,
				*(instruction->cmd));
		DELETE(instruction->cmd);
		DBOpReply reply(instruction->ctx);
		m_worker_output->Write(reply);
	}

	void DBWorker::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<DBOpReply>& e)
	{
		ArdbConnContext* context = e.GetMessage()->ctx;
		context->ReleaseRef();
		m_server->HandleReply(context);
	}

	void DBWorker::Submit(ArdbConnContext& ctx,
			ArdbServer::RedisCommandHandlerSetting* setting,
			RedisCommandFrame& cmd)
	{
		if (ctx.closed)
		{
			return;
		}
		ctx.AddRef();
		DBOpInstruction instruction(&ctx, setting, new RedisCommandFrame(cmd));
		m_server_output->Write(instruction);
	}
}


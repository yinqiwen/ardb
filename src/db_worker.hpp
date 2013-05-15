/*
 * db_worker.hpp
 *
 *  Created on: 2013-5-15
 *      Author: wqy
 */

#ifndef DB_WORKER_HPP_
#define DB_WORKER_HPP_

#include "ardb_server.hpp"

namespace ardb
{
	struct DBOpInstruction
	{
			ArdbConnContext* ctx;
			ArdbServer::RedisCommandHandlerSetting* setting;
			RedisCommandFrame* cmd;
			DBOpInstruction(ArdbConnContext* c = NULL,
					ArdbServer::RedisCommandHandlerSetting* s = NULL,
					RedisCommandFrame* m = NULL) :
					ctx(c), setting(s), cmd(m)
			{
			}
			size_t Size()
			{
				return sizeof(ctx) + sizeof(setting) + sizeof(cmd);
			}
	};

	struct DBOpReply
	{
			ArdbConnContext* ctx;
			DBOpReply(ArdbConnContext* c = NULL) :
					ctx(c)
			{
			}
			size_t Size()
			{
				return sizeof(ctx);
			}
	};

	struct DBOpInstructionEncoder: public ChannelDownstreamHandler<
			DBOpInstruction>
	{
			bool WriteRequested(ChannelHandlerContext& ctx,
					MessageEvent<DBOpInstruction>& e)
			{
				static Buffer buffer(e.GetMessage()->Size());
				DBOpInstruction* inst = e.GetMessage();
				buffer.Clear();
				buffer.Write(&(inst->ctx), sizeof(inst->ctx));
				buffer.Write(&(inst->setting), sizeof(inst->setting));
				buffer.Write(&(inst->cmd), sizeof(inst->cmd));
				return ctx.GetChannel()->Write(buffer);
			}
	};

	struct DBOpInstructionDecoder: public ardb::codec::StackFrameDecoder<
			DBOpInstruction>
	{
			bool Decode(ChannelHandlerContext& ctx, Channel* channel,
					Buffer& buffer, DBOpInstruction& msg)
			{
				if (buffer.ReadableBytes() < msg.Size())
				{
					return false;
				}
				buffer.Read(&msg.ctx, sizeof(msg.ctx));
				buffer.Read(&msg.setting, sizeof(msg.setting));
				buffer.Read(&msg.cmd, sizeof(msg.cmd));
				return true;
			}
	};
	struct DBOpReplyEncoder: public ChannelDownstreamHandler<DBOpReply>
	{
			bool WriteRequested(ChannelHandlerContext& ctx,
					MessageEvent<DBOpReply>& e)
			{
				static Buffer buffer(e.GetMessage()->Size());
				DBOpReply* inst = e.GetMessage();
				buffer.Clear();
				buffer.Write(&(inst->ctx), sizeof(inst->ctx));
				return ctx.GetChannel()->Write(buffer);
			}
	};

	struct DBOpReplyDecoder: public ardb::codec::StackFrameDecoder<DBOpReply>
	{
			bool Decode(ChannelHandlerContext& ctx, Channel* channel,
					Buffer& buffer, DBOpReply& msg)
			{
				if (buffer.ReadableBytes() < msg.Size())
				{
					return false;
				}
				buffer.Read(&msg.ctx, sizeof(msg.ctx));
				return true;
			}
	};
	class DBWorker: public Thread,
			public ChannelUpstreamHandler<DBOpInstruction>,
			public ChannelUpstreamHandler<DBOpReply>
	{
		private:
			ArdbServer* m_server;
			PipeChannel* m_worker_input;
			PipeChannel* m_worker_output;
			PipeChannel* m_server_input;
			PipeChannel* m_server_output;
			ChannelService m_serv;
			DBOpInstructionEncoder m_inst_encoder;
			DBOpInstructionDecoder m_inst_decoder;
			DBOpReplyEncoder m_reply_encoder;
			DBOpReplyDecoder m_reply_decoder;

			void Run();
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<DBOpInstruction>& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<DBOpReply>& e);
		public:
			DBWorker(ArdbServer* server);
			void Submit(ArdbConnContext& ctx,
					ArdbServer::RedisCommandHandlerSetting* setting,
					RedisCommandFrame& cmd);
			void Stop()
			{
				m_serv.Stop();
			}
	};
}

#endif /* DB_WORKER_HPP_ */

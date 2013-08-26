/*
 * master.cpp
 *
 *  Created on: 2013-08-22
 *      Author: yinqiwen
 */
#include "master.hpp"
#include "ardb_server.hpp"

#define REPL_INSTRUCTION_ADD_SLAVE 0
#define REPL_INSTRUCTION_FEED_CMD 1

#define SOFT_SIGNAL_REPL_INSTRUCTION 1

namespace ardb
{
	/*
	 * Master
	 */
	Master::Master(ArdbServer* server) :
			m_server(server), m_notify_channel(NULL)
	{
		m_notify_channel = m_channel_service.NewSoftSignalChannel();
	}

	int Master::Init()
	{
		m_notify_channel->Register(SOFT_SIGNAL_REPL_INSTRUCTION, this);
		struct HeartbeatTask: public Runnable
		{
				Master* serv;
				HeartbeatTask(Master* s) :
						serv(s)
				{
				}
				void Run()
				{
					serv->OnHeartbeat();
				}
		};
		m_channel_service.GetTimer().ScheduleHeapTask(new HeartbeatTask(this),
				m_server->m_cfg.repl_ping_slave_period,
				m_server->m_cfg.repl_ping_slave_period, SECONDS);

		Start();
		return 0;
	}

	void Master::Run()
	{
		m_channel_service.Start();
	}

	void Master::OnHeartbeat()
	{
		SlaveConnTable::iterator it = m_slave_table.begin();
		for (; it != m_slave_table.end(); it++)
		{
			if (it->second->state == SLAVE_STATE_SYNCED)
			{
				Buffer ping;
				ping.Write("PING\r\n", 6);
				it->second->conn->Write(ping);
			}
		}
	}

	void Master::OnInstructions()
	{
		ReplInstruction instruction;
		uint32 count = 0;
		while (m_inst_queue.Pop(instruction))
		{
			switch (instruction.type)
			{
				case REPL_INSTRUCTION_ADD_SLAVE:
				{
					SlaveConnection* conn = (SlaveConnection*) instruction.ptr;
					m_channel_service.AttachChannel(conn->conn, true);
					conn->state = SLAVE_STATE_CONNECTED;
					m_slave_table[conn->conn->GetID()] = conn;
					SyncSlave(*conn);
					break;
				}
				case REPL_INSTRUCTION_FEED_CMD:
				{
					RedisCommandFrame* cmd =
							(RedisCommandFrame*) instruction.ptr;
					DELETE(cmd);
					break;
				}
				default:
				{
					break;
				}
			}
		}
	}

	void Master::SyncSlave(SlaveConnection& slave)
	{
		if(slave.server_key.empty())
		{
			//redis 2.6/2.4
		}else
		{
			Buffer msg;
			if(m_backlog.IsValidOffset(slave.server_key, slave.sync_offset))
			{
				msg.Printf("+CONTINUE\r\n");
			}else
			{
				//FULLRESYNC
				//msg.Printf("+FULLRESYNC %s %lld\r\n");
			}
			slave.conn->Write(msg);
		}

	}

	void Master::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
	{
		ASSERT(soft_signo == SOFT_SIGNAL_REPL_INSTRUCTION);
		OnInstructions();
	}

	void Master::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
	{
		uint32 conn_id = ctx.GetChannel()->GetID();
		SlaveConnTable::iterator found = m_slave_table.find(conn_id);
		if (found != m_slave_table.end())
		{
			DELETE(found->second);
			m_slave_table.erase(found);
		}
	}
	void Master::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisCommandFrame>& e)
	{
	}

	void Master::OfferReplInstruction(ReplInstruction& inst)
	{
		m_inst_queue.Push(inst);
		if (NULL != m_notify_channel)
		{
			m_notify_channel->FireSoftSignal(SOFT_SIGNAL_REPL_INSTRUCTION, 0);
		} else
		{
			DEBUG_LOG("NULL singnal channel:%p %p", m_notify_channel, this);
		}
	}

	static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
	{
		ChannelUpstreamHandler<RedisCommandFrame>* handler =
				(ChannelUpstreamHandler<RedisCommandFrame>*) data;
		pipeline->AddLast("decoder", new RedisCommandDecoder);
		pipeline->AddLast("encoder", new RedisReplyEncoder);
		pipeline->AddLast("handler", handler);
	}

	static void slave_pipeline_finallize(ChannelPipeline* pipeline, void* data)
	{
		ChannelHandler* handler = pipeline->Get("decoder");
		DELETE(handler);
		handler = pipeline->Get("encoder");
		DELETE(handler);
	}

	void Master::AddSlave(Channel* slave, RedisCommandFrame& cmd)
	{
		slave->Flush();
		SlaveConnection* conn = NULL;
		NEW(conn, SlaveConnection);
		conn->conn = slave;
		if (!strcasecmp(cmd.GetCommand().c_str(), "sync"))
		{
			//Redis 2.6/2.4 send 'sync'
			conn->isRedisSlave = true;
			conn->sync_offset = (uint64) -1;
		} else    //Ardb/Redis 2.8+ send psync
		{
			conn->server_key = cmd.GetArguments()[0];
			const std::string& offset_str = cmd.GetArguments()[1];
			if (!string_toint64(offset_str, conn->sync_offset))
			{
				ERROR_LOG("Invalid offset argument:%s", offset_str.c_str());
				slave->Close();
				DELETE(conn);
				return;
			}
		}
		m_server->m_service->DetachChannel(slave, true);
		slave->ClearPipeline();
		ChannelUpstreamHandler<RedisCommandFrame>* handler = this;
		slave->SetChannelPipelineInitializor(slave_pipeline_init, this);
		slave->SetChannelPipelineFinalizer(slave_pipeline_finallize, NULL);
		ReplInstruction inst(conn, REPL_INSTRUCTION_ADD_SLAVE);
		OfferReplInstruction(inst);
	}

	void Master::FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd)
	{
		RedisCommandFrame* newcmd = new RedisCommandFrame;
		*newcmd = cmd;
		ReplInstruction inst(newcmd, REPL_INSTRUCTION_FEED_CMD);
		OfferReplInstruction(inst);
	}
}


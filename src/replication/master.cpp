/*
 * master.cpp
 *
 *  Created on: 2013-08-22
 *      Author: yinqiwen
 */
#include "master.hpp"
#include "rdb.hpp"
#include "ardb_server.hpp"
#include <fcntl.h>
#include <sys/stat.h>

#define REPL_INSTRUCTION_ADD_SLAVE 0
#define REPL_INSTRUCTION_FEED_CMD 1

#define SOFT_SIGNAL_REPL_INSTRUCTION 1

#define MAX_SEND_CACHE_SIZE 4096

namespace ardb
{
	/*
	 * Master
	 */
	Master::Master(ArdbServer* server) :
			m_server(server), m_notify_channel(NULL), m_dumping_db(false), m_dumpdb_offset(
					-1)
	{

	}

	int Master::Init()
	{
		std::string repl_path = m_server->m_cfg.home + "/repl";
		if (0 != m_backlog.Init(repl_path, 1024 * 1024))
		{
			return -1;
		}
		m_notify_channel = m_channel_service.NewSoftSignalChannel();
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
			} else if (it->second->state == SLAVE_STATE_WAITING_DUMP_DATA)
			{
				Buffer cr;
				cr.Write("\n", 1);
				it->second->conn->Write(cr);
			}
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

	void Master::OnInstructions()
	{
		ReplicationInstruction instruction;
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
					conn->conn->ClearPipeline();
					conn->conn->SetChannelPipelineInitializor(
							slave_pipeline_init, this);
					conn->conn->SetChannelPipelineFinalizer(
							slave_pipeline_finallize, NULL);

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
	static void DumpRDBRoutine(void* cb)
	{
		ChannelService* serv = (ChannelService*) cb;
		serv->Continue();
	}
	void Master::FullResyncRedisSlave(SlaveConnection& slave)
	{
		std::string dump_file_path = m_server->m_cfg.home + "/repl/dump.rdb";
		m_dumping_db = true;
		slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
		if (m_dumping_db)
		{
			return;
		}
		m_dumpdb_offset = m_backlog.GetReplEndOffset();
		RedisDumpFile rdb(m_server->m_db, dump_file_path);
		rdb.Dump(DumpRDBRoutine, &m_channel_service);
		m_dumping_db = false;
		INFO_LOG("[REPL]Saved rdb dump file:%s", dump_file_path.c_str());
		onDumpComplete();
	}

	void Master::onDumpComplete()
	{
		SlaveConnTable::iterator it = m_slave_table.begin();
		while (it != m_slave_table.end())
		{
			if (it->second->state == SLAVE_STATE_WAITING_DUMP_DATA)
			{
				SendDumpToSlave(*(it->second));
			}
			it++;
		}
	}

	static void OnDumpFileSendComplete(void* data)
	{
		std::pair<void*, void*>* p = (std::pair<void*, void*>*) data;
		Master* master = (Master*) p->first;
		SlaveConnection* slave = (SlaveConnection*) p->second;
		DELETE(p);
		close(slave->repldbfd);
		slave->repldbfd = -1;
		master->SendCacheToSlave(*slave);
	}

	static void OnDumpFileSendFailure(void* data)
	{
		std::pair<void*, void*>* p = (std::pair<void*, void*>*) data;
		//Master* master = (Master*) p->first;
		SlaveConnection* slave = (SlaveConnection*) p->second;
		DELETE(p);
		close(slave->repldbfd);
		slave->repldbfd = -1;
	}

	void Master::SendCacheToSlave(SlaveConnection& slave)
	{
		slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
		slave.conn->EnableWriting();
		INFO_LOG("[PSYNC] Slave request offset: %lld", slave.sync_offset);
	}

	void Master::SendDumpToSlave(SlaveConnection& slave)
	{
		slave.state = SLAVE_STATE_SYNING_DUMP_DATA;
		std::string dump_file_path = m_server->m_cfg.home + "/repl/dump.rdb";
		SendFileSetting setting;
		setting.fd = open(dump_file_path.c_str(), O_RDONLY);
		if (-1 == setting.fd)
		{
			int err = errno;
			ERROR_LOG("Failed to open file:%s for reason:%s",
					dump_file_path.c_str(), strerror(err));
			slave.conn->Close();
			return;
		}
		setting.file_offset = 0;
		struct stat st;
		fstat(setting.fd, &st);
		Buffer header;
		header.Printf("$%llu\r\n", st.st_size);
		slave.conn->Write(header);

		setting.file_rest_len = st.st_size;
		setting.on_complete = OnDumpFileSendComplete;
		setting.on_failure = OnDumpFileSendFailure;
		std::pair<void*, void*>* cb = new std::pair<void*, void*>;
		cb->first = this;
		cb->second = &slave;
		setting.data = cb;
		slave.repldbfd = setting.fd;
		slave.conn->SendFile(setting);
	}

	void Master::SyncSlave(SlaveConnection& slave)
	{
		if (slave.server_key.empty())
		{
			//redis 2.6/2.4
			slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
		} else
		{
			Buffer msg;
			if (m_backlog.IsValidOffset(slave.server_key, slave.sync_offset))
			{
				msg.Printf("+CONTINUE\r\n");
				slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
			} else
			{
				//FULLRESYNC
				msg.Printf("+FULLRESYNC %s %lld\r\n",
						m_backlog.GetServerKey().c_str(),
						m_backlog.GetReplEndOffset());
				slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
			}
			slave.conn->Write(msg);
		}
		if (slave.state == SLAVE_STATE_WAITING_DUMP_DATA)
		{
			FullResyncRedisSlave(slave);
		} else
		{
			SendCacheToSlave(slave);
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
	void Master::ChannelWritable(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		uint32 conn_id = ctx.GetChannel()->GetID();
		SlaveConnTable::iterator found = m_slave_table.find(conn_id);
		if (found != m_slave_table.end())
		{
			SlaveConnection* slave = found->second;
			if (slave->state == SLAVE_STATE_SYNING_CACHE_DATA)
			{
				if ((uint64) slave->sync_offset == m_backlog.GetReplEndOffset())
				{
					slave->state = SLAVE_STATE_SYNCED;
					slave->conn->DisableWriting();
				} else
				{
					if (!m_backlog.IsValidOffset(slave->sync_offset))
					{
						slave->conn->Close();
					} else
					{
						size_t len = m_backlog.WriteChannel(slave->conn,
								slave->sync_offset, MAX_SEND_CACHE_SIZE);
						slave->sync_offset += len;
					}
				}
			}
		}
	}
	void Master::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisCommandFrame>& e)
	{
	}

	void Master::OfferReplInstruction(ReplicationInstruction& inst)
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
			conn->sync_offset = -1;
		} else
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
			//Redis 2.8+ send psync, Ardb send psync2
			if (!strcasecmp(cmd.GetCommand().c_str(), "psync"))
			{
				conn->isRedisSlave = true;
			} else
			{
				conn->isRedisSlave = false;
			}
		}
		m_server->m_service->DetachChannel(slave, true);
		ReplicationInstruction inst(conn, REPL_INSTRUCTION_ADD_SLAVE);
		OfferReplInstruction(inst);
	}

	void Master::FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd)
	{
		RedisCommandFrame* newcmd = new RedisCommandFrame;
		*newcmd = cmd;
		ReplicationInstruction inst(newcmd, REPL_INSTRUCTION_FEED_CMD);
		OfferReplInstruction(inst);
	}
}


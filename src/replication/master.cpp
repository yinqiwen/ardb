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
			        -1),m_current_dbid(ARDB_GLOBAL_DB)
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
		RedisCommandFrame ping("ping");
		WriteSlaves(0, ping, false);
		DEBUG_LOG("Ping slaves.");
		m_backlog.PersistSyncState();
	}

	void Master::WriteCmdToSlaves(RedisCommandFrame& cmd)
	{
		Buffer buffer(256);
		RedisCommandEncoder::Encode(buffer, cmd);
		m_backlog.Feed(buffer);

		SlaveConnTable::iterator it = m_slave_table.begin();
		for (; it != m_slave_table.end(); it++)
		{
			if (it->second->state == SLAVE_STATE_SYNCED)
			{
				buffer.SetReadIndex(0);
				it->second->conn->Write(buffer);
			}
		}
	}

	void Master::WriteSlaves(const DBID& dbid, RedisCommandFrame& cmd,
	        bool dbid_related)
	{
		if(dbid_related)
		{
			if(m_current_dbid != dbid)
			{
				m_current_dbid = dbid;
				RedisCommandFrame select("select %u", dbid);
				WriteCmdToSlaves(select);
			}
		}
		WriteCmdToSlaves(cmd);
	}

	static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
	{
		Master* master = (Master*) data;
		pipeline->AddLast("decoder", new RedisCommandDecoder);
		pipeline->AddLast("encoder", new RedisReplyEncoder);
		pipeline->AddLast("handler", master);
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
					RedisCommandWithDBID* cmd =
					        (RedisCommandWithDBID*) instruction.ptr;
                    WriteSlaves(cmd->first, cmd->second, true);
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
		slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
		if (m_dumping_db)
		{
			return;
		}
		INFO_LOG("[Master]Start sump data to file:%s", dump_file_path.c_str());
		m_dumping_db = true;
		m_dumpdb_offset = m_backlog.GetReplEndOffset();
		RedisDumpFile rdb(m_server->m_db, dump_file_path);
		rdb.Dump(DumpRDBRoutine, &m_channel_service);
		m_dumping_db = false;
		INFO_LOG("[REPL]Saved rdb dump file:%s", dump_file_path.c_str());
		OnDumpComplete();
	}

	void Master::OnDumpComplete()
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
		INFO_LOG("[REPL]Send dump file to slave");
		slave.state = SLAVE_STATE_SYNING_DUMP_DATA;
		std::string dump_file_path = m_server->m_cfg.home + "/repl/dump.rdb";
		SendFileSetting setting;
		setting.fd = open(dump_file_path.c_str(), O_RDONLY);
		if (-1 == setting.fd)
		{
			int err = errno;
			ERROR_LOG(
			        "Failed to open file:%s for reason:%s", dump_file_path.c_str(), strerror(err));
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
		}
		else
		{
			Buffer msg;
			if (m_backlog.IsValidOffset(slave.server_key, slave.sync_offset))
			{
				msg.Printf("+CONTINUE\r\n");
				slave.state = SLAVE_STATE_SYNING_CACHE_DATA;
			}
			else
			{
				//FULLRESYNC
				msg.Printf("+FULLRESYNC %s %lld\r\n",
				        m_backlog.GetServerKey().c_str(),
				        m_backlog.GetReplEndOffset() - 1);
				slave.state = SLAVE_STATE_WAITING_DUMP_DATA;
			}
			slave.conn->Write(msg);
		}
		if (slave.state == SLAVE_STATE_WAITING_DUMP_DATA)
		{
			FullResyncRedisSlave(slave);
		}
		else
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
				if ((uint64) slave->sync_offset >= m_backlog.GetReplEndOffset())
				{
					slave->state = SLAVE_STATE_SYNCED;
					slave->conn->DisableWriting();
				}
				else
				{
					if (!m_backlog.IsValidOffset(slave->sync_offset))
					{
						slave->conn->Close();
					}
					else
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
		}
		else
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
		}
		else
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
			}
			else
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
		RedisCommandWithDBID* newcmd= new RedisCommandWithDBID(dbid, cmd);
		ReplicationInstruction inst(newcmd, REPL_INSTRUCTION_FEED_CMD);
		OfferReplInstruction(inst);
	}
}


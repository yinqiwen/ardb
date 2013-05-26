/*
 * replication.cpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */
#include "replication.hpp"
#include "ardb_server.hpp"
#include <algorithm>

namespace ardb
{
	static const uint32 kSlaveStateConnecting = 1;
	static const uint32 kSlaveStateConnected = 2;
	static const uint32 kSlaveStateSyncing = 3;
	static const uint32 kSlaveStateSynced = 4;

	static const uint8 kInstrctionSlaveClientQueue = 1;
	static const uint8 kInstrctionRecordSetCmd = 2;
	static const uint8 kInstrctionRecordDelCmd = 3;
	static const uint8 kInstrctionRecordRedisCmd = 4;

	static const uint8 kSoftSinglaInstruction = 1;

	static const uint8 kRedisTestDB = 1;
	static const uint8 kArdbDB = 2;

	struct kWriteReplInstructionData
	{
			std::vector<void*> ptrs;
			bool from_master;
			kWriteReplInstructionData() :
					from_master(false)
			{
			}
	};

	SlaveConn::SlaveConn(Channel* c) :
			conn(c), synced_cmd_seq(0), state(kSlaveStateConnected), type(
					kRedisTestDB)
	{

	}
	SlaveConn::SlaveConn(Channel* c, const std::string& key, uint64 seq) :
			conn(c), server_key(key), synced_cmd_seq(seq), state(
					kSlaveStateConnected), type(kArdbDB)
	{
	}

	void SlaveConn::WriteRedisCommand(RedisCommandFrame& cmd)
	{
		static Buffer buf;
		buf.Clear();
		RedisCommandEncoder::Encode(buf, cmd);
		conn->Write(buf);
	}

	void SlaveClient::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisCommandFrame>& e)
	{
		DEBUG_LOG("Recv master cmd %s", e.GetMessage()->GetCommand().c_str());
		RedisCommandFrame* cmd = e.GetMessage();
		if (!strcasecmp(cmd->GetCommand().c_str(), "ping"))
		{
			m_ping_recved = true;
			return;
		} else if (!strcasecmp(cmd->GetCommand().c_str(), "arsynced"))
		{
			m_server_key = *(cmd->GetArgument(0));
			m_slave_state = kSlaveStateSynced;
			uint64 seq;
			string_touint64(*(cmd->GetArgument(1)), seq);
			m_sync_seq = seq;
			return;
		}

		if (m_slave_state == kSlaveStateSynced)
		{
			//extract sequence from last part
			const std::string& seq = cmd->GetArguments().back();
			string_touint64(seq, m_sync_seq);
			cmd->GetArguments().pop_back();
		}

		ArdbConnContext actx;
		actx.is_slave_conn = true;
		actx.conn = ctx.GetChannel();
		m_serv->ProcessRedisCommand(actx, *cmd);
	}

	void SlaveClient::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<Buffer>& e)
	{
		Buffer* msg = e.GetMessage();
		__process_buf: if (m_slave_state == kSlaveStateConnected)
		{
			int index = msg->IndexOf("\r\n", 2);
			if (index != -1)
			{
				char tmp;
				msg->ReadByte(tmp);
				if (tmp != '$' && tmp != '+')
				{
					ERROR_LOG("Expected char '$' or '+' : %d", tmp);
					return;
				}

				char buf[index];
				msg->Read(buf, index - 1);
				buf[index - 1] = 0;
				if (tmp == '+')
				{
					DEBUG_LOG("Recv status:%s", buf);
					msg->SkipBytes(2);
					goto __process_buf;
				}
				if (!str_touint32(buf, m_chunk_len))
				{
					ERROR_LOG("Invalid lenght %s.", buf);
					return;
				}
				if (m_chunk_len == 0)
				{
					//just check first response chunk length to distinguish servers(redis/ardb)
					m_server_type = kArdbDB;
				} else
				{
					m_server_type = kRedisTestDB;
				}
				msg->SkipBytes(2);
				//DEBUG_LOG("Sync bulk %d bytes", m_chunk_len);
				m_slave_state = kSlaveStateSyncing;
			} else
			{
				return;
			}
		}

		//discard redis synced  chunk
		uint32 bytes = msg->ReadableBytes();
		if (bytes < m_chunk_len)
		{
			m_chunk_len -= msg->ReadableBytes();
			msg->Clear();
		} else
		{
			msg->SkipBytes(m_chunk_len);
			m_chunk_len = 0;
			m_client->GetPipeline().Remove("handler");
			m_client->GetPipeline().AddLast("decoder", &m_decoder);
			m_client->GetPipeline().AddLast("encoder", &m_encoder);
			ChannelUpstreamHandler<RedisCommandFrame>* handler = this;
			m_client->GetPipeline().AddLast("handler", handler);

			//ardb server would send 'arsynced' after all data synced
			if (m_server_type == kRedisTestDB)
			{
				m_slave_state = kSlaveStateSynced;
			}
		}
	}

	void SlaveClient::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		m_client = NULL;
		//reconnect master after 500ms
		m_serv->GetTimer().Schedule(this, 500, -1);
	}

	void SlaveClient::Timeout()
	{
		WARN_LOG("Master connection timeout.");
		Close();
	}

	void SlaveClient::LoadSyncState()
	{
		std::string path = m_serv->m_cfg.repl_data_dir + "/repl.sync.state";
		Buffer content;
		if (file_read_full(path, content) == 0)
		{
			std::string str = content.AsString();
			std::vector<std::string> ss = split_string(str, " ");
			if (ss.size() >= 2)
			{
				m_server_key = ss[0];
				string_touint64(ss[1], m_sync_seq);
			}
			DEBUG_LOG(
					"Load repl state %s:%u", m_server_key.c_str(), m_sync_seq);
		}
	}

	void SlaveClient::PersistSyncState()
	{
		if (m_server_key == "-")
		{
			return;
		}
		char syncState[1024];
		sprintf(syncState, "%s %llu", m_server_key.c_str(), m_sync_seq);
		std::string path = m_serv->m_cfg.repl_data_dir + "/repl.sync.state";
		std::string content = syncState;
		file_write_content(path, content);
	}

	void SlaveClient::Run()
	{
		if (NULL == m_client && !m_master_addr.GetHost().empty())
		{
			ConnectMaster(m_master_addr.GetHost(), m_master_addr.GetPort());
		} else
		{
			if (m_slave_state == kSlaveStateSynced)
			{
				//do nothing
				if (!m_ping_recved)
				{
					Timeout();
				}
				m_ping_recved = false;
			}
		}
	}

	void SlaveClient::ChannelConnected(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		Buffer sync;
		sync.Printf("replconf listening-port %u\r\n",
				m_serv->GetServerConfig().listen_port);
		sync.Printf("arsync %s %lld\r\n", m_server_key.c_str(), m_sync_seq);
		ctx.GetChannel()->Write(sync);
		m_slave_state = kSlaveStateConnected;
		m_ping_recved = true;
	}

	int SlaveClient::ConnectMaster(const std::string& host, uint32 port)
	{
		if (!m_cron_inited)
		{
			m_cron_inited = true;
			m_serv->GetTimer().Schedule(this, m_serv->m_cfg.repl_timeout,
					m_serv->m_cfg.repl_timeout, SECONDS);
			LoadSyncState();
			struct PersistTask: public Runnable
			{
					SlaveClient* c;
					PersistTask(SlaveClient* cc) :
							c(cc)
					{
					}
					void Run()
					{
						c->PersistSyncState();
					}
			};
			m_serv->GetTimer().ScheduleHeapTask(new PersistTask(this),
					m_serv->m_cfg.repl_syncstate_persist_period,
					m_serv->m_cfg.repl_syncstate_persist_period, SECONDS);
		}
		SocketHostAddress addr(host, port);
		if (m_master_addr == addr && NULL != m_client)
		{
			return 0;
		}
		m_master_addr = addr;
		Close();
		m_client = m_serv->m_service->NewClientSocketChannel();
		ChannelUpstreamHandler<Buffer>* handler = this;
		m_client->GetPipeline().AddLast("handler", handler);
		m_client->Connect(&m_master_addr);
		m_slave_state = kSlaveStateConnecting;
		return 0;
	}

	void SlaveClient::Stop()
	{
		SocketHostAddress empty;
		m_master_addr = empty;
		m_slave_state = 0;
		Close();
	}

	void SlaveClient::Close()
	{
		if (NULL != m_client)
		{
			m_client->Close();
			m_client = NULL;
		}
	}

	ReplicationService::ReplicationService(ArdbServer* serv) :
			m_server(serv), m_is_saving(false), m_last_save(0), m_oplogs(serv), m_inst_signal(
					NULL), m_master_slave_id(0)
	{
	}

	int ReplicationService::Init()
	{
		m_oplogs.Load();
		return 0;
	}

	void ReplicationService::PingSlaves()
	{
		SlaveConnTable::iterator it = m_slaves.begin();
		while (it != m_slaves.end())
		{
			Buffer tmp;
			tmp.Write("PING\r\n", 6);
			it->second.conn->Write(tmp);
			it++;
		}
	}

	void ReplicationService::CheckSlaveQueue()
	{
		while (!m_waiting_slaves.empty())
		{
			SlaveConn& ch = m_waiting_slaves.front();
			m_serv.AttachChannel(ch.conn, true);
			if (ch.type == kRedisTestDB)
			{
				//empty fake rdb dump response
				Buffer content;
				content.Printf("$10\r\nREDIS0004");
				content.WriteByte((char) 255);
				ch.conn->Write(content);
				ch.state = kSlaveStateSynced;
				m_slaves[ch.conn->GetID()] = ch;
			} else
			{
				//empty first response chunk
				Buffer content;
				content.Printf("$0\r\n");
				ch.conn->Write(content);
				content.Clear();
				if (m_oplogs.VerifyClient(ch.server_key, ch.synced_cmd_seq))
				{
					content.Printf("arsynced %s %llu\r\n",
							m_oplogs.GetServerKey().c_str(), ch.synced_cmd_seq);
					ch.conn->Write(content);
					ch.state = kSlaveStateSynced;
					//increase sequence since the cmd already synced
					ch.synced_cmd_seq++;
				} else
				{
					ch.state = kSlaveStateSyncing;
					FullSync(ch);
				}
				m_slaves[ch.conn->GetID()] = ch;
			}
			m_waiting_slaves.pop_front();
		}
	}

	void ReplicationService::FeedSlaves()
	{
		Buffer tmp;
		SlaveConnTable::iterator it = m_slaves.begin();
		while (it != m_slaves.end())
		{
			SlaveConn& conn = it->second;
			if (conn.state == kSlaveStateSynced)
			{
				while (m_oplogs.LoadOpLog(conn.synced_cmd_seq, tmp,
						conn.conn->GetID() == m_master_slave_id) == 1)
				{
					if (tmp.Readable())
					{
						conn.conn->Write(tmp);
					}
					tmp.Clear();
				}
				if (tmp.Readable())
				{
					conn.conn->Write(tmp);
				}
			}
			it++;
		}
	}

	void ReplicationService::Routine()
	{
		CheckSlaveQueue();
	}

	//static uint32 kCount = 0;
	void ReplicationService::Run()
	{
		struct HeartbeatTask: public Runnable
		{
				ReplicationService* serv;
				HeartbeatTask(ReplicationService* s) :
						serv(s)
				{
				}
				void Run()
				{
					serv->PingSlaves();
				}
		};
		struct InstTask: public Runnable
		{
				ReplicationService* serv;
				InstTask(ReplicationService* s) :
						serv(s)
				{
				}
				void Run()
				{
					serv->ProcessInstructions();
				}
		};
		m_serv.GetTimer().ScheduleHeapTask(new HeartbeatTask(this),
				m_server->m_cfg.repl_ping_slave_period,
				m_server->m_cfg.repl_ping_slave_period, SECONDS);
		m_serv.GetTimer().ScheduleHeapTask(new InstTask(this), 1, 1, MILLIS);
		m_inst_signal = m_serv.NewSoftSignalChannel();
		m_inst_signal->Register(kSoftSinglaInstruction, this);
		m_serv.Start();
	}

	void ReplicationService::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
	{
		switch (soft_signo)
		{
			case kSoftSinglaInstruction:
			{
				ProcessInstructions();
				break;
			}
			default:
			{
				break;
			}
		}
	}
	void ReplicationService::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		m_slaves.erase(ctx.GetChannel()->GetID());
	}

	void ReplicationService::ProcessInstructions()
	{
		ReplInstruction instruction;
		while (m_inst_queue.Pop(instruction))
		{
			switch (instruction.type)
			{
				case kInstrctionSlaveClientQueue:
				{
					SlaveConn* conn = (SlaveConn*) (instruction.ptr);
					m_waiting_slaves.push_back(*conn);
					DELETE(conn);
					CheckSlaveQueue();
					break;
				}
				case kInstrctionRecordSetCmd:
				{
					kWriteReplInstructionData* tp =
							(kWriteReplInstructionData*) (instruction.ptr);
					std::string* k = (std::string*) tp->ptrs[0];
					std::string* v = (std::string*) tp->ptrs[1];
					CachedOp* op = m_oplogs.SaveSetOp(*k, v);
					if (NULL != op)
					{
						op->from_master = tp->from_master;
					}
					DELETE(k);
					//DELETE(v);
					DELETE(tp);
					FeedSlaves();
					break;
				}
				case kInstrctionRecordDelCmd:
				{
					kWriteReplInstructionData* tp =
							(kWriteReplInstructionData*) (instruction.ptr);
					std::string* k = (std::string*) tp->ptrs[0];
					CachedOp* op = m_oplogs.SaveDeleteOp(*k);
					if (NULL != op)
					{
						op->from_master = tp->from_master;
					}
					DELETE(k);
					DELETE(tp);
					FeedSlaves();
					break;
				}
				default:
				{
					ERROR_LOG("Unknown instruct type:%d", instruction.type);
					break;
				}
			}
		}
	}

	void ReplicationService::OfferInstruction(ReplInstruction& inst)
	{
		m_inst_queue.Push(inst);
		m_inst_signal->FireSoftSignal(kSoftSinglaInstruction, 0);

	}

	void ReplicationService::ServARSlaveClient(Channel* client,
			const std::string& serverKey, uint64 seq)
	{
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
		ChannelUpstreamHandler<Buffer>* handler = this;
		client->GetPipeline().AddLast("handler", handler);
		ReplInstruction instrct(kInstrctionSlaveClientQueue,
				new SlaveConn(client, serverKey, seq));
		OfferInstruction(instrct);
	}

	void ReplicationService::ServSlaveClient(Channel* client)
	{
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
		ChannelUpstreamHandler<Buffer>* handler = this;
		client->GetPipeline().AddLast("handler", handler);
		ReplInstruction instrct(kInstrctionSlaveClientQueue,
				new SlaveConn(client));
		OfferInstruction(instrct);
	}

	void ReplicationService::FullSync(SlaveConn& conn)
	{
		if (m_master_slave_id == conn.conn->GetID())
		{
			//WARNING: master-master replication SHOULD NOT do full sync.
			WARN_LOG(
					"The slave is also the master of current ardb instance, the full sync would not executed.");
			Buffer content;
			content.Printf("arsynced %s %llu\r\n",
					m_oplogs.GetServerKey().c_str(), conn.synced_cmd_seq);
			conn.conn->Write(content);
			conn.state = kSlaveStateSynced;
			conn.synced_cmd_seq = m_oplogs.GetMaxSeq();
			return;
		}
		INFO_LOG("Start a full sync to slave.");
		struct VisitorTask: public RawValueVisitor
		{
				SlaveConn& c;
				VisitorTask(SlaveConn& cc) :
						c(cc)
				{
				}
				int OnRawKeyValue(const Slice& key,
						const Slice& value)
				{
					ArgumentArray strs;
					strs.push_back("__set__");
					strs.push_back(std::string(key.data(), key.size()));
					strs.push_back(std::string(value.data(), value.size()));
					RedisCommandFrame cmd(strs);
					c.WriteRedisCommand(cmd);
					return 0;
				}
		} visitor(conn);
		uint64 start = get_current_epoch_millis();
		m_server->m_db->VisitAllDB(&visitor);
		conn.synced_cmd_seq = m_oplogs.GetMaxSeq();
		Buffer content;
		content.Printf("arsynced %s %llu\r\n", m_oplogs.GetServerKey().c_str(),
				conn.synced_cmd_seq);
		conn.conn->Write(content);
		conn.state = kSlaveStateSynced;
		uint64 end = get_current_epoch_millis();
		INFO_LOG("Cost %llums to sync all data to slave.", (end-start));
	}

	int ReplicationService::OnKeyUpdated(const Slice& key,
			const Slice& value)
	{
		std::string* nk = new std::string(key.data(), key.size());
		std::string* nv = new std::string(value.data(), value.size());
		kWriteReplInstructionData* data = new kWriteReplInstructionData;
		data->ptrs.push_back(nk);
		data->ptrs.push_back(nv);
		if (NULL != m_server->GetCurrentContext())
		{
			data->from_master = m_server->GetCurrentContext()->is_slave_conn;
		}

		ReplInstruction instrct(kInstrctionRecordSetCmd, data);
		OfferInstruction(instrct);
		return 0;
	}
	int ReplicationService::OnKeyDeleted(const Slice& key)
	{
		std::string* nk = new std::string(key.data(), key.size());
		kWriteReplInstructionData* data = new kWriteReplInstructionData;
		data->ptrs.push_back(nk);
		if (NULL != m_server->GetCurrentContext())
		{
			data->from_master = m_server->GetCurrentContext()->is_slave_conn;
		}
		ReplInstruction instrct(kInstrctionRecordDelCmd, data);
		OfferInstruction(instrct);
		return 0;
	}

	void ReplicationService::RecordFlushDB(const DBID& db)
	{
		//m_oplogs.SaveFlushOp(db);
	}

	int ReplicationService::Save()
	{
		if (m_is_saving)
		{
			return 1;
		}
		if (m_server->m_cfg.backup_dir.empty())
		{
			ERROR_LOG("Empty bakup dir for backup.");
			return -1;
		}
		//m_server->m_db->CloseAll();
		m_is_saving = true;
		int ret = 0;
		char cmd[m_server->m_cfg.data_base_path.size() + 256];
		make_dir(m_server->m_cfg.backup_dir);
		std::string dest = m_server->m_cfg.backup_dir
				+ "/ardb_all_data.save.tar";
		std::string shasumfile = m_server->m_cfg.backup_dir
				+ "/ardb_all_data.sha1sum";
		sprintf(cmd, "tar cf %s %s;", dest.c_str(),
				m_server->m_cfg.data_base_path.c_str());
		ret = system(cmd);
		if (-1 == ret)
		{
			ERROR_LOG( "Failed to create backup data archive:%s", dest.c_str());
		} else
		{
			std::string sha1sum_str;
			ret = sha1sum_file(dest, sha1sum_str);
			if (-1 == ret)
			{
				ERROR_LOG(
						"Failed to compute sha1sum for data archive:%s", dest.c_str());
			} else
			{
				INFO_LOG("Save file SHA1sum is %s", sha1sum_str.c_str());
				file_write_content(shasumfile, sha1sum_str);
				m_last_save = time(NULL);
			}
		}
		m_is_saving = false;
		return ret;
	}

	int ReplicationService::BGSave()
	{
		struct BGTask: public Thread
		{
				ReplicationService* serv;
				BGTask(ReplicationService* s) :
						serv(s)
				{
				}
				void Run()
				{
					serv->Save();
					delete this;
				}
		};
		BGTask* task = new BGTask(this);
		task->Start();
		return 0;
	}

	ReplicationService::~ReplicationService()
	{
	}
}

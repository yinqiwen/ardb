/*
 * replication.cpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */
#include "replication.hpp"
#include "ardb_server.hpp"
#include <algorithm>
#include <sstream>

namespace ardb
{
	static const uint32 kSlaveStateConnecting = 1;
	static const uint32 kSlaveStateConnected = 2;
	static const uint32 kSlaveStateWaitingReplConfRes = 3;
	static const uint32 kSlaveStateArsyncRes = 4;
	static const uint32 kSlaveStateSyncing = 5;
	static const uint32 kSlaveStateSynced = 6;

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
	SlaveConn::SlaveConn(Channel* c, const std::string& key, uint64 seq,
	        DBIDSet& dbs) :
			conn(c), server_key(key), synced_cmd_seq(seq), state(
			        kSlaveStateConnected), type(kArdbDB), syncdbs(dbs)
	{
	}

	void SlaveConn::WriteRedisCommand(RedisCommandFrame& cmd)
	{
		Buffer buf;
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
		}
		else if (!strcasecmp(cmd->GetCommand().c_str(), "arsynced"))
		{
			m_server_key = *(cmd->GetArgument(0));
			m_slave_state = kSlaveStateSynced;
			uint64 seq;
			string_touint64(*(cmd->GetArgument(1)), seq);
			m_sync_seq = seq;
			return;
		}

		if (m_slave_state == kSlaveStateSynced && m_server_type == kArdbDB)
		{
			//extract sequence from last part
			const std::string& seq = cmd->GetArguments().back();
			string_touint64(seq, m_sync_seq);
			cmd->GetArguments().pop_back();
		}
		if (NULL == m_actx)
		{
			NEW(m_actx, ArdbConnContext);
		}
		m_actx->is_slave_conn = true;
		m_actx->conn = ctx.GetChannel();
		m_serv->ProcessRedisCommand(*m_actx, *cmd);
	}

	void SlaveClient::MessageReceived(ChannelHandlerContext& ctx,
	        MessageEvent<Buffer>& e)
	{
		Buffer* msg = e.GetMessage();
		switch (m_slave_state)
		{
			case kSlaveStateWaitingReplConfRes:
			{
				if (msg->IndexOf("\r\n", 2) == -1)
				{
					return;
				}
				char tmp;
				msg->ReadByte(tmp);
				/*
				 * '+':success  '-':fail
				 */
				if (tmp != '+' && tmp != '-')
				{
					ERROR_LOG("Expected char '+'/'-' : %d", tmp);
					return;
				}
				/*
				 * ignore return response now.
				 */
				msg->Clear();
				Buffer sync;
				if (m_sync_dbs.empty())
				{
					sync.Printf("arsync %s %lld\r\n", m_server_key.c_str(),
					        m_sync_seq);
				}
				else
				{
					std::stringstream stream;
					DBIDSet::iterator it = m_sync_dbs.begin();
					while (it != m_sync_dbs.end())
					{
						stream << *it << " ";
						it++;
					}
					sync.Printf("arsync %s %lld %s\r\n", m_server_key.c_str(),
					        m_sync_seq, stream.str().c_str());
				}
				ctx.GetChannel()->Write(sync);
				m_slave_state = kSlaveStateArsyncRes;
				return;
			}
			case kSlaveStateArsyncRes:
			{
				char tmp;
				msg->ReadByte(tmp);
				if (tmp == '-')
				{
					if (msg->IndexOf("\r\n", 2) == -1)
					{
						return;
					}
					msg->Clear();
					m_server_type = kRedisTestDB;
					/*
					 * Downgrade to redis db
					 */
					Buffer sync;
					sync.Printf("sync\r\n");
					ctx.GetChannel()->Write(sync);
					m_slave_state = kSlaveStateArsyncRes;
					return;
				}
				else if (tmp != '$')
				{
					WARN_LOG(
					        "Expected length header : %d %d", tmp, msg->ReadableBytes());
					msg->Clear();
					return;
				}
				if (0 == m_server_type)
				{
					m_server_type = kArdbDB;
				}
				std::string chunklenstr;
				while (msg->Readable())
				{
					char tmp = 0;
					msg->ReadByte(tmp);
					if (tmp >= '0' && tmp <= '9')
					{
						chunklenstr.append(&tmp, 1);
					}
					else if (tmp == '\r' || tmp == '\n')
					{
						/*
						 * skip '\r\n'
						 */
						continue;
					}
					else
					{
						msg->AdvanceReadIndex(-1);
						break;
					}
				}
				DEBUG_LOG("chunklenstr = %s", chunklenstr.c_str());
				if (!string_touint32(chunklenstr, m_chunk_len))
				{
					ERROR_LOG("Invalid lenght %s.", chunklenstr.c_str());
					return;
				}
				//DEBUG_LOG("Sync bulk %d bytes", m_chunk_len);
				m_slave_state = kSlaveStateSyncing;
				break;
			}
			case kSlaveStateSyncing:
			{
				assert(m_server_type == kRedisTestDB);
				break;
			}
			default:
			{
				ERROR_LOG("Slave client is in invalid state:%d", m_slave_state);
				break;
			}
		}

		//discard redis synced  chunk
		uint32 bytes = msg->ReadableBytes();
		if (bytes < m_chunk_len)
		{
			m_chunk_len -= msg->ReadableBytes();
			msg->Clear();
		}
		else
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
		m_slave_state = 0;
		DELETE(m_actx);
		//reconnect master after 1000ms
		struct ReconnectTask: public Runnable
		{
				SlaveClient& sc;
				ReconnectTask(SlaveClient& ssc) :
						sc(ssc)
				{
				}
				void Run()
				{
					if (NULL == sc.m_client
					        && !sc.m_master_addr.GetHost().empty())
					{
						sc.ConnectMaster(sc.m_master_addr.GetHost(),
						        sc.m_master_addr.GetPort());
					}
				}
		};
		m_serv->GetTimer().ScheduleHeapTask(new ReconnectTask(*this), 1000, -1,
		        MILLIS);
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

	void SlaveClient::ChannelConnected(ChannelHandlerContext& ctx,
	        ChannelStateEvent& e)
	{
		Buffer replconf;
		replconf.Printf("replconf listening-port %u\r\n",
		        m_serv->GetServerConfig().listen_port);
		ctx.GetChannel()->Write(replconf);
		m_slave_state = kSlaveStateWaitingReplConfRes;
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
		m_slave_state = kSlaveStateConnecting;
		m_client->Connect(&m_master_addr);
		return 0;
	}

	void SlaveClient::Stop()
	{
		SocketHostAddress empty;
		m_master_addr = empty;
		m_slave_state = 0;
		m_sync_seq = 0;
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
			}
			else
			{
				//empty first response chunk
				Buffer content;
				content.Printf("$0\r\n");
				ch.conn->Write(content);

				if (m_oplogs.VerifyClient(ch.server_key, ch.synced_cmd_seq))
				{
					content.Clear();
					content.Printf("arsynced %s %llu\r\n",
					        m_oplogs.GetServerKey().c_str(), ch.synced_cmd_seq);
					ch.conn->Write(content);
					ch.state = kSlaveStateSynced;
					//increase sequence since the cmd already synced
					ch.synced_cmd_seq++;
				}
				else
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
				while (m_oplogs.LoadOpLog(conn.syncdbs, conn.synced_cmd_seq,
				        tmp, conn.conn->GetID() == m_master_slave_id) == 1)
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
					serv->m_oplogs.Routine();
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
		m_serv.GetTimer().ScheduleHeapTask(new InstTask(this), 100, 100,
		        MILLIS);
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
	        const std::string& serverKey, uint64 seq, DBIDSet& dbs)
	{
		client->Flush();
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
		ChannelUpstreamHandler<Buffer>* handler = this;
		client->GetPipeline().AddLast("handler", handler);
		ReplInstruction instrct(kInstrctionSlaveClientQueue,
		        new SlaveConn(client, serverKey, seq, dbs));
		OfferInstruction(instrct);
	}

	void ReplicationService::ServSlaveClient(Channel* client)
	{
		client->Flush();
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
		m_serv.GetTimer().Schedule(new FullSyncTask(this, conn), 1, -1);
	}

	int ReplicationService::OnKeyUpdated(const Slice& key, const Slice& value)
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
		}
		else
		{
			std::string sha1sum_str;
			ret = sha1sum_file(dest, sha1sum_str);
			if (-1 == ret)
			{
				ERROR_LOG(
				        "Failed to compute sha1sum for data archive:%s", dest.c_str());
			}
			else
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

	void ReplicationService::Stop()
	{
		m_serv.Stop();
		m_serv.Wakeup();
	}

	Ardb& ReplicationService::GetDB()
	{
		return *(m_server->m_db);
	}

	ArdbServerConfig& ReplicationService::GetConfig()
	{
		return m_server->m_cfg;
	}

	void ReplicationService::OnFullSynced(FullSyncTask* task)
	{
		DELETE(task);
		CheckSlaveQueue();
	}

	ReplicationService::~ReplicationService()
	{
	}

	/*
	 * Full Sync Task
	 */
	static const uint8 kFullSyncIter = 0;
	static const uint8 kFullSyncLogs = 1;
	static const uint8 kFullSyncMem = 2;
	static const uint32 kMaxSyncRecordsPeriod = 2000;

	void FullSyncTask::SyncDBIter()
	{
		if (NULL == m_iter)
		{
			m_start_time = get_current_epoch_millis();
			m_seq_after_sync_iter = m_repl->GetOpLogs().GetMaxSeq();
			if (m_conn.syncdbs.empty())
			{
				m_iter = m_repl->GetDB().NewIterator();
			}
			else
			{
				m_iter = m_repl->GetDB().NewIterator(*(m_conn.syncdbs.begin()));
				m_syncing_dbs = m_conn.syncdbs;
			}
		}
		struct VisitorTask: public RawValueVisitor
		{
				SlaveConn& c;
				uint32 count;
				VisitorTask(SlaveConn& cc) :
						c(cc), count(0)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					ArgumentArray strs;
					strs.push_back("__set__");
					strs.push_back(std::string(key.data(), key.size()));
					strs.push_back(std::string(value.data(), value.size()));
					RedisCommandFrame cmd(strs);
					c.WriteRedisCommand(cmd);
					count++;
					if (count == kMaxSyncRecordsPeriod)
					{
						return -1;
					}
					return 0;
				}
		} visitor(m_conn);

		if (m_conn.syncdbs.empty())
		{
			m_repl->GetDB().VisitAllDB(&visitor, m_iter);
			if (visitor.count < kMaxSyncRecordsPeriod)
			{
				m_state = kFullSyncLogs;
				m_conn.synced_cmd_seq = m_seq_after_sync_iter;
				DELETE(m_iter);
			}
		}
		else
		{
			DBID db = *(m_syncing_dbs.begin());
			m_repl->GetDB().VisitDB(db, &visitor, m_iter);
			if (visitor.count < kMaxSyncRecordsPeriod)
			{
				m_syncing_dbs.erase(db);
				DELETE(m_iter);
				if (m_syncing_dbs.empty())
				{
					m_state = kFullSyncLogs;
					m_conn.synced_cmd_seq = m_seq_after_sync_iter;
				}
				else
				{
					m_iter = m_repl->GetDB().NewIterator(
					        *(m_syncing_dbs.begin()));
				}
			}
		}
		//sync after 1ms in next schedule
		m_repl->GetChannelService().GetTimer().Schedule(this, 1, -1);
	}

	void FullSyncTask::SyncOpLogs()
	{
		if (m_seq_after_sync_iter >= m_repl->GetOpLogs().GetMinSeq())
		{
			m_state = kFullSyncMem;
		}
		else
		{
			//compute oplog file sequence
			uint64 start_seq;
			for (uint32 i = m_repl->GetConfig().repl_max_backup_logs - 1; i > 0;
			        i--)
			{
				if (m_repl->GetOpLogs().PeekOpLogStartSeq(i, start_seq))
				{
					if (start_seq > m_seq_after_sync_iter)
					{
						ERROR_LOG(
						        "Can NOT sync to slave, since current node is too busy on writing.");
						m_repl->OnFullSynced(this);
						return;
					}
					if(start_seq + m_repl->GetConfig().repl_backlog_size > m_seq_after_sync_iter)
					{
						//TODO start sync from oplogs
						break;
					}
				}
			}
		}
		//sync after 1ms in next schedule
		m_repl->GetChannelService().GetTimer().Schedule(this, 1, -1);
	}
	void FullSyncTask::Run()
	{
		switch (m_state)
		{
			case kFullSyncIter:
			{
				SyncDBIter();
				break;
			}
			case kFullSyncLogs:
			{
				SyncOpLogs();
				break;
			}
			case kFullSyncMem:
			{
				m_conn.state = kSlaveStateSynced;
				Buffer content;
				content.Printf("arsynced %s %llu\r\n",
				        m_repl->GetOpLogs().GetServerKey().c_str(),
				        m_conn.synced_cmd_seq);
				m_conn.conn->Write(content);
				uint64 end = get_current_epoch_millis();
				INFO_LOG(
				        "Cost %llums to sync all data to slave.", (end-m_start_time));
				m_repl->OnFullSynced(this);
				return;
			}
			default:
			{
				break;
			}
		}
	}

	FullSyncTask::~FullSyncTask()
	{
		DELETE(m_iter);
	}
}

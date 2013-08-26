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

	static const uint8 kFullSyncIter = 0;
	static const uint8 kFullSyncLogs = 1;
	static const uint8 kFullSyncMem = 2;
	static const uint32 kMaxSyncRecordsPeriod = 2000;

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

	bool SlaveConn::WriteRedisCommand(RedisCommandFrame& cmd)
	{
		Buffer buf;
		RedisCommandEncoder::Encode(buf, cmd);
		return conn->Write(buf);
	}



	void Slave::Close()
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
		m_inst_signal = m_serv.NewSoftSignalChannel();
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
				m_slaves[ch.conn->GetID()] = ch;
				SlaveConn& c = m_slaves[ch.conn->GetID()];
				if (m_oplogs.VerifyClient(c.server_key, c.synced_cmd_seq))
				{
					content.Clear();
					content.Printf("arsynced %s %llu\r\n",
							m_oplogs.GetServerKey().c_str(), c.synced_cmd_seq);
					c.conn->Write(content);
					c.state = kSlaveStateSynced;
					//increase sequence since the cmd already synced
					c.synced_cmd_seq++;
				} else
				{
					if (m_oplogs.GetServerKey() != c.server_key)
					{
						c.synced_cmd_seq = 0;
					}
					c.state = kSlaveStateSyncing;
					LoadSync(c);
				}
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
			//slave connection is synced & have data less than 1mb to write
			if (conn.state == kSlaveStateSynced
					&& conn.conn->WritableBytes() <= 1024 * 1024)
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
			} else
			{
				//do nothing
			}
			it++;
		}
	}

	void ReplicationService::Routine()
	{
		CheckSlaveQueue();
	}

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
		uint32 count = 0;
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
			count++;
			if (count >= kMaxSyncRecordsPeriod)
			{
				if (NULL != m_inst_signal)
				{
					m_inst_signal->FireSoftSignal(kSoftSinglaInstruction, 0);
				}
				return;
			}
		}
	}

	void ReplicationService::OfferInstruction(ReplInstruction& inst)
	{
		m_inst_queue.Push(inst);
		if (NULL != m_inst_signal)
		{
			m_inst_signal->FireSoftSignal(kSoftSinglaInstruction, 0);
		} else
		{
			DEBUG_LOG("NULL singnal channel:%p %p", m_inst_signal, this);
		}
	}

	void ReplicationService::ServARSlaveClient(Channel* client,
			const std::string& serverKey, uint64 seq, DBIDSet& dbs)
	{
		DEBUG_LOG("ServARSlaveClient for %s", serverKey.c_str());
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

	void ReplicationService::LoadSync(SlaveConn& conn)
	{
		INFO_LOG("Start a load sync task.");
		uint8 state = kFullSyncIter;
		//WARNING: master-master replication SHOULD NOT do full sync from storage engine.
		if (m_master_slave_id == conn.conn->GetID()
				|| m_oplogs.IsInDiskOpLogs(conn.synced_cmd_seq))
		{
			//Read oplogs from disk
			state = kFullSyncLogs;
		}
		m_serv.GetTimer().Schedule(
				new LoadSyncTask(this, conn.conn->GetID(), state), 1, -1);
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

	void ReplicationService::OnLoadSynced(LoadSyncTask* task, bool success)
	{
		DELETE(task);
		if (success)
		{
			FeedSlaves();
		} else
		{
			ERROR_LOG("Load sync failed.");
		}
	}

	SlaveConn* ReplicationService::GetSlaveConn(uint32 id)
	{
		SlaveConnTable::iterator found = m_slaves.find(id);
		if (found != m_slaves.end())
		{
			return &(found->second);
		}
		return NULL;
	}

	ReplicationService::~ReplicationService()
	{
	}

	/*
	 * Load Sync Task
	 */

	LoadSyncTask::LoadSyncTask(ReplicationService* repl, uint32 id, uint8 state) :
			m_repl(repl), m_conn_id(id), m_iter(NULL), m_op_log(NULL), m_start_time(
					0), m_seq_after_sync_iter(0), m_state(state)
	{
		SlaveConn* conn = m_repl->GetSlaveConn(m_conn_id);
		if (NULL != conn)
		{
			m_syncing_dbs = conn->syncdbs;
		}
		m_start_time = get_current_epoch_millis();
	}
	void LoadSyncTask::SyncDBIter()
	{
		SlaveConn* conn = m_repl->GetSlaveConn(m_conn_id);
		if (NULL == m_iter)
		{
			m_seq_after_sync_iter = m_repl->GetOpLogs().GetMaxSeq();
			if (conn->syncdbs.empty())
			{
				m_iter = m_repl->GetDB().NewIterator();
			} else
			{
				m_iter = m_repl->GetDB().NewIterator(*(m_syncing_dbs.begin()));
			}
		}
		struct VisitorTask: public RawValueVisitor
		{
				SlaveConn& c;
				uint32 count;
				bool fail;
				VisitorTask(SlaveConn& cc) :
						c(cc), count(0), fail(false)
				{
				}
				int OnRawKeyValue(const Slice& key, const Slice& value)
				{
					ArgumentArray strs;
					strs.push_back("__set__");
					strs.push_back(std::string(key.data(), key.size()));
					strs.push_back(std::string(value.data(), value.size()));
					RedisCommandFrame cmd(strs);
					if (!c.WriteRedisCommand(cmd))
					{
						fail = true;
						return -1;
					}
					count++;
					if (count == kMaxSyncRecordsPeriod)
					{
						return -1;
					}
					return 0;
				}
		} visitor(*conn);

		if (conn->syncdbs.empty())
		{
			m_repl->GetDB().VisitAllDB(&visitor, m_iter);
			if (visitor.fail)
			{
				m_repl->OnLoadSynced(this, false);
				return;
			}
			if (visitor.count < kMaxSyncRecordsPeriod)
			{
				m_state = kFullSyncLogs;
				conn->synced_cmd_seq = m_seq_after_sync_iter;
				DELETE(m_iter);
			}
		} else
		{
			m_syncing_dbs.insert(ARDB_GLOBAL_DB);
			DBID db = *(m_syncing_dbs.begin());
			m_repl->GetDB().VisitDB(db, &visitor, m_iter);
			if (visitor.fail)
			{
				m_repl->OnLoadSynced(this, false);
				return;
			}
			if (visitor.count < kMaxSyncRecordsPeriod)
			{
				m_syncing_dbs.erase(db);
				DELETE(m_iter);
				if (m_syncing_dbs.empty())
				{
					m_state = kFullSyncLogs;
					conn->synced_cmd_seq = m_seq_after_sync_iter;
				} else
				{
					m_iter = m_repl->GetDB().NewIterator(
							*(m_syncing_dbs.begin()));
				}
			}
		}
		//sync after 1ms in next schedule
		m_repl->GetChannelService().GetTimer().Schedule(this, 1, -1);
	}

	void LoadSyncTask::SyncOpLogs()
	{
		SlaveConn* conn = m_repl->GetSlaveConn(m_conn_id);
		if ((conn->synced_cmd_seq + 1) >= m_repl->GetOpLogs().GetMinSeq())
		{
			m_state = kFullSyncMem;
		} else
		{
			//compute oplog file sequence
			if (NULL == m_op_log)
			{
				uint64 start_seq;
				for (uint32 i = m_repl->GetConfig().repl_max_backup_logs; i > 0;
						i--)
				{
					if (m_repl->GetOpLogs().PeekOpLogStartSeq(i, start_seq))
					{
						if (start_seq > (conn->synced_cmd_seq + 1))
						{
							ERROR_LOG(
									"Can NOT sync to slave, since current node is too busy on writing.");
							m_repl->OnLoadSynced(this, false);
							return;
						}
						if (start_seq + m_repl->GetConfig().repl_backlog_size
								> conn->synced_cmd_seq + 1)
						{
							m_op_log = new OpLogFile(
									m_repl->GetOpLogs().GetOpLogPath(i));
							m_op_log->OpenRead();
							INFO_LOG(
									"Start sync from oplog:%s", m_repl->GetOpLogs().GetOpLogPath(i).c_str());
							break;
						}
					}
				}
			}
			if (NULL != m_op_log)
			{
				struct OpLogVisitor: public CachedOpVisitor
				{
						ReplicationService* repl;
						SlaveConn& conn;
						uint32 count;
						bool fail;
						OpLogVisitor(ReplicationService* serv, SlaveConn& c) :
								repl(serv), conn(c), count(0), fail(false)
						{
						}
						int OnCachedOp(CachedOp* op, uint64 seq)
						{
							count++;
							if (seq >= conn.synced_cmd_seq + 1)
							{
								conn.synced_cmd_seq = seq;
								if (!conn.syncdbs.empty())
								{
									DBID db;
									if (repl->GetOpLogs().PeekDBID(op, db)
											&& conn.syncdbs.count(db) == 0)
									{
										//ignore
										DELETE(op);
										return 0;
									}
								}
								RedisCommandFrame cmd;
								if (repl->GetOpLogs().CachedOp2RedisCommand(op,
										cmd, 0))
								{
									conn.synced_cmd_seq = seq;
									if (!conn.WriteRedisCommand(cmd))
									{
										fail = true;
									}
								}
								DELETE(op);
							}
							return 0;
						}
				} visitor(m_repl, *conn);
				int ret = m_op_log->Load(&visitor, 4 * 1024);
				if (visitor.fail)
				{
					m_repl->OnLoadSynced(this, false);
					return;
				}
				if (visitor.count == 0 || ret < 0)
				{
					DELETE(m_op_log);
				}
			} else
			{
				ERROR_LOG(
						"No oplog file found for syncing while slave synced:%"PRIu64"  and min seq in mem:%"PRIu64, conn->synced_cmd_seq, m_repl->GetOpLogs().GetMinSeq());
				m_repl->OnLoadSynced(this, false);
				return;
			}
		}
		//sync after 1ms in next schedule
		m_repl->GetChannelService().GetTimer().Schedule(this, 1, -1);
	}
	void LoadSyncTask::Run()
	{
		SlaveConn* conn = m_repl->GetSlaveConn(m_conn_id);
		if (NULL == conn)
		{
			WARN_LOG("Slave connection closed.");
			m_repl->OnLoadSynced(this, false);
			return;
		}
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
				conn->state = kSlaveStateSynced;
				Buffer content;
				content.Printf("arsynced %s %llu\r\n",
						m_repl->GetOpLogs().GetServerKey().c_str(),
						conn->synced_cmd_seq);
				conn->conn->Write(content);
				uint64 end = get_current_epoch_millis();
				INFO_LOG(
						"Cost %llums to sync all data to slave.", (end-m_start_time));
				m_repl->OnLoadSynced(this, true);
				return;
			}
			default:
			{
				break;
			}
		}
	}

	LoadSyncTask::~LoadSyncTask()
	{
		DELETE(m_iter);
		DELETE(m_op_log);
	}
}

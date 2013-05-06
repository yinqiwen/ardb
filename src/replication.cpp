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

	static const uint8 kRedisTestDB = 1;
	static const uint8 kArdbDB = 2;

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
		actx.conn = ctx.GetChannel();
		m_serv->ProcessRedisCommand(actx, *cmd);
	}

	void SlaveClient::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<Buffer>& e)
	{
		Buffer* msg = e.GetMessage();
		if (m_slave_state == kSlaveStateConnected)
		{
			int index = msg->IndexOf("\r\n", 2);
			if (index != -1)
			{
				char tmp;
				msg->ReadByte(tmp);
				if (tmp != '$')
				{
					ERROR_LOG("Expected char '$'.");
					return;
				}
				char buf[index];
				msg->Read(buf, index - 1);
				buf[index - 1] = 0;
				if (!str_touint32(buf, m_chunk_len))
				{
					ERROR_LOG("Invalid lenght %s.", buf);
					return;
				}
				if (m_chunk_len == 0)
				{
					//just check first response chunk length to distinguish server(redis/ardb)
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
			m_server(serv), m_is_saving(false), m_last_save(0), m_soft_signal(
					NULL), m_oplogs(*this, serv)
	{
	}

	int ReplicationService::Init()
	{
		return 0;
	}

	void ReplicationService::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
	{
		Routine();
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
		LockGuard<ThreadMutexLock> guard(m_slaves_lock);
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

	void ReplicationService::Routine()
	{
		CheckSlaveQueue();
		Buffer tmp;
		SlaveConnTable::iterator it = m_slaves.begin();
		while (it != m_slaves.end())
		{
			SlaveConn& conn = it->second;
			if (conn.state == kSlaveStateSynced)
			{
				while (m_oplogs.LoadOpLog(conn.synced_cmd_seq, tmp) == 1)
				{
					if (tmp.Readable())
					{
						//DEBUG_LOG("Synced %s", tmp.AsString().c_str());
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
		m_soft_signal = m_serv.NewSoftSignalChannel();
		m_soft_signal->Register(1, this);
		m_serv.GetTimer().ScheduleHeapTask(new HeartbeatTask(this),
				m_server->m_cfg.repl_ping_slave_period,
				m_server->m_cfg.repl_ping_slave_period, SECONDS);
		m_oplogs.Start();
		m_serv.Start();
	}

	void ReplicationService::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		m_slaves.erase(ctx.GetChannel()->GetID());
	}

	void ReplicationService::ServARSlaveClient(Channel* client,
			const std::string& serverKey, uint64 seq)
	{
		SlaveConn slave(client, serverKey, seq);
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
		client->GetPipeline().AddLast("handler", this);
		{
			LockGuard<ThreadMutexLock> guard(m_slaves_lock);
			m_waiting_slaves.push_back(slave);
		}
		if (NULL != m_soft_signal)
		{
			m_soft_signal->FireSoftSignal(1, 1);
		}
	}

	void ReplicationService::ServSlaveClient(Channel* client)
	{
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
		SlaveConn slave(client);
		{
			LockGuard<ThreadMutexLock> guard(m_slaves_lock);
			m_waiting_slaves.push_back(slave);
		}
		FireSyncEvent();
	}

	void ReplicationService::FullSync(SlaveConn& conn)
	{
		INFO_LOG("Start full sync to client.");
		struct VisitorTask: public RawValueVisitor
		{
				SlaveConn& c;
				VisitorTask(SlaveConn& cc) :
						c(cc)
				{
				}
				int OnRawKeyValue(const DBID& db, const Slice& key,
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
		m_server->m_db->VisitAllDB(&visitor);
		conn.synced_cmd_seq = m_oplogs.GetMaxSeq();
		Buffer content;
		content.Printf("arsynced %s %llu\r\n", m_oplogs.GetServerKey().c_str(),
				conn.synced_cmd_seq);
		conn.conn->Write(content);
		conn.state = kSlaveStateSynced;
	}

	int ReplicationService::OnKeyUpdated(const DBID& db, const Slice& key,
			const Slice& value)
	{
		RecordChangedKeyValue(db, key, value);
		return 0;
	}
	int ReplicationService::OnKeyDeleted(const DBID& db, const Slice& key)
	{
		RecordDeletedKey(db, key);
		return 0;
	}

	void ReplicationService::RecordChangedKeyValue(const DBID& db,
			const Slice& key, const Slice& value)
	{
		m_oplogs.SaveSetOp(db, key, value);
	}
	void ReplicationService::RecordDeletedKey(const DBID& db, const Slice& key)
	{
		m_oplogs.SaveDeleteOp(db, key);
	}
	void ReplicationService::RecordFlushDB(const DBID& db)
	{
		m_oplogs.SaveFlushOp(db);
	}

	void ReplicationService::FireSyncEvent()
	{
		if (NULL != m_soft_signal)
		{
			//DEBUG_LOG("Fire sync event.");
			m_soft_signal->FireSoftSignal(1, 1);
		}
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
		m_server->m_db->CloseAll();
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
}

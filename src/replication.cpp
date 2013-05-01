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
	static uint64 kBinLogIdSeed = 0;
	static const uint32 kSlaveStateConnecting = 1;
	static const uint32 kSlaveStateConnected = 2;
	static const uint32 kSlaveStateSyncing = 3;
	static const uint32 kSlaveStateSynced = 4;

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
				msg->SkipBytes(2);
				DEBUG_LOG("Sync bulk %d bytes", m_chunk_len);
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
			m_slave_state = kSlaveStateSynced;
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
		sync.Printf("sync\r\n");
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

	void SyncCommandQueue::Offer(RedisCommandFrame& cmd, uint32 seq)
	{
		m_mem_queue.push_back(new RedisCommandFrameFeed(cmd, seq));
	}

	ReplicationService::ReplicationService(ArdbServer* serv) :
			m_server(serv), m_is_saving(false), m_last_save(0), m_soft_signal(
					NULL)
	{

	}

	void ReplicationService::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
	{
		Routine();
	}

	void ReplicationService::PingSlaves()
	{
		INFO_LOG("Send ping to slaves");
		SlaveConnTable::iterator it = m_slaves.begin();
		while (it != m_slaves.end())
		{
			Buffer tmp;
			tmp.Write("PING\r\n", 6);
			it->second.conn->Write(tmp);
			it++;
		}
	}

	void ReplicationService::Routine()
	{
		LockGuard<ThreadMutexLock> guard(m_slaves_lock);
		while (!m_waiting_slaves.empty())
		{
			Channel* ch = m_waiting_slaves.front();
			m_serv.AttachChannel(ch, true);
			//empty fake rdb dump
			Buffer content;
			content.Printf("$10\r\nREDIS0004");
			content.WriteByte((char) 255);
			ch->Write(content);
			SlaveConn c;
			c.conn = ch;
			c.state = kSlaveStateSynced;
			m_slaves[ch->GetID()] = c;
			m_waiting_slaves.pop_front();
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
		m_serv.Start();
	}

//	static void slave_pipeline_init(ChannelPipeline* pipeline, void* data)
//	{
//		ArdbServer* serv = (ArdbServer*) data;
//		pipeline->AddLast("encoder", new RedisCommandEncoder);
//	}
//
//	static void slave_pipeline_finallize(ChannelPipeline* pipeline, void* data)
//	{
//		ChannelHandler* handler = pipeline->Get("encoder");
//		DELETE(handler);
//	}

	void ReplicationService::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		m_slaves.erase(ctx.GetChannel()->GetID());
	}

	void ReplicationService::ServSlaveClient(Channel* client)
	{
		m_server->m_service->DetachChannel(client, true);
		client->ClearPipeline();
//		client->SetChannelPipelineInitializor(slave_pipeline_init);
//		client->SetChannelPipelineFinalizer(slave_pipeline_finallize);
		{
			LockGuard<ThreadMutexLock> guard(m_slaves_lock);
			m_waiting_slaves.push_back(client);
		}
		if (NULL != m_soft_signal)
		{
			m_soft_signal->FireSoftSignal(1, 1);
		}

	}

	void ReplicationService::FullSync(Channel* client)
	{

	}

	void ReplicationService::RecordChangedKeyValue(const DBID& db,
			const Slice& key, const Slice& value)
	{
		ArgumentArray strs;
		strs.push_back("__set__");
		strs.push_back(std::string(key.data(), key.size()));
		strs.push_back(std::string(value.data(), key.size()));
		RedisCommandFrame cmd(strs);
		RecordOp(db, cmd);
	}
	void ReplicationService::RecordDeletedKey(const DBID& db, const Slice& key)
	{
		ArgumentArray strs;
		strs.push_back("__del__");
		strs.push_back(std::string(key.data(), key.size()));
		RedisCommandFrame cmd(strs);
		RecordOp(db, cmd);
	}
	void ReplicationService::RecordFlushDB(const DBID& db)
	{
		ArgumentArray strs;
		strs.push_back("flushdb");
		RedisCommandFrame cmd(strs);
		RecordOp(db, cmd);
	}

	void ReplicationService::RecordOp(const DBID& dbid,
			RedisCommandFrame& syncCmd)
	{
		LockGuard<ThreadMutexLock> guard(m_slaves_lock);
		if (m_current_db != dbid)
		{
			//generate 'select' cmd
			m_current_db = dbid;
			ArgumentArray strs;
			strs.push_back("select");
			strs.push_back(m_current_db);
			m_oplogs.Save(new RedisCommandFrame(strs));
		}
		m_oplogs.Save(new RedisCommandFrame(syncCmd));
		if (NULL != m_soft_signal)
		{
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

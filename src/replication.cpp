/*
 * replication.cpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */
#include "replication.hpp"
#include "ardb_server.hpp"
#include "util/thread.hpp"

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
		ArdbConnContext actx;
		actx.conn = ctx.GetChannel();
		m_serv->ProcessRedisCommand(actx, *(e.GetMessage()));
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

		//discard synced  chunk
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
	}

	int SlaveClient::ConnectMaster(const std::string& host, uint32 port)
	{
		if (!m_cron_inited)
		{
			m_cron_inited = true;
			m_serv->GetTimer().Schedule(this,
					m_serv->m_cfg.repl_ping_slave_period,
					m_serv->m_cfg.repl_ping_slave_period, SECONDS);
		}
		SocketHostAddress addr(host, port);
		if (m_master_addr == addr && NULL != m_client)
		{
			return 0;
		}
		Close();
		m_master_addr = addr;
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
			m_server(serv), m_is_saving(false), m_last_save(0)
	{

	}

	void ReplicationService::Run()
	{
		m_serv.Start();
	}
//
//	int ReplicationService::Start()
//	{
//		Start();
//		return 0;
//	}

	static bool veify_checkpoint_file(const std::string& data_file,
			const std::string& cksum_file)
	{
		if (!is_file_exist(cksum_file) || !is_file_exist(data_file))
		{
			remove(cksum_file.c_str());
			remove(data_file.c_str());
			return false;
		}
		Buffer sha1sum;
		int ret = file_read_full(cksum_file, sha1sum);
		if (ret < 0)
		{
			ERROR_LOG("Failed to read chepoint sha1sum file.");
			return false;
		}
		std::string sha1sum_str;
		ret = sha1sum_file(data_file, sha1sum_str);
		if (ret < 0)
		{
			ERROR_LOG("Failed to compute chepoint sha1sum.");
			return false;
		}
		if (sha1sum_str != sha1sum.AsString())
		{
			ERROR_LOG(
					"Invalid check sum %s VS %s.", sha1sum_str.c_str(), sha1sum.AsString().c_str());
			return false;
		}
		return true;
	}

//	int ReplicationService::Backup(const std::string& dstfile)
//	{
//		if (m_is_saving)
//		{
//			return 1;
//		}
//		struct BackupTask: public Thread
//		{
//				std::string dest;
//				ArdbServerConfig& cfg;
//				ReplicationService* serv;
//				BackupTask(ArdbServerConfig& c) :
//						cfg(c)
//				{
//				}
//				void Run()
//				{
//					char cmd[cfg.data_base_path.size() + 256];
//					sprintf(cmd, "tar cf %s %s;", dest.c_str(),
//							cfg.data_base_path.c_str());
//					int ret = system(cmd);
//					if (-1 == ret)
//					{
//						ERROR_LOG(
//								"Failed to create backup data archive:%s", dest.c_str());
//					} else
//					{
//						std::string sha1sum_str;
//						ret = sha1sum_file(dest, sha1sum_str);
//						if (-1 == ret)
//						{
//							ERROR_LOG(
//									"Failed to compute sha1sum for data archive:%s", dest.c_str());
//						}
//					}
//					serv->m_is_saving = false;
//					delete this;
//				}
//		};
//		BackupTask* task = new BackupTask(m_server->m_cfg);
//		task->Start();
//		return 0;
//	}
//
//	int ReplicationService::CheckPoint()
//	{
//		if (m_is_saving)
//		{
//			return 1;
//		}
//		m_server->m_db->CloseAll();
//		m_is_saving = true;
//
//		return 0;
//	}
//
//	int ReplicationService::WriteBinLog(Channel* sourceConn,
//			RedisCommandFrame& cmd)
//	{
//		//Write bin log & log index
//		//Send to slave conns
//		return 0;
//	}
//
//	int ReplicationService::ServSync(Channel* conn, RedisCommandFrame& syncCmd)
//	{
//		//conn->DetachFD();
//		//send latest checkpoint file
//		//send latest binlog
//		return 0;
//	}

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

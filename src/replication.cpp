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
		m_server->m_db->CloseAll();
		m_is_saving = true;
		int ret = 0;
		char cmd[m_server->m_cfg.data_base_path.size() + 256];
		m_server->m_cfg.repl_data_dir = "./repl";
		make_dir(m_server->m_cfg.repl_data_dir);
		std::string dest = m_server->m_cfg.repl_data_dir + "/ardb_data.save";
		std::string shasumfile = m_server->m_cfg.repl_data_dir
		        + "/ardb_data.sha1sum";
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
}

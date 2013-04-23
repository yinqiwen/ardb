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
			m_server(serv), m_in_checkpoint(false)
	{
		Start();
		CheckPoint();
	}

	void ReplicationService::Run()
	{
		struct CheckPointTask: public Thread
		{
				void Run()
				{
					Thread::Sleep(1);
				}
		} ckpt(this);
		ckpt.Start();
		m_serv.Start();
	}

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

	int ReplicationService::CheckPoint()
	{
		if (m_in_checkpoint)
		{
			return 1;
		}
		if (m_server->m_cfg.checkpoint_interval > 0)
		{

		}
		m_server->m_db->CloseAll();
		m_in_checkpoint = true;

		char cmd[m_server->m_cfg.data_base_path.size() + 256];
		sprintf(cmd, "tar cf %s %s;", m_server->m_cfg.data_base_path.c_str());
		system(cmd);
		//spawn a thread to create data tar file
		//tar czf *.tar dir, checksum
		return 0;
	}

	int ReplicationService::WriteBinLog(Channel* sourceConn,
			RedisCommandFrame& cmd)
	{
		//Write bin log & log index
		//Send to slave conns
		return 0;
	}

	int ReplicationService::ServSync(Channel* conn, RedisCommandFrame& syncCmd)
	{
		//conn->DetachFD();
		//send latest checkpoint file
		//send latest binlog
		return 0;
	}
}

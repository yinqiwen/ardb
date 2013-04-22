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
	}

	void ReplicationService::Run()
	{
		m_serv.Start();
	}

	int ReplicationService::CheckPoint()
	{
		if (m_in_checkpoint)
		{
			return 1;
		}
		m_server->m_db->CloseAll();
		m_in_checkpoint = true;
		//spawn a thread to create data tar file
        //tar czf *.tar.gz dir
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

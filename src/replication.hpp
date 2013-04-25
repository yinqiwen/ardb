/*
 * replication.hpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */

#ifndef REPLICATION_HPP_
#define REPLICATION_HPP_
#include "channel/all_includes.hpp"
#include "ardb.hpp"
#include "util/thread.hpp"
#include <stdio.h>

using namespace ardb::codec;

namespace ardb
{
	struct SlaveConn
	{
			Channel* conn;
			uint32 synced_checkpoint;
			uint32 synced_cmd_seq;
	};

	class BinLogWriter: public Runnable
	{
		private:
			FILE* m_index_log;
			FILE* m_bin_log;
			void Run();
	};

	class ArdbServer;
	class ReplicationService: public Thread
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			bool m_is_saving;
			uint32 m_last_save;
			void Run();
//			int Backup(const std::string& dstfile);
		public:
			ReplicationService(ArdbServer* serv);
//			int Start();
//			int CheckPoint();
//			int WriteBinLog(Channel* sourceConn, RedisCommandFrame& cmd);
//			int ServSync(Channel* conn, RedisCommandFrame& syncCmd);
//			void SyncMaster(const std::string& host, int port);
//			void HandleSyncedCommand(RedisCommandFrame& syncCmd);
			int Save();
			int BGSave();
			uint32 LastSave()
			{
				return m_last_save;
			}
	};
}

#endif /* REPLICATION_HPP_ */

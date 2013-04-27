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

	class SlaveClient: public ChannelUpstreamHandler<RedisCommandFrame>,
			public ChannelUpstreamHandler<Buffer>
	{
		private:
			ArdbServer* m_serv;
			Channel* m_client;
			SocketHostAddress m_master_addr;
			int m_state;
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<Buffer>& e);
			void ChannelClosed(ChannelHandlerContext& ctx,
								ChannelStateEvent& e);
		public:
			SlaveClient(ArdbServer* serv) :
					m_serv(serv), m_client(NULL), m_state(0)
			{
			}
			int ConnectMaster(const std::string& host, uint32 port);
			void CloseSlave();
	};

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
			int Slaveof(const std::string& host, int port);
			int Save();
			int BGSave();
			uint32 LastSave()
			{
				return m_last_save;
			}
	};
}

#endif /* REPLICATION_HPP_ */

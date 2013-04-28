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
			public ChannelUpstreamHandler<Buffer>,
			public Runnable
	{
		private:
			ArdbServer* m_serv;
			Channel* m_client;
			SocketHostAddress m_master_addr;
			uint32 m_chunk_len;
			int m_slave_state;
			bool m_cron_inited;
			RedisCommandDecoder m_decoder;
			NullRedisReplyEncoder m_encoder;
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<Buffer>& e);
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void ChannelConnected(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void Run();
		public:
			SlaveClient(ArdbServer* serv) :
					m_serv(serv), m_client(NULL), m_chunk_len(0), m_slave_state(
							0), m_cron_inited(false)
			{
			}
			int ConnectMaster(const std::string& host, uint32 port);
			void Close();
			void Stop();
	};

//	class MasterConn:public Runnable
//	{
//		private:
//			void Run();
//	};

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

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
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#include "util/thread/lock_guard.hpp"
#include <stdio.h>

using namespace ardb::codec;

namespace ardb
{
	struct SlaveConn
	{
			Channel* conn;
			uint32 synced_cmd_seq;
			uint32 state;
	};

	struct RedisCommandFrameFeed
	{

			RedisCommandFrame cmd;
			uint32 cmd_seq;
			RedisCommandFrameFeed(RedisCommandFrame& c, uint32 seq) :
					cmd(c), cmd_seq(seq)
			{
			}
	};

	class SyncCommandQueue
	{
		private:
			typedef std::deque<RedisCommandFrameFeed*> MemCommandQueue;
			MemCommandQueue m_mem_queue;
		public:
			void Offer(RedisCommandFrame& cmd, uint32 seq);
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
			bool m_ping_recved;
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
			void Timeout();
			void Run();
		public:
			SlaveClient(ArdbServer* serv) :
					m_serv(serv), m_client(NULL), m_chunk_len(0), m_slave_state(
					        0), m_cron_inited(false), m_ping_recved(false)
			{
			}
			int ConnectMaster(const std::string& host, uint32 port);
			void Close();
			void Stop();
	};

	class ReplicationService: public Thread,
	        public SoftSignalHandler,
	        public ChannelUpstreamHandler<Buffer>
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			bool m_is_saving;
			uint32 m_last_save;
			DBID m_current_db;
			void Run();
			typedef std::deque<Channel*> SyncClientQueue;

			typedef std::map<uint32, SlaveConn> SlaveConnTable;
			SyncClientQueue m_waiting_slaves;
			ThreadMutexLock m_slaves_lock;
			SlaveConnTable m_slaves;

			SoftSignalChannel* m_soft_signal;

			SyncCommandQueue m_sync_queue;
			void Routine();
			void PingSlaves();
			void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);
			void ChannelClosed(ChannelHandlerContext& ctx,
			        ChannelStateEvent& e);
			void MessageReceived(ChannelHandlerContext& ctx,
			        MessageEvent<Buffer>& e)
			{

			}
		public:
			ReplicationService(ArdbServer* serv);
			void ServSlaveClient(Channel* client);
			void FeedSlaves(const DBID& dbid, RedisCommandFrame& syncCmd);
			int Save();
			int BGSave();
			uint32 LastSave()
			{
				return m_last_save;
			}
	};
}

#endif /* REPLICATION_HPP_ */

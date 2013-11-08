/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * slave_client.hpp
 *
 *  Created on: 2013-09-09      Author: wqy
 */

#ifndef SLAVE_CLIENT_HPP_
#define SLAVE_CLIENT_HPP_
#include "channel/all_includes.hpp"
#include "util/mmap.hpp"
#include "ardb.hpp"
#include "rdb.hpp"
#include "repl_backlog.hpp"

using namespace ardb::codec;

namespace ardb
{
	struct ArdbConnContext;
	class ArdbServer;
	class Slave: public ChannelUpstreamHandler<RedisMessage>
	{
		private:
			ArdbServer* m_serv;
			Channel* m_client;
			SocketHostAddress m_master_addr;
			uint32 m_slave_state;
			bool m_cron_inited;
			uint32 m_ping_recved_time;
			RedisMessageDecoder m_decoder;
			NullRedisReplyEncoder m_encoder;

			uint8 m_server_type;
			bool m_server_support_psync;
			/*
			 * empty means all db
			 */
			DBIDSet m_sync_dbs;

			ArdbConnContext *m_actx;

			/**
			 * Redis dump file
			 */
			RedisDumpFile* m_rdb;
			ReplBacklog& m_backlog;

			void HandleRedisCommand(Channel* ch, RedisCommandFrame& cmd);
			void HandleRedisReply(Channel* ch, RedisReply& reply);
			void HandleRedisDumpChunk(Channel* ch, RedisDumpFileChunk& chunk);
			void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisMessage>& e);
			//void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisReply>& e);
			void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
			void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
			void Timeout();
			void Routine();
			void InitCron();
			RedisDumpFile* GetNewRedisDumpFile();
			ArdbConnContext* GetArdbConnContext();
			static void LoadRDBRoutine(void* cb);
		public:
			Slave(ArdbServer* serv);
			bool Init();
			const SocketHostAddress& GetMasterAddress()
			{
				return m_master_addr;
			}
			void SetSyncDBs(DBIDSet& dbs)
			{
				m_sync_dbs = dbs;
			}
			int ConnectMaster(const std::string& host, uint32 port);
			void Close();
			void Stop();
			bool IsMasterConnected();
	};
}

#endif /* SLAVE_CLIENT_HPP_ */

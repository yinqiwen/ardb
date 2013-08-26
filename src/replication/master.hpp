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
 * master.hpp
 *
 *  Created on: 2013-08-22
 *      Author: wqy
 */

#ifndef MASTER_HPP_
#define MASTER_HPP_

#include "channel/all_includes.hpp"
#include "util/thread/thread.hpp"
#include "util/concurrent_queue.hpp"
#include "ardb.hpp"
#include "repl.hpp"
#include "repl_backlog.hpp"
#include <map>

using namespace ardb::codec;

namespace ardb
{
	struct ReplInstruction
	{
			void* ptr;
			uint8 type;
			ReplInstruction(void* p, uint8 t) :
					ptr(p), type(t)
			{
			}
	};

	struct SlaveConnection
	{
			Channel* conn;
			std::string server_key;
			int64 sync_offset;
			uint64 acktime;
			bool isRedisSlave;
			uint8 state;
			SlaveConnection() :
					conn(NULL), sync_offset(0), acktime(0), isRedisSlave(false), state(
							0)
			{
			}
	};

	class ArdbServer;
	class Master: public Thread,
			public SoftSignalHandler,
			public ChannelUpstreamHandler<RedisCommandFrame>
	{
		private:
			ChannelService m_channel_service;
			ArdbServer* m_server;
			SoftSignalChannel* m_notify_channel;
			MPSCQueue<ReplInstruction> m_inst_queue;
			typedef std::map<uint32, SlaveConnection*> SlaveConnTable;
			SlaveConnTable m_slave_table;

			ReplBacklog m_backlog;

			void Run();
			void OnHeartbeat();
			void OnInstructions();

			void SyncSlave(SlaveConnection& slave);

			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);
			void OfferReplInstruction(ReplInstruction& inst);
		public:
			Master(ArdbServer* server);
			int Init();
			void AddSlave(Channel* slave, RedisCommandFrame& cmd);
			void FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd);
	};
}

#endif /* MASTER_HPP_ */

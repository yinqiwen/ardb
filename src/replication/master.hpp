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
#include "util/thread/thread_mutex.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "db.hpp"
#include "repl.hpp"
#include "repl_backlog.hpp"
#include <map>

using namespace ardb::codec;

namespace ardb
{
    struct ReplicationInstruction
    {
            void* ptr;
            uint8 type;
            ReplicationInstruction(void* p = NULL, uint8 t = 0) :
                    ptr(p), type(t)
            {
            }
    };

    struct RedisReplCommand
    {
            RedisCommandFrame cmd;
            DBID dbid;
            Channel* src;
    };

    struct SlaveConnection
    {
            Channel* conn;
            std::string server_key;
            int64 sync_offset;
            uint32 acktime;
            uint32 port;
            int repldbfd;
            bool isRedisSlave;
            uint8 state;

            DBID syncing_from;

            DBIDSet include_dbs;
            DBIDSet exclude_dbs;
            SlaveConnection() :
                    conn(NULL), sync_offset(0), acktime(0), port(0), repldbfd(-1), isRedisSlave(false), state(0), syncing_from(
                            ARDB_GLOBAL_DB)
            {
            }
    };

    class ArdbServer;
    class Master: public Runnable, public SoftSignalHandler, public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            ChannelService m_channel_service;
            ArdbServer* m_server;
            SoftSignalChannel* m_notify_channel;
            MPSCQueue<ReplicationInstruction> m_inst_queue;
            typedef TreeMap<uint32, SlaveConnection*>::Type SlaveConnTable;
            typedef TreeMap<uint32, uint32>::Type SlavePortTable;
            SlaveConnTable m_slave_table;
            SlavePortTable m_slave_port_table;
            ThreadMutex m_port_table_mutex;

            ReplBacklog& m_backlog;
            bool m_dumping_db;
            int64 m_dumpdb_offset;

            time_t m_repl_no_slaves_since;
            volatile bool m_backlog_enable;

            Thread* m_thread;
            bool m_thread_running;

            void Run();
            void OnHeartbeat();
            void OnInstructions();
            void OnDumpComplete();

            void FullResyncRedisSlave(SlaveConnection& slave);
            void FullResyncArdbSlave(SlaveConnection& slave);
            void SyncSlave(SlaveConnection& slave);

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);
            void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);
            void OfferReplInstruction(ReplicationInstruction& inst);
            void WriteCmdToSlaves(const RedisCommandFrame& cmd);
            void WriteSlaves(const RedisReplCommand* cmd);
        public:
            Master(ArdbServer* server);
            int Init();
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void AddSlavePort(Channel* slave, uint32 port);
            void FeedSlaves(Channel* src, const DBID& dbid, RedisCommandFrame& cmd);
            void SendDumpToSlave(SlaveConnection& slave);
            void SendCacheToSlave(SlaveConnection& slave);
            size_t ConnectedSlaves();
            void Stop();
            ReplBacklog& GetReplBacklog()
            {
                return m_backlog;
            }
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
            ~Master();
    };
}

#endif /* MASTER_HPP_ */

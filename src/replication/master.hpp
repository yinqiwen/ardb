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
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "repl.hpp"
#include "repl_backlog.hpp"
#include "codec.hpp"
#include "iterator.hpp"
#include <map>

using namespace ardb::codec;

namespace ardb
{

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
            DBIDSet syncing_dbs;

            Iterator* db_iter;
            uint64 repl_chsm_before_sync;
            DBID repl_dbid_before_sync;

            std::string GetAddress();
            SlaveConnection() :
                    conn(NULL), sync_offset(0), acktime(0), port(0), repldbfd(-1), isRedisSlave(false), state(0), syncing_from(
                            0), db_iter(NULL), repl_chsm_before_sync(0), repl_dbid_before_sync(0)
            {
            }
    };

    class Master: public Thread, public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            ChannelService m_channel_service;
            typedef TreeMap<uint32, SlaveConnection*>::Type SlaveConnTable;
            SlaveConnTable m_slave_table;

            bool m_dumping_rdb;
            bool m_dumping_ardb;
            int64 m_dump_rdb_offset;
            int64 m_dump_ardb_offset;

            time_t m_repl_no_slaves_since;
            volatile bool m_backlog_enable;

            void Run();
            void OnHeartbeat();
            void OnRedisDumpComplete();
            void OnArdbDumpComplete();

            void FullResyncRedisSlave(SlaveConnection& slave);
            void FullResyncArdbSlave(SlaveConnection& slave);
            void SyncSlave(SlaveConnection& slave);

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);
            void WriteCmdToSlaves(const RedisCommandFrame& cmd);
            void WriteSlaves(DBID db, const RedisCommandFrame& cmd);
            void SendRedisDumpToSlave(SlaveConnection& slave);
            void SendArdbDumpToSlave(SlaveConnection& slave);
            void SendCacheToSlave(SlaveConnection& slave);
            SlaveConnection& GetSlaveConn(Channel* conn);

            static void OnAddSlave(Channel*, void*);
            static void OnFeedSlave(Channel*, void*);
            static void OnDisconnectAllSlaves(Channel*, void*);
            static void OnDumpFileSendComplete(void* data);
            static void OnDumpFileSendFailure(void* data);
            static int DumpRDBRoutine(void* cb);
        public:
            Master();
            int Init();
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void AddSlavePort(Channel* slave, uint32 port);
            void FeedSlaves(const DBID& dbid, RedisCommandFrame& cmd);

            size_t ConnectedSlaves();
            void Stop();
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
            ~Master();
    };
}

#endif /* MASTER_HPP_ */

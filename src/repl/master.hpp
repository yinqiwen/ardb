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
#include "common/common.hpp"
#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include <map>

using namespace ardb::codec;

OP_NAMESPACE_BEGIN
    class SlaveConn;
    typedef TreeMap<uint32, SlaveConn*>::Type SlaveConnTable;
    class Master: public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            SlaveConnTable m_slaves;
            time_t m_repl_noslaves_since;

            void OnHeartbeat();

            void SyncSlave(SlaveConn* slave);

            void ClearNilSlave();

            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);

            void SyncWAL(SlaveConn* slave);
            void SendSnapshotToSlave(SlaveConn* slave);
            int CreateSnapshot(bool is_redis_type);
            static int DumpRDBRoutine(void* cb);
        public:
            Master();
            int Init();
            void AddSlave(SlaveConn* slave);
            void AddSlave(Channel* slave, RedisCommandFrame& cmd);
            void SetSlavePort(Channel* slave, uint32 port);
            size_t ConnectedSlaves();
            void SyncWAL();
            void FullResyncSlaves(bool is_redis_type);
            void PrintSlaves(std::string& str);
            void DisconnectAllSlaves();
            ~Master();
    };
OP_NAMESPACE_END

#endif /* MASTER_HPP_ */

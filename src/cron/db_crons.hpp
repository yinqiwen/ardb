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

#ifndef DB_HELPERS_HPP_
#define DB_HELPERS_HPP_
#include <stdint.h>
#include <float.h>
#include <map>
#include <vector>
#include <list>
#include <string>
#include <deque>
#include "common.hpp"
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "util/lru.hpp"
#include "ardb_server.hpp"
namespace ardb
{

    class ExpireCheck: public Runnable
    {
        private:
            DBID m_checking_db;
            ArdbServer* m_server;
            void Run();
            static void ExpireCheckCallback(const DBID& db, const Slice& key, void* data);
        public:
            ExpireCheck(ArdbServer* serv);
    };


    class CompactGC:public Runnable
    {
        private:
            ArdbServer* m_server;
            volatile uint64 m_read_count;
            volatile uint64 m_read_latency;
            volatile uint64 m_write_count;
            volatile uint64 m_write_latency;
            time_t m_last_compact;
            uint64 m_last_compact_duration;
            typedef std::vector<uint32> Counts;
            CompactParams m_compact_trigger;
            Counts m_latency_exceed_counts;
            bool m_is_compacting;
            uint32 m_max_read_latency;
            uint32 m_max_write_latency;
            void Run();
        public:
            CompactGC(ArdbServer* serv);
            void StatReadLatency(uint64 latency);
            void StatWriteLatency(uint64 latency);
            double AverageReadLatency();
            double AverageWriteLatency();
            uint32 MaxReadLatency();
            uint32 MaxWriteLatency();
            double PeriodReadOps();
            double PeriodWriteOps();
            uint64 PeriodWriteCount();
            uint64 PeriodReadCount();
            std::string LastCompactTime();
            uint64 LastCompactDuration();
            bool IsCompacting();
    };

    class DBCrons: public Thread
    {
        private:
            static DBCrons* g_crons;
            ChannelService m_serv;
            ArdbServer* m_db_server;
            ExpireCheck* m_expire_check;
            CompactGC* m_gc;
            void Run();
            DBCrons();
        public:
            int Init(ArdbServer* server);
            CompactGC& GetCompactGC();
            void StopSelf();
            static DBCrons& GetSingleton();
            static void DestroySingleton();

    };
}

#endif /* DB_HELPERS_HPP_ */

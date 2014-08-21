/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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
#ifndef STATISTICS_HPP_
#define STATISTICS_HPP_

#include "common/common.hpp"
#include "util/atomic.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "util/string_helper.hpp"
#define ARDB_OPS_SEC_SAMPLES 16
OP_NAMESPACE_BEGIN

    struct ServerStatistics
    {
            volatile uint64 stat_numcommands;
            volatile uint64 ops_sec_last_sample_ops;
            volatile uint32 ops_sec_idx;
            volatile uint64 connections_received;
            uint64 ops_sec_samples[ARDB_OPS_SEC_SAMPLES];
            volatile uint64 instantaneous_ops;
            volatile uint64 refused_connections;
            int64 ops_limit;
            ServerStatistics() :
                    stat_numcommands(0), ops_sec_last_sample_ops(0), ops_sec_idx(0), connections_received(0), instantaneous_ops(
                            0),refused_connections(0), ops_limit(0)
            {
                memset(ops_sec_samples, 0, sizeof(ops_sec_samples));
            }
            uint64 GetOperationsPerSecond()
            {
                int j;
                uint64 sum = 0;

                for (j = 0; j < ARDB_OPS_SEC_SAMPLES; j++)
                    sum += ops_sec_samples[j];
                return sum / ARDB_OPS_SEC_SAMPLES;
            }
    };

    struct DBLatencyStatistics
    {
            volatile uint64 total_read_latency;
            volatile uint64 read_count;
            volatile uint64 max_read_latency;
            volatile uint64 total_seek_latency;
            volatile uint64 seek_count;
            volatile uint64 max_seek_latency;
            volatile uint64 total_write_latency;
            volatile uint64 write_count;
            volatile uint64 max_write_latency;
            volatile uint64 write_count_since_last_compact;

            time_t stat_start_time;
            DBLatencyStatistics() :
                    total_read_latency(0), read_count(0), max_read_latency(0), total_seek_latency(0), seek_count(0), max_seek_latency(
                            0), total_write_latency(0), write_count(0), max_write_latency(0), write_count_since_last_compact(
                            0), stat_start_time(time(NULL))
            {
            }
            void StatReadLatency(uint64 latency)
            {
                atomic_add_uint64(&total_read_latency, latency);
                atomic_add_uint64(&read_count, 1);
                if (max_read_latency == 0 || max_read_latency < latency)
                {
                    max_read_latency = latency;
                }
            }
            void StatWriteLatency(uint64 latency)
            {
                atomic_add_uint64(&total_write_latency, latency);
                atomic_add_uint64(&write_count, 1);
                atomic_add_uint64(&write_count_since_last_compact, 1);
                if (max_write_latency == 0 || max_write_latency < latency)
                {
                    max_write_latency = latency;
                }
            }
            void StatSeekLatency(uint64 latency)
            {
                atomic_add_uint64(&total_seek_latency, latency);
                atomic_add_uint64(&seek_count, 1);
                if (max_seek_latency == 0 || max_seek_latency < latency)
                {
                    max_seek_latency = latency;
                }
            }
            void Clear()
            {
                total_read_latency = 0;
                read_count = 0;
                total_seek_latency = 0;
                seek_count = 0;
                total_write_latency = 0;
                write_count = 0;
                max_seek_latency = 0;
                max_read_latency = 0;
                max_write_latency = 0;
                stat_start_time = time(NULL);
            }
            const std::string& PrintStat(std::string& str)
            {
                time_t elpased = time(NULL) - stat_start_time;
                str.append("read_ops:").append(elpased == 0 ? "0" : stringfromll(read_count / elpased)).append("\n");
                str.append("write_ops:").append(elpased == 0 ? "0" : stringfromll(write_count / elpased)).append("\n");
                str.append("seek_ops:").append(elpased == 0 ? "0" : stringfromll(seek_count / elpased)).append("\n");
                str.append("avg_read_latency:").append(read_count == 0 ? "0" : stringfromll(total_read_latency / read_count)).append("\n");
                str.append("avg_write_latency:").append(write_count == 0 ? "0" : stringfromll(total_write_latency / write_count)).append("\n");
                str.append("avg_seek_latency:").append(seek_count == 0 ? "0" : stringfromll(total_seek_latency / seek_count)).append("\n");
                str.append("max_read_latency:").append(stringfromll(max_read_latency)).append("us\n");
                str.append("max_write_latency:").append(stringfromll(max_write_latency)).append("us\n");
                str.append("max_seek_latency:").append(stringfromll(max_seek_latency)).append("us\n");
                return str;
            }
    };
    class Statistics
    {
        private:
            typedef TreeMap<std::string, ServerStatistics>::Type ServerStatisticsTable;
            ServerStatisticsTable m_server_stats;
            uint64 m_ops_sec_last_sample_time;


            DBLatencyStatistics m_latency_stat;

        public:
            Statistics();
            void Init();
            int StatReadLatency(uint64 latency);
            int StatWriteLatency(uint64 latency);
            int StatSeekLatency(uint64 latency);
            void IncAcceptedClient(const std::string& server, int v);
            void IncRefusedConnection(const std::string& server);
            int IncRecvCommands(const std::string& server);
            void TrackOperationsPerSecond();
            DBLatencyStatistics& GetLatencyStat()
            {
                return m_latency_stat;
            }
            const std::string& PrintStat(std::string& str);

    };

OP_NAMESPACE_END

#endif /* STATISTICS_HPP_ */

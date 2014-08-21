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
#include "statistics.hpp"
#include "ardb.hpp"
OP_NAMESPACE_BEGIN
    Statistics::Statistics() :
            m_ops_sec_last_sample_time(get_current_epoch_millis())
    {
    }

    void Statistics::Init()
    {
        StringArray& servers = g_db->GetConfig().listen_addresses;
        for (uint32 i = 0; i < servers.size(); i++)
        {
            m_server_stats[servers[i]].ops_limit = g_db->GetConfig().qps_limits[i];
        }
        m_server_stats[MASTER_SERVER_ADDRESS_NAME].ops_limit = 0;
    }
    int Statistics::StatReadLatency(uint64 latency)
    {
        m_latency_stat.StatReadLatency(latency);
        return 0;
    }
    int Statistics::StatWriteLatency(uint64 latency)
    {
        m_latency_stat.StatWriteLatency(latency);
        return 0;
    }
    int Statistics::StatSeekLatency(uint64 latency)
    {
        m_latency_stat.StatSeekLatency(latency);
        return 0;
    }
    void Statistics::IncAcceptedClient(const std::string& server, int v)
    {
        ServerStatistics& stat = m_server_stats[server];
        atomic_add_uint64(&stat.connections_received, v);
    }
    void Statistics::IncRefusedConnection(const std::string& server)
    {
        ServerStatistics& stat = m_server_stats[server];
        atomic_add_uint64(&stat.refused_connections, 1);
    }
    int Statistics::IncRecvCommands(const std::string& server)
    {
        ServerStatistics& stat = m_server_stats[server];
        atomic_add_uint64(&stat.stat_numcommands, 1);
        atomic_add_uint64(&stat.instantaneous_ops, 1);
        if (stat.ops_limit > 0 && stat.instantaneous_ops >= stat.ops_limit)
        {
            return ERR_OVERLOAD;
        }
        return 0;
    }

    void Statistics::TrackOperationsPerSecond()
    {
        uint64 t = get_current_epoch_millis() - m_ops_sec_last_sample_time;
        ServerStatisticsTable::iterator it = m_server_stats.begin();
        while (it != m_server_stats.end())
        {
            ServerStatistics& st = it->second;
            long long ops = st.stat_numcommands - st.ops_sec_last_sample_ops;
            long long ops_sec;
            ops_sec = t > 0 ? (ops * 1000 / t) : 0;
            st.ops_sec_samples[st.ops_sec_idx] = ops_sec;
            st.ops_sec_idx = (st.ops_sec_idx + 1) % ARDB_OPS_SEC_SAMPLES;
            st.ops_sec_last_sample_ops = st.stat_numcommands;
            st.instantaneous_ops = 0;
            it++;
        }
        m_ops_sec_last_sample_time = get_current_epoch_millis();
    }

    const std::string& Statistics::PrintStat(std::string& str)
    {
        m_latency_stat.PrintStat(str);

        std::string substat;
        uint64 total_connections_received = 0;
        uint64 total_commands_processed = 0;
        uint64 instantaneous_ops_per_sec = 0;
        uint64 refused_client = 0;
        ServerStatisticsTable::iterator it = m_server_stats.begin();
        while (it != m_server_stats.end())
        {
            ServerStatistics& st = it->second;
            if (st.stat_numcommands > 0)
            {
                total_connections_received += st.connections_received;
                total_commands_processed += st.stat_numcommands;
                instantaneous_ops_per_sec += st.GetOperationsPerSecond();
                substat.append(it->first).append(": ");
                substat.append("connections_received=").append(stringfromll(st.connections_received)).append(" ");
                substat.append("commands_processed=").append(stringfromll(st.stat_numcommands)).append(" ");
                substat.append("ops_per_sec=").append(stringfromll(st.GetOperationsPerSecond())).append(
                        "\n");
                substat.append("refused_client=").append(stringfromll(st.refused_connections)).append(
                                        "\n");
            }
            it++;
        }
        str.append("total_connections_received:").append(stringfromll(total_connections_received)).append("\n");
        str.append("total_commands_processed:").append(stringfromll(total_commands_processed)).append("\n");
        str.append("instantaneous_ops_per_sec:").append(stringfromll(instantaneous_ops_per_sec)).append("\n");
        str.append("refused_client:").append(stringfromll(refused_client)).append("\n");
        str.append(substat);
        return str;
    }
OP_NAMESPACE_END


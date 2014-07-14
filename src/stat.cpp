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
#include "stat.hpp"
#include "util/string_helper.hpp"

namespace ardb
{
    static const uint32 kMaxPeriodSlot = 100;

    typedef TreeMap<std::string, ServerStat*>::Type ServerStatTable;

    static ServerStatTable g_stat_table;

    ServerStat& ServerStat::GetStatInstance(const std::string& name)
    {
        ServerStatTable::iterator found = g_stat_table.find(name);
        if (found == g_stat_table.end())
        {
            ServerStat* st = new ServerStat;
            g_stat_table[name] = st;
            st->name = name;
            return *st;
        }
        else
        {
            return *(found->second);
        }

    }

    ServerStat::ServerStat() :
            stat_numcommands(0), connected_clients(0), stat_numconnections(0), qps_limit(0)
    {
        now = time(NULL);
        stat_period_numcommands.resize(kMaxPeriodSlot);
    }

    void ServerStat::Run()
    {
        now = time(NULL);
        uint32 idx = now % kMaxPeriodSlot;
        stat_period_numcommands[idx] = 0;
    }
    int ServerStat::IncRecvCommands()
    {
        if (qps_limit > 0 && stat_period_numcommands[now % kMaxPeriodSlot] > (uint64)qps_limit)
        {
            return -1;
        }
        atomic_add_uint64(&stat_numcommands, 1);
        atomic_add_uint64(&(stat_period_numcommands[now % kMaxPeriodSlot]), 1);
        return 0;
    }
    void ServerStat::IncAcceptedClient()
    {
        atomic_add_uint64(&connected_clients, 1);
        atomic_add_uint64(&stat_numconnections, 1);
    }
    void ServerStat::DecAcceptedClient()
    {
        atomic_sub_uint64(&connected_clients, 1);
    }
    double ServerStat::CurrentQPS()
    {
        uint64 total = 0;
        for (uint32 i = 0; i < kMaxPeriodSlot; i++)
        {
            total += stat_period_numcommands[i];
        }
        return (total * 1.0) / kMaxPeriodSlot;
    }

    std::string ServerStat::ToString()
    {
        std::string info;
        info.append("address=").append(name).append(",");
        info.append("total_commands_processed=").append(stringfromll(stat_numcommands)).append(", ");
        info.append("total_connections_received=").append(stringfromll(stat_numconnections)).append(", ");
        char qps[100];
        sprintf(qps, "%.2f", CurrentQPS());
        info.append("qps=").append(qps).append("\r\n");
        //info.append("qps_limit=").append(stringfromll(qps_limit)).append("\r\n");
        return info;
    }

    std::string ServerStat::AllStats()
    {
        std::string info;
        int64 all_cmds = 0;
        int64 all_connections_recved = 0;
        double all_qps = 0;
        ServerStatTable::iterator it = g_stat_table.begin();
        while (it != g_stat_table.end())
        {
            all_cmds += it->second->stat_numcommands;
            all_connections_recved += it->second->stat_numconnections;
            all_qps += it->second->CurrentQPS();
            it++;
        }
        info.append("total_commands_processed:").append(stringfromll(all_cmds)).append("\r\n");
        info.append("total_connections_received:").append(stringfromll(all_connections_recved)).append("\r\n");
        char qps[100];
        sprintf(qps, "%.2f", all_qps);
        info.append("current_qps:").append(qps).append("\r\n");
        uint32 i = 0;
        it = g_stat_table.begin();
        while (it != g_stat_table.end())
        {
            info.append("server").append(stringfromll(i)).append(":").append(it->second->ToString());
            i++;
            it++;
        }
        return info;
    }
    std::string ServerStat::ClientsStat()
    {
        std::string info;
        int64 all_clients = 0;
        ServerStatTable::iterator it = g_stat_table.begin();
        while (it != g_stat_table.end())
        {
            all_clients += it->second->connected_clients;
            it++;
        }
        info.append("connected_clients:").append(stringfromll(all_clients)).append("\r\n");

        uint32 i = 0;
        it = g_stat_table.begin();
        while (it != g_stat_table.end())
        {
            if (it->second->connected_clients > 0)
            {
                info.append("server").append(stringfromll(i)).append(": address=").append(it->second->name).append(
                        ", ");
                info.append("connected_clients=").append(stringfromll(it->second->connected_clients)).append("\r\n");
                i++;
            }
            it++;
        }
        return info;
    }
}


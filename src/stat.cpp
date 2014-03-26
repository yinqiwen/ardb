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

namespace ardb
{
    static const uint32 kMaxPeriodSlot = 100;
    static ServerStat kSignleton;

    ServerStat& ServerStat::GetSingleton()
    {
        return kSignleton;
    }

    ServerStat::ServerStat() :
            stat_numcommands(0), connected_clients(0), stat_numconnections(0)
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
    void ServerStat::IncRecvCommands()
    {
        atomic_add_uint64(&stat_numcommands, 1);
        atomic_add_uint64(&(stat_period_numcommands[now % kMaxPeriodSlot]), 1);
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
        return (total * 1.0 - 1) / kMaxPeriodSlot;
    }
}


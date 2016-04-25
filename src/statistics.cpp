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
OP_NAMESPACE_BEGIN
    static Statistics* g_singleton = NULL;

    void CostTrack::Dump(std::string& str)
    {

    }

    void QPSTrack::Dump(std::string& str, bool clear)
    {
        str.append(Name).append(":").append(stringfromll(msgCount)).append("\r\n");
        str.append(qpsName).append(":").append(stringfromll(QPS())).append("\r\n");
    }

    void CountRefTrack::Dump(std::string& str, bool clear)
    {
        str.append(Name).append(":").append(stringfromll(cb(cbdata))).append("\r\n");
    }

    void CountTrack::Dump(std::string& str, bool clear)
    {
        str.append(Name).append(":").append(stringfromll(count)).append("\r\n");
    }

    CostTrack::CostTrack()
    {

    }
    void CostTrack::SetCostRanges(const CostRanges& ranges)
    {
        this->ranges = ranges;
        recs.resize(ranges.size() + 1);
    }
    void CostTrack::AddCost(uint64 cost)
    {
        atomic_add_uint64(&recs[0].cost, cost);
        atomic_add_uint64(&recs[0].count, 1);
        for (size_t i = 0; i < ranges.size(); i++)
        {
            if (cost >= ranges[i].min && cost <= ranges[i].max)
            {
                atomic_add_uint64(&recs[i + 1].cost, cost);
                atomic_add_uint64(&recs[i + 1].count, 1);
                return;
            }
        }
    }
    void CostTrack::Dump(std::string& str, bool clear)
    {
        if (recs[0].count == 0)
        {
            return;
        }
        uint64 allCount = 0;
        for (size_t i = 0; i < recs.size(); i++)
        {
            if (recs[i].count == 0)
            {
                continue;
            }
            char field[1024];
            std::string countKey, costKey;
            char tmp[1024];
            snprintf(tmp, sizeof(tmp) - 1, "%s:%d[%.4f%%]\r\n", recs[i].count, (double(recs[i].count) / double(recs[0].count)) * 100);
            str.append(tmp);
            snprintf(tmp, sizeof(tmp) - 1, "%s: %d\r\n", recs[i].cost);
            str.append(tmp);
        }
    }

    Statistics::Statistics()
    {
    }

    Statistics& Statistics::GetSingleton()
    {
        if (NULL == g_singleton)
        {
            g_singleton = new Statistics;
        }
        return *g_singleton;
    }
    int Statistics::AddTrack(Track* track)
    {
        m_tracks[track->Name] = track;
        return 0;
    }

    void Statistics::Dump(std::string& info)
    {

    }

OP_NAMESPACE_END


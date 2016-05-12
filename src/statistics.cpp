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

    static void stringStatisticsCallback(const std::string& info, void* data)
    {
        std::string* ss = (std::string*) data;
        ss->append(info).append("\r\n");
    }
    static void logStatisticsCallback(const std::string& info, void* data)
    {
        INFO_LOG("%s", info.c_str());
    }

    void QPSTrack::Dump(TrackDumpCallback* cb, void* data)
    {
        std::string str;
        str.append(name).append(":").append(stringfromll(msgCount));
        cb(str, data);
        str.clear();
        str.append(qpsName).append(":").append(stringfromll(QPS()));
        cb(str, data);
    }

    void CountRefTrack::Dump(TrackDumpCallback* tcb, void* tdata)
    {
        std::string str;
        str.append(name).append(":").append(stringfromll(cb(cbdata)));
        tcb(str, tdata);
    }

    void CountTrack::Dump(TrackDumpCallback* cb, void* data)
    {
        std::string str;
        str.append(name).append(":").append(stringfromll(count));
        cb(str, data);
    }

    CostTrack::CostTrack()
    {
        Clear();
    }
    void CostTrack::SetCostRanges(const CostRanges& ranges)
    {
        this->ranges = ranges;
        Clear();
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
    void CostTrack::Dump(TrackDumpCallback* cb, void* data)
    {
        if (0 == recs[0].count)
        {
            return;
        }
        for (size_t i = 0; i < recs.size(); i++)
        {
            if (0 == recs[i].count)
            {
                continue;
            }
            char range[1024];
            if (i == 0)
            {
                sprintf(range, "all");
            }
            else
            {
                if (ranges[i - 1].max == UINT64_MAX)
                {
                    snprintf(range, sizeof(range) - 1, "range[%llu-]", ranges[i - 1].min);
                }
                else
                {
                    snprintf(range, sizeof(range) - 1, "range[%llu-%llu]", ranges[i - 1].min, ranges[i - 1].max);
                }
            }

            char tmp[1024];
            snprintf(tmp, sizeof(tmp) - 1, "coststat_%s_%s:calls=%llu,costs=%llu,cost_per_call=%llu,percents=%.4f%%", name.c_str(), range, recs[i].count,
                    recs[i].cost, recs[i].cost / recs[i].count, (double(recs[i].count) / double(recs[0].count)) * 100);
            cb(tmp, data);
        }
    }

    void CostTrack::Clear()
    {
        recs.clear();
        recs.resize(ranges.size() + 1);
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
        m_tracks.push_back(track);
        if (InstanceOf<QPSTrack>(track).OK)
        {
            m_qps_tracks.push_back(track);
        }
        return 0;
    }

    void Statistics::TrackQPSPerSecond()
    {
        for (size_t i = 0; i < m_qps_tracks.size(); i++)
        {
            ((QPSTrack*) m_qps_tracks[i])->trackQPSPerSecond();
        }
    }

    void Statistics::Clear(int type)
    {
        for (size_t i = 0; i < m_tracks.size(); i++)
        {
            if (m_tracks[i]->GetType() & type)
            {
                m_tracks[i]->Clear();
            }
        }
    }

    void Statistics::DumpString(std::string& info, int flags, int type)
    {
        Dump(stringStatisticsCallback, &info, flags, type);
    }
    void Statistics::DumpLog(int flags, int type)
    {
        Dump(logStatisticsCallback, NULL, flags, type);
    }

    void Statistics::Dump(TrackDumpCallback* cb, void* data, int flags, int type)
    {
        for (size_t i = 0; i < m_tracks.size(); i++)
        {
            if (m_tracks[i]->GetType() & type)
            {
                if (m_tracks[i]->dump_flags & flags)
                {
                    m_tracks[i]->Dump(cb, data);
                    if (flags & STAT_DUMP_PERIOD & m_tracks[i]->dump_flags)
                    {
                        if (m_tracks[i]->dump_flags & STAT_DUMP_PERIOD_CLEAR)
                        {
                            m_tracks[i]->Clear();
                        }
                    }
                }
            }
        }
    }

OP_NAMESPACE_END


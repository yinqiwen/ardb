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
#include "db_crons.hpp"

namespace ardb
{
    static time_t g_init_time = time(NULL);

    bool CompactParaCompareFunc(const CompactParam& i, const CompactParam& j)
    {
        return i.latency_limit > j.latency_limit;
    }

    CompactGC::CompactGC(ArdbServer* serv) :
            m_server(serv), m_read_count(0), m_read_latency(0), m_write_count(0), m_write_latency(0), m_last_compact(0)
    {
        m_compact_trigger = serv->GetConfig().compact_paras;
        std::sort(m_compact_trigger.begin(), m_compact_trigger.end(), CompactParaCompareFunc);
        m_latency_exceed_counts.resize(m_compact_trigger.size());
    }
    void CompactGC::StatReadLatency(uint64 latency)
    {
        atomic_add_uint64(&m_read_count, 1);
        atomic_add_uint64(&m_read_latency, latency);

        for (uint32 i = 0; i < m_latency_exceed_counts.size(); i++)
        {
            if (latency >= m_server->GetConfig().compact_paras[i].latency_limit * 1000)
            {
                atomic_add_uint32(&(m_latency_exceed_counts[i]), 1);
                break;
            }
        }
    }

    void CompactGC::StatWriteLatency(uint64 latency)
    {
        atomic_add_uint64(&m_write_count, 1);
        atomic_add_uint64(&m_write_latency, latency);
    }

    double CompactGC::AverageReadLatency()
    {
        return m_read_count == 0 ? 0 : m_read_latency * 1.0 / m_read_count;
    }
    double CompactGC::AverageWriteLatency()
    {
        return m_write_count == 0 ? 0 : m_write_latency * 1.0 / m_write_count;
    }

    std::string CompactGC::LastCompactTime()
    {
        if (0 == m_last_compact)
        {
            return "";
        }
        struct tm tt;
        if (NULL == localtime_r(&m_last_compact, &tt))
        {
            return "";
        }

        char tmp[1024];
        size_t len = strftime(tmp, sizeof(tmp), "%F %T", &tt);
        return std::string(tmp, len);
    }

    void CompactGC::Run()
    {
        if (!m_server->GetConfig().compact_enable)
        {
            return;
        }
        time_t now = time(NULL);
        bool should_compact = false;
        if (m_last_compact > 0)
        {
            if (now - m_last_compact < m_server->GetConfig().compact_min_interval)
            {
                return;
            }
            if (now - m_last_compact >= m_server->GetConfig().compact_max_interval)
            {
                should_compact = true;
            }
        }
        if (!should_compact)
        {
            for (uint32 i = 0; i < m_latency_exceed_counts.size(); i++)
            {
                if (m_latency_exceed_counts[i] >= m_compact_trigger[i].exceed_count)
                {
                    should_compact = true;
                    break;
                }
            }
        }
        if (!should_compact && now - g_init_time >= m_server->GetConfig().compact_max_interval)
        {
            should_compact = true;
        }
        if (should_compact)
        {
            m_last_compact = now;
            m_read_count = 0;
            m_read_latency = 0;
            m_write_count = 0;
            m_write_latency = 0;
            m_latency_exceed_counts.clear();
            m_latency_exceed_counts.resize(m_compact_trigger.size());
            INFO_LOG("Start compact DB.");
            uint64 start = get_current_epoch_millis();
            m_server->GetDB().CompactAll(true);
            uint64 end = get_current_epoch_millis();
            INFO_LOG("Cost %llums to compact DB.", end - start);
        }
    }
}

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
#include "util/time_helper.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "util/string_helper.hpp"
#include <vector>
#define ARDB_OPS_SEC_SAMPLES 16

//dump flags
#define STAT_DUMP_PERIOD        2   //dump periodically into log
#define STAT_DUMP_INFO_CMD      4   //dump with info command
#define STAT_DUMP_PERIOD_CLEAR  8   //clear after period dump
#define STAT_DUMP_DEFAULT  (STAT_DUMP_PERIOD|STAT_DUMP_INFO_CMD)
#define STAT_DUMP_ALL  (STAT_DUMP_PERIOD|STAT_DUMP_INFO_CMD|STAT_DUMP_PERIOD_CLEAR)

#define STAT_TYPE_COUNT 1
#define STAT_TYPE_COST  2
#define STAT_TYPE_QPS   4
#define STAT_TYPE_ALL   (STAT_TYPE_COUNT|STAT_TYPE_COST|STAT_TYPE_QPS)
OP_NAMESPACE_BEGIN

    typedef void TrackDumpCallback(const std::string& info, void* data);

    struct Track
    {
            std::string name;
            int dump_flags;
            Track() :
                    dump_flags(STAT_DUMP_DEFAULT)
            {
            }
            virtual int GetType() = 0;
            virtual void Dump(TrackDumpCallback* cb, void* data) = 0;
            virtual void Clear() = 0;
            virtual ~Track()
            {
            }
    };

    struct CountTrack: public Track
    {
            volatile uint64_t count;
            CountTrack() :
                    count(0)
            {
            }
            int GetType()
            {
                return STAT_TYPE_COUNT;
            }
            void Clear()
            {
                count = 0;
            }
            void Dump(TrackDumpCallback* cb, void* data);
            uint64_t Add(uint64_t inc)
            {
                return atomic_add_uint64(&count, inc);
            }
            void Set(uint64_t v)
            {
                count = v;
            }
    };

    typedef uint64_t CountCallback(void* data);
    struct CountRefTrack: public Track
    {
            CountCallback* cb;
            void* cbdata;
            CountRefTrack(CountCallback* callback, void* data) :
                    cb(callback), cbdata(data)
            {
            }
            int GetType()
            {
                return STAT_TYPE_COUNT;
            }
            void Clear()
            {
            }
            void Dump(TrackDumpCallback* cb, void* data);
    };

    struct QPSTrack: public Track
    {
            uint64_t qpsSamples[16];
            int qpsSampleIdx;
            uint64_t lastMsgCount;
            uint64_t lastSampleTime;
            uint64_t msgCount;
            std::string qpsName;
            QPSTrack() :
                    qpsSampleIdx(0), lastMsgCount(0), lastSampleTime(0), msgCount(0)
            {
                memset(qpsSamples, 0, sizeof(qpsSamples));
            }
            int GetType()
            {
                return STAT_TYPE_QPS;
            }
            void trackQPSPerSecond()
            {
                uint64_t now = get_current_epoch_millis();
                uint64_t qps = 0;
                if (lastSampleTime == 0)
                {
                    lastSampleTime = now;
                }
                if (now > lastSampleTime)
                {
                    qps = ((msgCount - lastMsgCount) * 1000) / (now - lastSampleTime);
                }
                qpsSamples[qpsSampleIdx] = qps;
                qpsSampleIdx = (qpsSampleIdx + 1) % arraysize(qpsSamples);
                lastMsgCount = msgCount;
                lastSampleTime = now;
            }
            uint64_t QPS()
            {
                uint64_t sum = 0;
                for (size_t i = 0; i < arraysize(qpsSamples); i++)
                {
                    sum += qpsSamples[i];
                }
                return sum / arraysize(qpsSamples);
            }
            void IncMsgCount(uint64_t inc)
            {
                atomic_add_uint64(&msgCount, inc);
            }
            void Clear()
            {
                memset(qpsSamples, 0, sizeof(qpsSamples));
                qpsSampleIdx = 0;
                lastMsgCount = 0;
                msgCount = 0;
                lastSampleTime = 0;
            }
            void Dump(TrackDumpCallback* cb, void* data);
    };

    struct CostRange
    {
            uint64 min;
            uint64 max;
            CostRange(uint64 v1 = 0, uint64 v2 = 0) :
                    min(v1), max(v2)
            {
            }
    };
    struct CostRecord
    {
            uint64 cost;
            uint64 count;
            CostRecord() :
                    cost(0), count(0)
            {
            }
    };
    typedef std::vector<CostRange> CostRanges;
    typedef std::vector<CostRecord> CostRecords;

    struct CostTrack: public Track
    {
            CostRanges ranges;
            CostRecords recs;
            CostTrack();
            void SetCostRanges(const CostRanges& ranges);
            void AddCost(uint64 cost);
            void Dump(TrackDumpCallback* cb, void* data);
            void Clear();
            int GetType()
            {
                return STAT_TYPE_COST;
            }
    };

    struct InstantQPS
    {
    	volatile uint64_t count;
    	volatile time_t   ts;
    	InstantQPS():count(0),ts(0)
    	{
    	}
    	uint64_t Inc(time_t now)
    	{
    		if(ts != now)
    		{
    			ts = now;
    			count = 0;
    		}
    		return atomic_add_uint64(&count, 1);
    	}
    };

    class Statistics
    {
        private:
            typedef std::vector<Track*> TrackArray;
            TrackArray m_tracks;
            TrackArray m_qps_tracks;
            Statistics();
        public:
            static Statistics& GetSingleton();
            int AddTrack(Track* track);
            void Dump(TrackDumpCallback* cb, void* data, int flags, int type = STAT_TYPE_ALL);
            void DumpString(std::string& info, int flags, int type = STAT_TYPE_ALL);
            void DumpLog(int flags, int type = STAT_TYPE_ALL);
            void Clear(int type = STAT_TYPE_ALL);
            void TrackQPSPerSecond();

    };

OP_NAMESPACE_END

#endif /* STATISTICS_HPP_ */

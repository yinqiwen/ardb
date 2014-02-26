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
#include "level1_cache.hpp"
#include "ardb.hpp"
#include <algorithm>

namespace ardb
{
    ZSetCache::ZSetCache() :
            CacheItem((uint8) ZSET_META)
    {
        m_estimate_mem_size = sizeof(ZSetCache);
    }

    void ZSetCache::EraseElement(const ZSetCaheElement& e)
    {
        ZSetCacheElementSet::iterator found = m_cache.find(e);
        if (found != m_cache.end())
        {
            uint32 delta = sizeof(ZSetCaheElement);
            delta += found->value.size();
            delta += found->attr.size();
            delta += (uint32)(ZSetCacheElementSet::average_bytes_per_value() + 0.5);
            SubEstimateMemSize(delta);
            m_cache.erase(found);
        }
    }

    int ZSetCache::Rem(ValueData& v)
    {
        CacheWriteLockGuard guard(m_lock);
        Buffer buf1, buf2;
        v.Encode(buf1);
        ZSetCaheElement e;
        e.value.assign(buf1.GetRawReadBuffer(), buf1.ReadableBytes());
        ZSetCacheScoreMap::iterator sit = m_cache_score_dict.find(e.value);
        if (sit != m_cache_score_dict.end())
        {
            e.score = sit->second;
            EraseElement(e);
            uint32 delta = 0;
            delta += e.value.size();
            delta += sizeof(double);
            delta += (uint32)(ZSetCacheElementSet::average_bytes_per_value() + 0.5);
            delta += (uint32)(ZSetCacheScoreMap::average_bytes_per_value() + 0.5);
            SubEstimateMemSize(delta);
            m_cache_score_dict.erase(sit);
            return 0;
        }
        return -1;
    }

    int ZSetCache::Add(const ValueData& score, const ValueData& value, const ValueData& attr, bool thread_safe)
    {
        Buffer buf1, buf2;
        value.Encode(buf1);
        attr.Encode(buf2);
        std::string v, a;
        ZSetCaheElement e;
        e.score = score.NumberValue();
        e.value.assign(buf1.GetRawReadBuffer(), buf1.ReadableBytes());
        e.attr.assign(buf2.GetRawReadBuffer(), buf2.ReadableBytes());
        int ret = 0;
        uint32 delta = 0;
        CacheWriteLockGuard guard(m_lock, thread_safe);
        ZSetCacheScoreMap::iterator sit = m_cache_score_dict.find(e.value);
        if (sit != m_cache_score_dict.end())
        {
            e.score = sit->second;
            if (sit->second == score.NumberValue())
            {
                ZSetCacheElementSet::iterator cit = m_cache.find(e);
                if(cit->attr == e.attr)
                {
                    return ZSET_CACHE_NONEW_ELEMENT;
                }else
                {
                    SubEstimateMemSize(cit->attr.size());
                    AddEstimateMemSize(e.attr.size());
                    cit->attr = e.attr;
                    return ZSET_CACHE_ATTR_CHANGED;
                }
            }
            EraseElement(e);
            sit->second = score.NumberValue();
            e.score = score.NumberValue();
            ret = ZSET_CACHE_SCORE_CHANGED;
        }
        else
        {
            m_cache_score_dict[e.value] = score.NumberValue();
            delta += e.value.size();
            delta += sizeof(double);
            delta += (uint32)(ZSetCacheScoreMap::average_bytes_per_value() + 0.5);
            ret = ZSET_CACHE_NEW_ELEMENT;
        }
        m_cache.insert(e);
        delta += sizeof(ZSetCaheElement);
        delta += e.value.size();
        delta += e.attr.size();
        delta += (uint32)(ZSetCacheElementSet::average_bytes_per_value() + 0.5);
        AddEstimateMemSize(delta);
        return ret;
    }

    int ZSetCache::Add(const ValueData& score, const Slice& value, const Slice& attr, bool thread_safe)
    {
        ValueData v;
        ValueData a;
        v.SetValue(value, true);
        v.SetValue(attr, true);
        return Add(score, v, a, thread_safe);
    }

    void ZSetCache::GetRange(const ZRangeSpec& range, bool with_scores, bool with_attrs, ValueStoreCallback* cb,
            void* cbdata)
    {
        uint64 start = get_current_epoch_millis();
        CacheReadLockGuard guard(m_lock);
        ZSetCaheElement min_ele(range.min.NumberValue(), "");
        ZSetCaheElement max_ele(range.max.NumberValue(), "");
        ZSetCacheElementSet::iterator min_it = m_cache.lower_bound(min_ele);
        ZSetCacheElementSet::iterator max_it = m_cache.lower_bound(max_ele);
        int cursor = 0;
        if (min_it != m_cache.end())
        {
            while (!range.contain_min && min_it != m_cache.end() && min_it->score == range.min.NumberValue())
            {
                min_it++;
            }
            while (range.contain_max && max_it != m_cache.end() && max_it->score == range.max.NumberValue())
            {
                max_it++;
            }
            while (min_it != max_it && min_it != m_cache.end())
            {
                ValueData v;
                Buffer buf(const_cast<char*>(min_it->value.data()), 0, min_it->value.size());
                v.Decode(buf);
                cb(v, cursor++, cbdata);
                if (with_scores)
                {
                    ValueData score;
                    score.SetDoubleValue(min_it->score);
                    cb(score, cursor++, cbdata);
                }
                if (with_attrs)
                {
                    ValueData attr_value;
                    Buffer attr_buf(const_cast<char*>(min_it->attr.data()), 0, min_it->attr.size());
                    attr_value.Decode(attr_buf);
                    cb(attr_value, cursor++, cbdata);
                }
                min_it++;
            }
        }
        uint64 end = get_current_epoch_millis();
        if (end - start > 10)
        {
            DEBUG_LOG("Cost %llums to get %d elements in between range [%.2f, %.2f]", end - start, cursor,
                    range.min.NumberValue(), range.max.NumberValue());
        }
    }
}


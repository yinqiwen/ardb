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
#include "cache_service.hpp"
#include "ardb.hpp"
#include <algorithm>

namespace ardb
{
    ZSetCache::ZSetCache() :
            CacheItem((uint8) ZSET_META)
    {
        m_estimate_mem_size = sizeof(ZSetCache);
    }

    void ZSetCache::DirectAdd(ValueData& score, ValueData& v)
    {
        Buffer buf;
        v.Encode(buf);
        ZSetCaheElement e;
        e.value.assign(buf.GetRawReadBuffer(), buf.ReadableBytes());
        e.score = score.NumberValue();
        m_cache.push_back(e);
        m_estimate_mem_size += sizeof(ZSetCaheElement);
        m_estimate_mem_size += e.value.size();
    }

    void ZSetCache::GetRange(const ZRangeSpec& range, bool with_scores,
            ValueDataArray& res)
    {
        ZSetCaheElement min_ele(range.min.NumberValue(), "");
        ZSetCaheElement max_ele(range.max.NumberValue(), "");
        ZSetCaheElementDeque::iterator min_it = std::lower_bound(
                m_cache.begin(), m_cache.end(), min_ele);
        ZSetCaheElementDeque::iterator max_it = std::lower_bound(
                m_cache.begin(), m_cache.end(), max_ele);
        if (min_it != m_cache.end())
        {
            while (!range.contain_min && min_it != m_cache.end()
                    && min_it->score == range.min.NumberValue())
            {
                min_it++;
            }
            while (!range.contain_max && max_it != m_cache.end()
                    && max_it->score == range.max.NumberValue())
            {
                max_it--;
            }
            while (min_it <= max_it && min_it != m_cache.end())
            {
                ValueData v;
                Buffer buf(const_cast<char*>(min_it->value.data()), 0, min_it->value.size());
                v.Decode(buf);
                res.push_back(v);
                if (with_scores)
                {
                    ValueData score;
                    score.SetDoubleValue(min_it->score);
                    res.push_back(score);
                }
                min_it++;
            }
        }
    }

    static inline uint32 SizeOfDBItemKey(const DBItemKey& key)
    {
        return sizeof(key) + key.key.size();
    }

    CacheService::CacheService(Ardb* db) :
            m_db(db), m_estimate_mem_size(0)
    {
    }

    CacheService::~CacheService()
    {
    }

    int CacheService::Evict(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Erase(cache_key, item);
        if (NULL != item)
        {
            m_estimate_mem_size -= item->GetEstimateMemorySize();
            //DELETE(item);
            item->DecRef();
        }
        return 0;
    }

    void CacheService::LoadZSetCache(const DBItemKey& key, ZSetCache* item)
    {
        //m_db->ZRangeByScoreRange();
        ZSetQueryOptions z_options;
        z_options.withscores = true;
        ValueDataArray values;
        Iterator* iter = NULL;
        ZRangeSpec range;
        range.contain_min = false;
        range.contain_max = false;
        range.min.SetDoubleValue(-DBL_MAX);
        range.max.SetDoubleValue(DBL_MAX);
        m_db->ZRangeByScoreRange(key.db, key.key, range, iter, values,
                z_options, false);
        DELETE(iter);
        ValueDataArray::iterator vit = values.begin();
        while (vit != values.end())
        {
            ValueData& v = *vit;
            vit++;
            ValueData& score = *vit;
            vit++;
            item->DirectAdd(score, v);
        }
    }

    int CacheService::Load(const DBID& dbid, const Slice& key)
    {
        if (m_db->m_config.L1_cache_item_limit <= 0
                || m_db->m_config.L1_cache_memory_limit <= 0)
        {
            return ERR_INVALID_OPERATION;
        }
        if (IsCached(dbid, key))
        {
            return 0;
        }
        int type = m_db->Type(dbid, key);
        if (type != ZSET_META)
        {
            //only zset supported now
            return ERR_INVALID_TYPE;
        }
        ZSetCache* item = NULL;
        DBItemKey cache_key(dbid, key);
        NEW(item, ZSetCache);
        LoadZSetCache(cache_key, item);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        LRUCache<DBItemKey, CacheItem*>::CacheEntry entry;
        if (m_cache.Insert(cache_key, item, entry))
        {
            m_estimate_mem_size -= entry.second->GetEstimateMemorySize();
            m_estimate_mem_size -= SizeOfDBItemKey(entry.first);
            entry.second->DecRef();
        }
        m_estimate_mem_size += item->GetEstimateMemorySize();
        m_estimate_mem_size += SizeOfDBItemKey(cache_key);
        if (m_estimate_mem_size > m_db->m_config.L1_cache_memory_limit)
        {
            if (m_cache.PeekFront(entry) && entry.second != item)
            {
                m_estimate_mem_size -= SizeOfDBItemKey(entry.first);
                m_estimate_mem_size -= entry.second->GetEstimateMemorySize();
                entry.second->DecRef();
                m_cache.PopFront();
            }
        }
        return 0;
    }
    bool CacheService::IsCached(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        return m_cache.Contains(cache_key);
    }
    CacheItem* CacheService::Get(const DBID& dbid, const Slice& key,
            KeyType type)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Get(cache_key, item);
        if (NULL != item && item->GetType() == type)
        {
            item->IncRef();
        }
        return item;
    }
    void CacheService::Recycle(CacheItem* item)
    {
        if (NULL != item)
        {
            item->DecRef();
        }
    }
}


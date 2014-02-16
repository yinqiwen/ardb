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
#include "helper/db_helpers.hpp"
#include <algorithm>

namespace ardb
{
    static const uint32 kCacheLoadType = 1;
    static const uint32 kCacheEvictType = 2;

    static const uint32 kCacheSignal = 1;

    static const uint8 kCacheLoading = 1;
    static const uint8 kCacheLoaded = 2;

    static inline uint32 SizeOfDBItemKey(const DBItemKey& key)
    {
        return sizeof(key) + key.key.size();
    }

    L1Cache::L1Cache(Ardb* db) :
            m_serv(db->m_db_helper->GetChannelService()), m_db(db), m_estimate_mem_size(0), m_signal_notifier(NULL)
    {
        m_signal_notifier = m_serv.NewSoftSignalChannel();
        m_signal_notifier->Register(kCacheSignal, this);
        m_serv.GetTimer().Schedule(this, 100, 100, MILLIS);
    }

    L1Cache::~L1Cache()
    {
    }

    void L1Cache::Run()
    {
        CheckInstQueue();
    }

    void L1Cache::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
    {
        CheckInstQueue();
    }

    void L1Cache::CheckInstQueue()
    {
        CacheInstruction instruction;
        while (m_inst_queue.Pop(instruction))
        {
            if (instruction.type == kCacheEvictType)
            {
                DoEvict(instruction.db, instruction.key);
            }
            else
            {
                DoLoad(instruction.db, instruction.key);
            }
        }
    }

    int L1Cache::DoEvict(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Erase(cache_key, item);
        if (NULL != item)
        {
            m_estimate_mem_size -= item->GetEstimateMemorySize();
            item->DecRef();
        }
        return 0;
    }

    void L1Cache::LoadZSetCache(const DBItemKey& key, ZSetCache* item)
    {
        ZSetQueryOptions z_options;
        z_options.withscores = true;
        z_options.withattr = true;
        ValueDataArray values;
        Iterator* iter = NULL;
        ZRangeSpec range;
        range.contain_min = false;
        range.contain_max = false;
        range.min.SetDoubleValue(-DBL_MAX);
        range.max.SetDoubleValue(DBL_MAX);
        m_db->ZRangeByScoreRange(key.db, key.key, range, iter, values, z_options, false);
        DELETE(iter);
        ValueDataArray::iterator vit = values.begin();
        while (vit != values.end())
        {
            ValueData& v = *vit;
            vit++;
            ValueData& score = *vit;
            vit++;
            ValueData& attr = *vit;
            vit++;
            item->DirectAdd(score, v, attr);
        }
    }

    int L1Cache::DoLoad(const DBID& dbid, const Slice& key)
    {
        if (IsInCache(dbid, key))
        {
            return 0;
        }
        ZSetCache* item = NULL;
        DBItemKey cache_key(dbid, key);
        NEW(item, ZSetCache);
        item->SetStatus(kCacheLoading);
        LRUCache<DBItemKey, CacheItem*>::CacheEntry entry;
        {
            LockGuard<ThreadMutex> guard(m_cache_mutex);
            if (m_cache.Insert(cache_key, item, entry))
            {
                m_estimate_mem_size -= entry.second->GetEstimateMemorySize();
                m_estimate_mem_size -= SizeOfDBItemKey(entry.first);
                entry.second->DecRef();
            }
        }
        LoadZSetCache(cache_key, item);
        item->SetStatus(kCacheLoaded);
        m_estimate_mem_size += item->GetEstimateMemorySize();
        m_estimate_mem_size += SizeOfDBItemKey(cache_key);
        if (m_estimate_mem_size > m_db->m_config.L1_cache_memory_limit)
        {
            LockGuard<ThreadMutex> guard(m_cache_mutex);
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
    void L1Cache::PushInst(const CacheInstruction& inst)
    {
        m_inst_queue.Push(inst);
        m_signal_notifier->FireSoftSignal(kCacheSignal, 0);
    }

    bool L1Cache::IsInCache(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        return m_cache.Contains(cache_key);
    }

    int L1Cache::Evict(const DBID& dbid, const Slice& key)
    {
        if (!IsInCache(dbid, key))
        {
            return 0;
        }
        CacheInstruction inst;
        inst.db = dbid;
        inst.key.assign(key.data(), key.size());
        inst.type = kCacheEvictType;
        PushInst(inst);
        return 0;
    }
    int L1Cache::Load(const DBID& dbid, const Slice& key)
    {
        if (m_db->m_config.L1_cache_item_limit <= 0 || m_db->m_config.L1_cache_memory_limit <= 0)
        {
            return ERR_INVALID_OPERATION;
        }
        if (IsInCache(dbid, key))
        {
            return 0;
        }
        int type = m_db->Type(dbid, key);
        if (type != ZSET_META)
        {
            //only zset supported now
            return ERR_INVALID_TYPE;
        }
        CacheInstruction inst;
        inst.db = dbid;
        inst.key.assign(key.data(), key.size());
        inst.type = kCacheLoadType;
        PushInst(inst);
        return 0;
    }

    int L1Cache::SyncLoad(const DBID& dbid, const Slice& key)
    {
        int ret = Load(dbid, key);
        if(0 != ret)
        {
            return ret;
        }
        while(!IsCached(dbid, key))
        {
            Thread::Sleep(10, MILLIS);
        }
        return 0;
    }

    int L1Cache::PeekCacheStatus(const DBID& dbid, const Slice& key, uint8& status)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Peek(cache_key, item);
        if (NULL == item)
        {
            return -1;
        }
        status = item->GetStatus();
        return 0;
    }

    bool L1Cache::IsCached(const DBID& dbid, const Slice& key)
    {
        uint8 status = 0;
        return 0 == PeekCacheStatus(dbid, key, status) && status == kCacheLoaded;
    }
    CacheItem* L1Cache::Get(const DBID& dbid, const Slice& key, KeyType type)
    {
        DBItemKey cache_key(dbid, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Get(cache_key, item);
        if (NULL != item && item->GetType() == type && item->GetStatus() == kCacheLoaded)
        {
            item->IncRef();
            return item;
        }
        return NULL;
    }
    void L1Cache::Recycle(CacheItem* item)
    {
        if (NULL != item)
        {
            item->DecRef();
        }
    }
}


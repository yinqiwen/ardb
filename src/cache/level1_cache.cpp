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
        m_cache.SetMaxCacheSize(UINT_MAX);
    }

    L1Cache::~L1Cache()
    {
    }

    void L1Cache::Run()
    {
        CheckMemory(NULL);
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
            if (instruction.type == kCacheLoadType)
            {
                DoLoad(instruction.db, instruction.key, instruction.key_type);
            }
            else if (instruction.type == kCacheEvictType)
            {
                DoEvict(instruction.db, instruction.key);
            }
            else
            {
                WARN_LOG("Invalid instruction type:%u", instruction.type);
            }
        }
    }

    void L1Cache::CheckMemory(CacheItem* exclude_item)
    {
        while (m_estimate_mem_size > m_db->m_config.L1_cache_memory_limit && m_cache.Size() > 1)
        {
            LRUCache<DBItemKey, CacheItem*>::CacheEntry entry;
            CacheLockGuard guard(m_cache_mutex);
            if (m_cache.PeekFront(entry) && entry.second != exclude_item)
            {
                m_estimate_mem_size -= SizeOfDBItemKey(entry.first);
                m_estimate_mem_size -= entry.second->GetEstimateMemorySize();
                m_estimate_mem_size -= L1CacheTable::AverageBytesPerValue();
                entry.second->DecRef();
                m_cache.PopFront();
            }
        }
    }

    static int ZSetStoreCallback(const ValueData& value, int cursor, void* cb)
    {
        ZSetCacheLoadContext* ctx = (ZSetCacheLoadContext*) cb;
        ZSetCache* c = ctx->cache;
        int left = cursor % 3;
        switch (left)
        {
            case 0:
            {
                ctx->value = value;
                break;
            }
            case 1:
            {
                ctx->score = value;
                break;
            }
            case 2:
            {
                ctx->attr = value;
                c->Add(ctx->score, ctx->value, ctx->attr, false);
                ctx->value.Clear();
                ctx->attr.Clear();
                ctx->score.Clear();
                break;
            }
            default:
            {
                break;
            }
        }
        return 0;
    }

    void L1Cache::LoadZSetCache(const DBItemKey& key, ZSetCache* item)
    {
        ZSetQueryOptions z_options;
        z_options.withscores = true;
        z_options.withattr = true;
        Iterator* iter = NULL;
        ZRangeSpec range;
        range.contain_min = false;
        range.contain_max = false;
        range.min.SetDoubleValue(-DBL_MAX);
        range.max.SetDoubleValue(DBL_MAX);
        ZSetCacheLoadContext ctx;
        ctx.cache = item;
        m_db->ZRangeByScoreRange(key.db, key.key, range, iter, z_options, false, ZSetStoreCallback, &ctx);
        DELETE(iter);
        //WARN_LOG("zset bytes used:%u %u", item->m_cache.bytes_used(), item->m_cache_score_dict.bytes_used());
    }

    CacheItem* L1Cache::DoCreateCacheEntry(const DBID& dbid, const Slice& key, KeyType type)
    {
        DBItemKey cache_key(dbid, key);
        if (m_cache.Contains(cache_key))
        {
            return NULL;
        }
        CacheItem* item = NULL;
        switch (type)
        {
            case ZSET_META:
            {
                NEW(item, ZSetCache);
                break;
            }
            default:
            {
                return NULL;
            }
        }
        item->SetStatus(L1_CACHE_LOADED);
        item->IncRef();
        item->m_total_mem_size_ref = &m_estimate_mem_size;
        LRUCache<DBItemKey, CacheItem*>::CacheEntry entry;
        //insert never pop items
        m_cache.Insert(cache_key, item, entry);
        m_estimate_mem_size += item->GetEstimateMemorySize();
        m_estimate_mem_size += SizeOfDBItemKey(cache_key);
        m_estimate_mem_size += L1CacheTable::AverageBytesPerValue();
        return item;
    }

    CacheItem* L1Cache::CreateCacheEntry(const DBID& dbid, const Slice& key, KeyType type)
    {
        CacheLockGuard guard(m_cache_mutex);
        return DoCreateCacheEntry(dbid, key, type);

    }
    void L1Cache::PushInst(const CacheInstruction& inst)
    {
        m_inst_queue.Push(inst);
        m_signal_notifier->FireSoftSignal(kCacheSignal, 0);
    }
    int L1Cache::DoEvict(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        CacheLockGuard guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Erase(cache_key, item);
        if (NULL != item)
        {
            m_estimate_mem_size -= SizeOfDBItemKey(cache_key);
            m_estimate_mem_size -= L1CacheTable::AverageBytesPerValue();
            item->SetStatus(L1_CACHE_INVALID);
            item->DecRef();
            return 0;
        }
        return -1;
    }

    int L1Cache::DoLoad(const DBID& dbid, const Slice& key, KeyType key_type)
    {
        if (key_type != ZSET_META)
        {
            return -1;
        }
        CacheItem* item = NULL;
        DBItemKey cache_key(dbid, key);
        {
            CacheLockGuard guard(m_cache_mutex);
            m_cache.Peek(cache_key, item);
            if (NULL == item || L1_CACHE_LOADING != item->GetStatus())
            {
                return -1;
            }
            item->IncRef();
        }
        uint64 start = get_current_epoch_millis();
        LRUCache<DBItemKey, CacheItem*>::CacheEntry entry;
        switch (key_type)
        {
            case ZSET_META:
            {
                LoadZSetCache(cache_key, (ZSetCache*) item);
                break;
            }
            default:
            {
                return -1;
            }
        }

        uint64 end = get_current_epoch_millis();
        INFO_LOG("Cost %llums to load cache %u:%s", (end - start), cache_key.db, cache_key.key.c_str());
        if (item->GetStatus() == L1_CACHE_LOADING)
        {
            item->SetStatus(L1_CACHE_LOADED);
            CheckMemory(item);
        }
        item->DecRef();
        return 0;
    }

    bool L1Cache::IsInCache(const DBID& dbid, const Slice& key)
    {
        DBItemKey cache_key(dbid, key);
        CacheLockGuard guard(m_cache_mutex);
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
        if (m_db->m_config.L1_cache_memory_limit <= 0)
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
        CacheItem* item = CreateCacheEntry(dbid, key, (KeyType) type);
        if (NULL != item)
        {
            item->SetStatus(L1_CACHE_LOADING);
            item->DecRef();
        }
        else
        {
            return ERR_INVALID_TYPE;
        }
        /*
         * Just create an inst
         */
        CacheInstruction inst;
        inst.db = dbid;
        inst.key.assign(key.data(), key.size());
        inst.type = kCacheLoadType;
        inst.key_type = (KeyType) type;
        PushInst(inst);
        return 0;
    }

    int L1Cache::SyncLoad(const DBID& dbid, const Slice& key)
    {
        int ret = Load(dbid, key);
        if (0 != ret)
        {
            return ret;
        }
        while (!IsCached(dbid, key))
        {
            Thread::Sleep(10, MILLIS);
        }
        return 0;
    }

    int L1Cache::PeekCacheStatus(const DBID& dbid, const Slice& key, uint8& status)
    {
        DBItemKey cache_key(dbid, key);
        CacheLockGuard guard(m_cache_mutex);
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
        return 0 == PeekCacheStatus(dbid, key, status) && status == L1_CACHE_LOADED;
    }
    CacheItem* L1Cache::Get(const DBID& dbid, const Slice& key, KeyType type, bool createIfNoExist)
    {
        DBItemKey cache_key(dbid, key);
        CacheLockGuard guard(m_cache_mutex);
        CacheItem* item = NULL;
        m_cache.Get(cache_key, item);
        if (NULL == item && createIfNoExist)
        {
            item = DoCreateCacheEntry(dbid, key, type);
            item->DecRef();
        }
        if (NULL != item && item->GetType() == type)
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


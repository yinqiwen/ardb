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
#include "cache.hpp"
#include "thread/lock_guard.hpp"
#include "ardb.hpp"
#include "geo/geohash_helper.hpp"
#include <algorithm>

#define CACHE_STATE_INIT   1
#define CACHE_STATE_LOADING   2
#define CACHE_STATE_META_LOADED    3
#define CACHE_STATE_FULL_LOADED    4
#define CACHE_STATE_EVICTING  5
#define CACHE_STATE_DIRTY  7
#define CACHE_STATE_DESTROYING  8

OP_NAMESPACE_BEGIN

    bool CacheData::IsMetaLoaded()
    {
        return state == CACHE_STATE_META_LOADED || state == CACHE_STATE_FULL_LOADED || state == CACHE_STATE_LOADING;
    }
    void CacheData::Clear()
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        meta.clear();
        DoClear();
        state = CACHE_STATE_INIT;

    }
    uint32 CacheData::AddRef()
    {
        return atomic_add_uint32(&ref, 1);
    }
    uint32 CacheData::DecRef()
    {
        uint32 ret = atomic_sub_uint32(&ref, 1);
        if (0 == ret)
        {
            delete this;
        }
        return ret;
    }

    void ZSetData::GetLocation(Location& location)
    {
        if (NULL != loc)
        {
            location = *loc;
            return;
        }
        GeoHashHelper::GetMercatorXYByHash(score.value.iv, location.x, location.y);
    }

//    static bool zsetdata_point_less(const ZSetData* v1, const ZSetData* v2)
//    {
//        return (*v1) < (*v2);
//    }

    ZSetDataSet::iterator ZSetDataCache::LowerBound(const Data& score)
    {
        ZSetData entry;
        entry.score = score;
        return data.lower_bound(&entry);
    }

    void ZSetDataCache::Add(const Data& element, const Data& score, bool decode_loc)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        ZSetDataMap::iterator fit = scores.find(&element);
        int data_erased = 0;
        int score_erased = 0;
        if (fit != scores.end())
        {
            ZSetData* entry = fit->second;
            data_erased = data.erase(entry);
            scores.erase(fit);
            score_erased = 1;
            DELETE(entry);
        }
        if (data.size() != scores.size())
        {
            ERROR_LOG("Data size:%u, while score size:%u for erase counter %d:%d", data.size(), scores.size(),data_erased,score_erased);
            abort();
        }
        ZSetData* entry = new ZSetData;
        entry->value = element;
        entry->score = score;
        bool data_intersetd = data.insert(entry).second;
        std::pair<ZSetDataMap::iterator, bool> score_intert = scores.insert(
                ZSetDataMap::value_type(&(entry->value), entry));
        ZSetDataMap::iterator sit = score_intert.first;
        bool score_intersetd = data.insert(entry).second;
        if (decode_loc)
        {
            entry->loc = new Location;
            GeoHashHelper::GetMercatorXYByHash(score.value.iv, entry->loc->x, entry->loc->y);
        }
        if (data.size() != scores.size())
        {
            ERROR_LOG("Data size:%u, while score size:%u with insert flag %d:%d", data.size(), scores.size(), data_intersetd, score_intersetd);
            abort();
        }
    }

    void ZSetDataCache::Del(const Data& element)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        ZSetDataMap::iterator fit = scores.find(&element);
        if (fit != scores.end())
        {
            ZSetData* entry = fit->second;
            data.erase(entry);
            scores.erase(fit);
            DELETE(entry);
        }
    }

    void ZSetDataCache::DoClear()
    {
        ZSetDataSet::iterator it = data.begin();
        while (it != data.end())
        {
            delete *it;
            it++;
        }
        scores.clear();
        data.clear();
    }

    void SetDataCache::Add(const Data& element)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data.insert(element);
    }

    void SetDataCache::Del(const Data& element)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data.erase(element);
    }

    void HashDataCache::Add(const Data& field, const Data& value)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data[field] = value;
    }
    void HashDataCache::Del(const Data& field)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data.erase(field);
    }

    void ListDataCache::Add(const Data& index, const Data& value)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data[index] = value;
    }
    void ListDataCache::Del(const Data& index)
    {
        WriteLockGuard<SpinRWLock> guard(lock);
        data.erase(index);
    }

    L1Cache::L1Cache(const ArdbConfig& cfg) :
            m_cfg(cfg), m_estimate_memory_size(0)
    {
    }

    int L1Cache::Init()
    {
        m_zset_cache.cache.SetMaxCacheSize(m_cfg.L1_zset_max_cache_size);
        m_set_cache.cache.SetMaxCacheSize(m_cfg.L1_set_max_cache_size);
        m_hash_cache.cache.SetMaxCacheSize(m_cfg.L1_hash_max_cache_size);
        m_list_cache.cache.SetMaxCacheSize(m_cfg.L1_list_max_cache_size);
        m_string_cache.cache.SetMaxCacheSize(m_cfg.L1_string_max_cache_size);
        return 0;
    }

    void L1Cache::Run()
    {
        struct ClearStatTask: public Runnable
        {
                L1Cache& cache;
                ClearStatTask(L1Cache& c) :
                        cache(c)
                {
                }
                void Run()
                {
                    cache.m_string_cache.stat.Clear();
                    cache.m_hash_cache.stat.Clear();
                    cache.m_list_cache.stat.Clear();
                    cache.m_zset_cache.stat.Clear();
                    cache.m_set_cache.stat.Clear();
                }
        };
        m_serv.GetTimer().ScheduleHeapTask(new ClearStatTask(*this), 10, 300, SECONDS);
        m_serv.Start();
    }

    bool L1Cache::IsCacheEnable(uint8 type)
    {
        switch (type)
        {
            case KEY_META:
            case KEY_END:
            {
                return false;
            }
            case STRING_META:
            {
                return m_cfg.L1_string_max_cache_size > 0;
            }
            case HASH_META:
            case HASH_FIELD:
            {
                return m_cfg.L1_hash_max_cache_size > 0;
            }
            case LIST_META:
            case LIST_ELEMENT:
            {
                return m_cfg.L1_list_max_cache_size > 0;
            }
            case SET_META:
            case SET_ELEMENT:
            {
                return m_cfg.L1_set_max_cache_size > 0;
            }
            case ZSET_META:
            case ZSET_ELEMENT_SCORE:
            case ZSET_ELEMENT_VALUE:
            {
                return m_cfg.L1_zset_max_cache_size > 0;
            }
            default:
            {
                return false;
            }
        }
        return false;
    }

    CommonLRUCache& L1Cache::GetCacheByType(uint8 type)
    {
        switch (type)
        {
            case STRING_META:
            {
                return m_string_cache;
            }
            case HASH_META:
            case HASH_FIELD:
            {
                return m_hash_cache;
            }
            case LIST_META:
            case LIST_ELEMENT:
            {
                return m_list_cache;
            }
            case SET_META:
            case SET_ELEMENT:
            {
                return m_set_cache;
            }
            case ZSET_META:
            case ZSET_ELEMENT_SCORE:
            case ZSET_ELEMENT_VALUE:
            {
                return m_zset_cache;
            }
            default:
            {
                abort();
            }
        }

    }

    static void EvictCache(Channel* ch, void* data)
    {
        CacheData* cache = (CacheData*) data;
        if (NULL != cache)
        {
            cache->Clear();
            cache->DecRef();
        }
    }

    static void DestroyCache(Channel* ch, void* data)
    {
        CacheData* cache = (CacheData*) data;
        if (NULL != cache)
        {
            cache->DecRef();
        }
    }

    struct LoadCacheContext
    {
            CacheLoadOptions options;
            DBItemKey key;
            uint8 type;
            CacheData* cache;
            L1Cache* cache_serv;
            LoadCacheContext() :
                    type(0), cache(NULL), cache_serv(NULL)
            {
            }
    };

    void L1Cache::LoadCache(Channel* ch, void* data)
    {
        LoadCacheContext* ctx = (LoadCacheContext*) data;
        L1Cache* serv = (L1Cache*) ctx->cache_serv;
        Context tmpctx;
        tmpctx.currentDB = ctx->key.db;
        if (ctx->cache->state != CACHE_STATE_LOADING)
        {
            serv->Evict(ctx->key.db, ctx->key.key);
            DELETE(ctx);
            return;
        }
        ValueObject meta;
        meta.key.db = ctx->key.db;
        meta.key.type = ctx->type;
        meta.key.key = ctx->key.key;
        decode_value(ctx->cache->meta, meta);

        bool interrupt = false;
        switch (ctx->type)
        {
            case ZSET_META:
            {
                ZSetIterator iter;
                Data from;
                ZSetDataCache* cache = (ZSetDataCache*) ctx->cache;
                g_db->ZSetScoreIter(tmpctx, meta, from, iter, false);
                uint64 s1 = get_current_epoch_millis();
                while (iter.Valid())
                {
                    if (ctx->cache->state != CACHE_STATE_LOADING)
                    {
                        WARN_LOG("Loading key interrupt");
                        //intterupt loading
                        interrupt = true;
                        break;
                    }
                    cache->Add(*(iter.Element()), *(iter.Score()), ctx->options.deocode_geo);
                    iter.Next();
                }
                uint64 s2 = get_current_epoch_millis();
                DEBUG_LOG("Cost %llums to load cache.", s2 - s1);
                break;
            }
            case SET_META:
            {
                SetIterator iter;
                Data from;
                SetDataCache* cache = (SetDataCache*) ctx->cache;
                g_db->SetIter(tmpctx, meta, from, iter, false);
                while (iter.Valid())
                {
                    if (ctx->cache->state != CACHE_STATE_LOADING)
                    {
                        WARN_LOG("Loading key interrupt");
                        //intterupt loading
                        interrupt = true;
                        break;
                    }
                    cache->Add(*(iter.Element()));
                    iter.Next();
                }
                break;
            }
            case HASH_META:
            {
                HashIterator iter;
                ValueObject meta;
                HashDataCache* cache = (HashDataCache*) ctx->cache;
                g_db->HashIter(tmpctx, meta, "", iter, false);

                while (iter.Valid())
                {
                    if (ctx->cache->state != CACHE_STATE_LOADING)
                    {
                        WARN_LOG("Loading key interrupt");
                        //intterupt loading
                        interrupt = true;
                        break;
                    }
                    cache->Add(*(iter.Field()), *(iter.Value()));
                    iter.Next();
                }
                break;
            }
            case LIST_META:
            {
                ListIterator iter;
                ValueObject meta;
                ListDataCache* cache = (ListDataCache*) ctx->cache;
                g_db->ListIter(tmpctx, meta, iter, false);

                while (iter.Valid())
                {
                    if (ctx->cache->state != CACHE_STATE_LOADING)
                    {
                        WARN_LOG("Loading key interrupt");
                        //intterupt loading
                        interrupt = true;
                        break;
                    }
                    cache->Add(*(iter.Score()), *(iter.Element()));
                    iter.Next();
                }
                break;
            }
            default:
            {
                break;
            }
        }
        if (interrupt)
        {
            ctx->cache->DecRef();
            serv->Evict(ctx->key.db, ctx->key.key);
            //serv->Evict(ctx->key.db, ctx->key.key);
        }
        else
        {
            ctx->cache->state = CACHE_STATE_FULL_LOADED;
            ctx->cache->DecRef();
        }
        DELETE(ctx);
    }

    CacheResult L1Cache::GetCacheData(uint8 type, DBItemKey& key, const CacheGetOptions& options)
    {
        CacheData* item = NULL;
        CommonLRUCache& cache = GetCacheByType(type);
        CacheResult result(cache);
        {
            LockGuard<SpinMutexLock> guard(cache.lock);
            if (options.delete_entry)
            {
                cache.cache.Erase(key, item);
            }
            else
            {
                if (options.peek)
                {
                    cache.cache.Peek(key, item);
                }
                else
                {
                    cache.cache.Get(key, item);
                }
            }
        }

        if (NULL != item)
        {
            if (!options.delete_entry)
            {
                if (options.dirty_if_loading && item->state == CACHE_STATE_LOADING)
                {
                    item->state = CACHE_STATE_DIRTY;
                    return result;
                }
                if (!options.expected_states.empty())
                {
                    if (options.expected_states.count(item->state) == 0)
                    {
                        return result;
                    }
                }
                item->AddRef();
            }
        }
        else
        {
            if (options.create_if_notexist && !options.delete_entry)
            {
                switch (type)
                {
                    case STRING_META:
                    {
                        item = new CacheData;
                        break;
                    }
                    case HASH_META:
                    case HASH_FIELD:
                    {
                        item = new HashDataCache;
                        break;
                    }
                    case LIST_META:
                    case LIST_ELEMENT:
                    {
                        item = new ListDataCache;
                        break;
                    }
                    case SET_META:
                    case SET_ELEMENT:
                    {
                        item = new SetDataCache;
                        break;
                    }
                    case ZSET_META:
                    case ZSET_ELEMENT_SCORE:
                    case ZSET_ELEMENT_VALUE:
                    {
                        item = new ZSetDataCache;
                        break;
                    }
                    default:
                    {
                        abort();
                        break;
                    }
                }
                LockGuard<SpinMutexLock> guard(cache.lock);
                CacheTableEntry erased;
                if (cache.cache.Insert(key, item, erased))
                {
                    erased.second->state = CACHE_STATE_DESTROYING;
                    m_serv.AsyncIO(0, DestroyCache, erased.second);
                }
                item->AddRef();
            }
        }
        result.data = item;
        return result;
    }

    int L1Cache::Get(KeyObject& key, ValueObject& v)
    {
        if (!IsCacheEnable(v.type))
        {
            return ERR_NOT_EXIST_IN_CACHE;
        }
        int err = 0;
        DBItemKey kk;
        kk.db = key.db;
        kk.key.assign(key.key.data(), key.key.size());
        CacheGetOptions options;
        options.expected_states.insert(CACHE_STATE_FULL_LOADED);
        switch (key.type)
        {
            case KEY_META:
            {
                options.expected_states.insert(CACHE_STATE_META_LOADED);
                options.expected_states.insert(CACHE_STATE_LOADING);
                CacheResult result = GetCacheData(v.type, kk, options);
                result.cache.stat.IncQueryStat();
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return ERR_NOT_EXIST_IN_CACHE;
                }
                result.cache.stat.IncHitStat();
                ReadLockGuard<SpinRWLock> guard(meta->lock);
                decode_value(meta->meta, v);
                meta->DecRef();
                err = 0;
                break;
            }
            case SET_ELEMENT:
            {
                CacheResult result = GetCacheData(SET_ELEMENT, kk, options);
                result.cache.stat.IncQueryStat();
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return ERR_NOT_EXIST_IN_CACHE;
                }
                SetDataCache* sc = (SetDataCache*) meta;
                ReadLockGuard<SpinRWLock> guard(meta->lock);
                err = sc->data.count(key.element);
                meta->DecRef();
                err = err > 0 ? 0 : ERR_NOT_EXIST;
                if (err == 0)
                {
                    result.cache.stat.IncHitStat();
                }
                break;
            }
            case ZSET_ELEMENT_SCORE:
            case ZSET_ELEMENT_VALUE:
            {
                CacheResult result = GetCacheData(ZSET_ELEMENT_VALUE, kk, options);
                result.cache.stat.IncQueryStat();
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return ERR_NOT_EXIST_IN_CACHE;
                }

                ZSetDataCache* sc = (ZSetDataCache*) meta;
                ReadLockGuard<SpinRWLock> guard(meta->lock);
                ZSetDataMap::iterator fit = sc->scores.find(&(key.element));
                if (fit == sc->scores.end())
                {
                    err = ERR_NOT_EXIST;
                }
                else
                {
                    if (key.type == ZSET_ELEMENT_SCORE)
                    {
                        err = 0;
                    }
                    else
                    {
                        v.score = (fit->second)->score;
                        if (v.attach.fetch_loc)
                        {
                            fit->second->GetLocation(v.attach.loc);
                        }
                        err = 0;
                    }
                }
                if (err == 0)
                {
                    result.cache.stat.IncHitStat();
                }
                meta->DecRef();
                break;
            }
            case LIST_ELEMENT:
            {
                CacheResult result = GetCacheData(LIST_ELEMENT, kk, options);
                result.cache.stat.IncQueryStat();
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return ERR_NOT_EXIST_IN_CACHE;
                }

                ListDataCache* sc = (ListDataCache*) meta;
                ReadLockGuard<SpinRWLock> guard(meta->lock);
                DataMap::iterator fit = sc->data.find(key.score);
                if (fit == sc->data.end())
                {
                    err = ERR_NOT_EXIST;
                }
                else
                {
                    result.cache.stat.IncHitStat();
                    v.element = fit->second;
                    err = 0;
                }
                meta->DecRef();
                return err;
            }
            case HASH_FIELD:
            {
                CacheResult result = GetCacheData(LIST_ELEMENT, kk, options);
                result.cache.stat.IncQueryStat();
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return ERR_NOT_EXIST_IN_CACHE;
                }
                HashDataCache* sc = (HashDataCache*) meta;
                ReadLockGuard<SpinRWLock> guard(meta->lock);
                DataMap::iterator fit = sc->data.find(key.score);
                if (fit == sc->data.end())
                {
                    err = ERR_NOT_EXIST;
                }
                else
                {
                    result.cache.stat.IncHitStat();
                    v.element = fit->second;
                    err = 0;
                }
                meta->DecRef();
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
        return err;
    }

    bool L1Cache::ShouldCreateCacheOrNot(uint8 type, KeyObject& key, bool read_result)
    {
        switch (type)
        {
            case STRING_META:
            {
                return read_result ? m_cfg.L1_string_read_fill_cache : false;
            }
            case HASH_META:
            {
                return read_result ? m_cfg.L1_hash_read_fill_cache : false;
            }
            case LIST_META:
            {
                return read_result ? m_cfg.L1_list_read_fill_cache : false;
            }
            case SET_META:
            {
                return read_result ? m_cfg.L1_set_read_fill_cache : false;
            }
            case ZSET_META:
            {
                return read_result ? m_cfg.L1_zset_read_fill_cache : false;
            }
            default:
            {
                abort();
            }
        }
        return false;
    }
    int L1Cache::Put(KeyObject& key, ValueObject& v, const CacheSetOptions& set_opt)
    {
        if (key.type == ZSET_ELEMENT_VALUE)
        {
            return 0;
        }
        if (!IsCacheEnable(v.type))
        {
            return -1;
        }
        int err = 0;
        DBItemKey kk;
        kk.db = key.db;
        kk.key.assign(key.key.data(), key.key.size());
        CacheGetOptions options;
        options.dirty_if_loading = !set_opt.from_read_result;
        options.expected_states.insert(CACHE_STATE_FULL_LOADED);
        switch (key.type)
        {
            case KEY_META:
            {
                options.create_if_notexist = ShouldCreateCacheOrNot(v.type, key, set_opt.from_read_result);
                options.expected_states.insert(CACHE_STATE_META_LOADED);
                options.expected_states.insert(CACHE_STATE_LOADING);
                options.expected_states.insert(CACHE_STATE_INIT);
                CacheResult result = GetCacheData(v.type, kk, options);
                CacheData* meta = result.data;
                if (NULL != meta)
                {
                    if (!v.encode_buf.Readable())
                    {
                        v.Encode();
                    }
                    WriteLockGuard<SpinRWLock> guard(meta->lock);
                    meta->meta.clear();
                    meta->meta.assign(v.encode_buf.GetRawReadBuffer(), v.encode_buf.ReadableBytes());
                    if (meta->state == CACHE_STATE_INIT)
                    {
                        meta->state = CACHE_STATE_META_LOADED;
                    }
                    if (v.type == STRING_META
                            || (meta->state == CACHE_STATE_META_LOADED && v.meta.Encoding() > COLLECTION_ENCODING_RAW))
                    {
                        meta->state = CACHE_STATE_FULL_LOADED;
                    }
                    meta->DecRef();
                    return 0;
                }
                break;
            }
            case SET_ELEMENT:
            {
                CacheResult result = GetCacheData(SET_ELEMENT, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                SetDataCache* sc = (SetDataCache*) meta;
                sc->Add(key.element);
                meta->DecRef();
                break;
            }
            case ZSET_ELEMENT_SCORE:
            {
                CacheResult result = GetCacheData(ZSET_ELEMENT_SCORE, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                ZSetDataCache* sc = (ZSetDataCache*) meta;
                sc->Add(key.element, key.score, set_opt.cmd == REDIS_CMD_GEO_ADD);
                meta->DecRef();
                break;
            }
            case LIST_ELEMENT:
            {
                CacheResult result = GetCacheData(LIST_ELEMENT, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                ListDataCache* sc = (ListDataCache*) meta;
                sc->Add(key.score, v.element);
                meta->DecRef();
                return err;
            }
            case HASH_FIELD:
            {
                CacheResult result = GetCacheData(HASH_FIELD, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }

                HashDataCache* sc = (HashDataCache*) meta;
                sc->Add(key.element, v.element);
                meta->DecRef();
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
        return err;
    }
    int L1Cache::Del(KeyObject& key, uint8 type)
    {
        if (key.type == KEY_META)
        {
            type = key.meta_type;
        }
        if (!IsCacheEnable(type))
        {
            return -1;
        }
        int err = 0;
        DBItemKey kk;
        kk.db = key.db;
        kk.key.assign(key.key.data(), key.key.size());
        CacheGetOptions options;
        options.dirty_if_loading = true;
        switch (key.type)
        {
            case KEY_META:
            {
                options.peek = true;
                CacheResult result = GetCacheData(type, kk, options);
                CacheData* meta = result.data;
                if (NULL != meta)
                {
                    meta->Clear();
                    return 0;
                }
                break;
            }
            case SET_ELEMENT:
            {
                options.delete_entry = false;
                options.expected_states.insert( CACHE_STATE_FULL_LOADED);
                CacheResult result = GetCacheData(SET_ELEMENT, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                SetDataCache* sc = (SetDataCache*) meta;
                sc->Del(key.element);
                meta->DecRef();
                break;
            }
            case ZSET_ELEMENT_VALUE:
            {
                break;
            }
            case ZSET_ELEMENT_SCORE:
            {
                options.delete_entry = false;
                options.expected_states.insert( CACHE_STATE_FULL_LOADED);
                CacheResult result = GetCacheData(ZSET_ELEMENT_SCORE, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                ZSetDataCache* sc = (ZSetDataCache*) meta;
                sc->Del(key.element);
                meta->DecRef();
                break;
            }
            case LIST_ELEMENT:
            {
                options.delete_entry = false;
                options.expected_states.insert( CACHE_STATE_FULL_LOADED);
                CacheResult result = GetCacheData(LIST_ELEMENT, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }
                ListDataCache* sc = (ListDataCache*) meta;
                sc->Del(key.score);
                meta->DecRef();
                return err;
            }
            case HASH_FIELD:
            {
                options.delete_entry = false;
                options.expected_states.insert(CACHE_STATE_FULL_LOADED);
                CacheResult result = GetCacheData(HASH_FIELD, kk, options);
                CacheData* meta = result.data;
                if (NULL == meta)
                {
                    return -1;
                }

                HashDataCache* sc = (HashDataCache*) meta;
                sc->Del(key.element);
                meta->DecRef();
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
        return err;
    }
    int L1Cache::Load(DBID db, const std::string& key, uint8 type, const CacheLoadOptions& load_opt)
    {
        if (!IsCacheEnable(type))
        {
            return -1;
        }
        DBItemKey kk;
        kk.db = db;
        kk.key = key;
        CacheGetOptions options;
        options.create_if_notexist = true;
        options.expected_states.insert(CACHE_STATE_INIT);
        options.expected_states.insert(CACHE_STATE_META_LOADED);
        CacheData* item = GetCacheData(type, kk, options).data;
        if (NULL == item)
        {
            return 0;
        }
        if (item->state == CACHE_STATE_INIT)
        {
            ValueObject meta;
            Context tmpctx;
            tmpctx.currentDB = db;
            g_db->GetMetaValue(tmpctx, key, (KeyType) type, meta);
        }

        LoadCacheContext* ctx = new LoadCacheContext;
        item->state = CACHE_STATE_LOADING;

        ctx->cache_serv = this;
        ctx->cache = item;
        ctx->type = type;
        ctx->key = kk;
        ctx->options = load_opt;
        m_serv.AsyncIO(0, LoadCache, ctx);
        return 0;
    }
    int L1Cache::Evict(DBID db, const std::string& key)
    {
        Context tmpctx;
        tmpctx.currentDB = db;
        ValueObject meta;
        meta.key.type = KEY_META;
        meta.key.db = db;
        meta.key.key = key;
        KeyType type;
        if (0 != g_db->GetKeyValue(tmpctx, meta.key, &meta))
        {
            return -1;
        }
        type = (KeyType) meta.type;
        DBItemKey kk;
        kk.db = db;
        kk.key = key;
        CacheGetOptions options;
        options.peek = true;
        CacheData* item = GetCacheData(type, kk, options).data;
        if (NULL == item)
        {
            return -1;
        }
        else
        {
            item->state = CACHE_STATE_EVICTING;
            //use another thread to delete cache entry
            m_serv.AsyncIO(0, EvictCache, item);
        }
        return 0;
    }

    void L1Cache::ClearLRUCache(CommonLRUCache& cache, DBID dbid, bool withdb_limit)
    {
        LockGuard<SpinMutexLock> guard(cache.lock);
        CacheTable::CacheList& cachelist = cache.cache.GetCacheList();
        CacheTable::CacheList::iterator it = cachelist.begin();
        while (it != cachelist.end())
        {
            if (NULL != it->second)
            {
                if (!withdb_limit || (withdb_limit && it->first.db == dbid))
                {
                    it->second->Clear();
                }
            }
            it++;
        }
    }

    int L1Cache::EvictDB(DBID db)
    {
        ClearLRUCache(m_string_cache, db, true);
        ClearLRUCache(m_hash_cache, db, true);
        ClearLRUCache(m_set_cache, db, true);
        ClearLRUCache(m_list_cache, db, true);
        ClearLRUCache(m_zset_cache, db, true);
        return 0;
    }
    int L1Cache::EvictAll()
    {
        ClearLRUCache(m_string_cache, 0, false);
        ClearLRUCache(m_hash_cache, 0, false);
        ClearLRUCache(m_set_cache, 0, false);
        ClearLRUCache(m_list_cache, 0, false);
        ClearLRUCache(m_zset_cache, 0, false);
        return 0;
    }

    std::string L1Cache::Status(DBID db, const std::string& key)
    {
        DBItemKey kk;
        kk.db = db;
        kk.key = key;
        CacheGetOptions options;
        options.peek = true;
        CacheData* item = GetCacheData(STRING_META, kk, options).data;
        if (NULL == item)
        {
            item = GetCacheData(HASH_META, kk, options).data;
        }
        if (NULL == item)
        {
            item = GetCacheData(SET_META, kk, options).data;
        }
        if (NULL == item)
        {
            item = GetCacheData(LIST_META, kk, options).data;
        }
        if (NULL == item)
        {
            item = GetCacheData(ZSET_META, kk, options).data;
        }
        if (NULL == item)
        {
            return "Not loaded";
        }
        std::string status;
        switch (item->state)
        {
            case CACHE_STATE_LOADING:
            {
                status = "Loading";
                break;
            }
            case CACHE_STATE_META_LOADED:
            {
                status = "Meta Loaded";
                break;
            }
            case CACHE_STATE_FULL_LOADED:
            {
                status = "Full Loaded";
                break;
            }
            case CACHE_STATE_EVICTING:
            {
                status = "Evicting";
                break;
            }
            case CACHE_STATE_DIRTY:
            {
                status = "Dirty";
                break;
            }
            case CACHE_STATE_INIT:
            {
                status = "Init";
                break;
            }
            default:
            {
                abort();
            }
        }
        item->DecRef();
        return status;
    }

    CacheData* L1Cache::GetReadCache(DBID db, const std::string& key, uint8 type)
    {
        CacheData* item = NULL;
        CommonLRUCache& cache = GetCacheByType(type);
        cache.stat.IncQueryStat();
        LockGuard<SpinMutexLock> guard(cache.lock);
        DBItemKey kk(db, key);
        cache.cache.Get(kk, item);
        if (NULL != item)
        {
            if (item->state == CACHE_STATE_FULL_LOADED)
            {
                item->AddRef();
                item->lock.Lock(READ_LOCK);
                cache.stat.IncHitStat();
            }
            else
            {
                item = NULL;
            }
        }
        return item;
    }
    void L1Cache::RecycleReadCache(CacheData* cache)
    {
        if (NULL == cache)
        {
            return;
        }
        cache->lock.Unlock(READ_LOCK);
        cache->DecRef();
    }

    static void PrintCacheStat(CommonLRUCache& cache, std::string& str)
    {
        std::string hit_rate = "0";
        if (cache.stat.query_count > 0)
        {
            fast_dtoa(cache.stat.hit_count * 1.0 / cache.stat.query_count, 10, hit_rate);
        }
        LockGuard<SpinMutexLock> guard(cache.lock);
        str.append("size=").append(stringfromll(cache.cache.Size())).append(" ");
        str.append("max_size=").append(stringfromll(cache.cache.MaxCacheSize())).append(" ");
        str.append("hit_rate=").append(hit_rate);
    }

    const std::string& L1Cache::PrintStat(std::string& str)
    {
        str.clear();
        str.append("string: ");
        PrintCacheStat(m_string_cache, str);
        str.append("\n");
        str.append("hash: ");
        PrintCacheStat(m_hash_cache, str);
        str.append("\n");
        str.append("list: ");
        PrintCacheStat(m_list_cache, str);
        str.append("\n");
        str.append("set: ");
        PrintCacheStat(m_set_cache, str);
        str.append("\n");
        str.append("zset: ");
        PrintCacheStat(m_zset_cache, str);
        str.append("\n");
        return str;
    }

    void L1Cache::StopSelf()
    {
        m_serv.Stop();
        m_serv.Wakeup();
    }

    static void DestroyCache(CommonLRUCache& cache)
    {
        CacheTableEntry entry;
        while (cache.cache.PeekFront(entry))
        {
            entry.second->Clear();
            delete entry.second;
            cache.cache.PopFront();
        }
    }

    L1Cache::~L1Cache()
    {
        DestroyCache(m_string_cache);
        DestroyCache(m_hash_cache);
        DestroyCache(m_list_cache);
        DestroyCache(m_set_cache);
        DestroyCache(m_zset_cache);
    }
OP_NAMESPACE_END


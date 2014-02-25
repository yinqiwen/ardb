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

#ifndef CACHE_SERVICE_HPP_
#define CACHE_SERVICE_HPP_

#include "common.hpp"
#include "data_format.hpp"
#include "util/atomic.hpp"
#include "util/lru.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#include "util/thread/spin_rwlock.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "channel/all_includes.hpp"

#define L1_CACHE_NOT_EXIST  0
#define L1_CACHE_LOADING    1
#define L1_CACHE_LOADED     2
#define L1_CACHE_INVALID    4

#define ZSET_CACHE_NONEW_ELEMENT 0
#define ZSET_CACHE_SCORE_CHANGED 1
#define ZSET_CACHE_ATTR_CHANGED 2
#define ZSET_CACHE_NEW_ELEMENT 3

namespace ardb
{
    struct CacheInstruction
    {
            DBID db;
            std::string key;
            KeyType key_type;
            uint8 type;
    };
    class L1Cache;
    class CacheItem
    {
        protected:
            volatile uint64 m_estimate_mem_size;
            volatile uint64* m_total_mem_size_ref;
            SpinRWLock m_lock;
            typedef ReadLockGuard<SpinRWLock> CacheReadLockGuard;
            typedef WriteLockGuard<SpinRWLock> CacheWriteLockGuard;
//            ThreadMutex m_lock;
//            typedef LockGuard<ThreadMutex> ZCacheReadLockGuard;
//            typedef LockGuard<ThreadMutex> ZCacheWriteLockGuard;
            friend class L1Cache;
            void AddEstimateMemSize(uint32 delta)
            {
                m_estimate_mem_size += delta;
                atomic_add_uint64(m_total_mem_size_ref, delta);
            }
            void SubEstimateMemSize(uint32 delta)
            {
                m_estimate_mem_size -= delta;
                atomic_sub_uint64(m_total_mem_size_ref, delta);
            }
        private:
            volatile uint32 m_ref;
            uint8 m_type;
            uint8 m_status;
        public:
            CacheItem(uint8 type) :
                    m_estimate_mem_size(0), m_total_mem_size_ref(NULL), m_ref(1), m_type(type), m_status(0)
            {
            }
            virtual ~CacheItem()
            {
                atomic_sub_uint64(m_total_mem_size_ref, m_estimate_mem_size);
            }
            uint8 GetType()
            {
                return m_type;
            }
            uint8 GetStatus()
            {
                return m_status;
            }
            bool IsLoaded()
            {
                return m_status == L1_CACHE_LOADED;
            }
            bool ISValid()
            {
                return m_status == L1_CACHE_LOADING || m_status == L1_CACHE_LOADED;
            }
            void SetStatus(uint8 status)
            {
                m_status = status;
            }
            uint32 GetEstimateMemorySize()
            {
                return m_estimate_mem_size;
            }
            void IncRef()
            {
                atomic_add_uint32(&m_ref, 1);
            }
            void DecRef()
            {
                if (atomic_sub_uint32(&m_ref, 1) == 0)
                {
                    delete this;
                }
            }
    };

    struct ZSetCaheElement
    {
            double score;
            std::string value;
            std::string attr;
            ZSetCaheElement(double s = 0, const std::string v = "") :
                    score(s), value(v)
            {
            }
            bool operator<(const ZSetCaheElement& other) const
            {
                if (score < other.score)
                {
                    return true;
                }
                if (score > other.score)
                {
                    return false;
                }
                return value < other.value;
            }
    };
    typedef TreeSet<ZSetCaheElement>::Type ZSetCacheElementSet;
    typedef TreeMap<std::string, double>::Type ZSetCacheScoreMap;

    class ZSetCache: public CacheItem
    {
        private:
            ZSetCacheElementSet m_cache;
            ZSetCacheScoreMap m_cache_score_dict;

            friend class L1Cache;
            void EraseElement(const ZSetCaheElement& e);
        public:
            ZSetCache();
            void GetRange(const ZRangeSpec& range, bool with_scores, bool with_attrs, ValueStoreCallback* cb,
                    void* cbdata);
            /*
             * return 0:no data inserted  1:score changed, no new element  2:new element
             */
            int Add(const ValueData& score, const ValueData& value, const ValueData& attr, bool thread_safe);
            int Add(const ValueData& score, const Slice& value, const Slice& attr, bool thread_safe = true);
            int Rem(ValueData& v);

    };

    struct ZSetCacheLoadContext
    {
            ZSetCache* cache;
            ValueData value;
            ValueData score;
            ValueData attr;
            ZSetCacheLoadContext() :
                    cache(NULL)
            {
            }
    };

    /*
     * Only zset cache supported now
     */
    class Ardb;
    class L1Cache: public Runnable, public SoftSignalHandler
    {
        private:
            ChannelService& m_serv;
            Ardb* m_db;
            volatile uint64_t m_estimate_mem_size;

            SoftSignalChannel* m_signal_notifier;
            MPSCQueue<CacheInstruction> m_inst_queue;
            typedef LRUCache<DBItemKey, CacheItem*> L1CacheTable;
            L1CacheTable m_cache;
            ThreadMutex m_cache_mutex;
            void LoadZSetCache(const DBItemKey& key, ZSetCache* item);
            void Run();
            void CheckInstQueue();
            void OnSoftSignal(uint32 soft_signo, uint32 appendinfo);
            void PushInst(const CacheInstruction& inst);
            int DoLoad(const DBID& dbid, const Slice& key, KeyType key_type);
            int DoEvict(const DBID& dbid, const Slice& key);
            void CheckMemory(CacheItem* exclude_item = NULL);
            bool IsInCache(const DBID& dbid, const Slice& key);
            CacheItem* DoCreateCacheEntry(const DBID& dbid, const Slice& key, KeyType type);
        public:
            L1Cache(Ardb* db);
            int Evict(const DBID& dbid, const Slice& key);
            int Load(const DBID& dbid, const Slice& key);
            int PeekCacheStatus(const DBID& dbid, const Slice& key, uint8& status);
            CacheItem* CreateCacheEntry(const DBID& dbid, const Slice& key, KeyType type);

            /*
             * just for testing, do not invoke in ardb-server
             */
            int SyncLoad(const DBID& dbid, const Slice& key);

            bool IsCached(const DBID& dbid, const Slice& key);
            CacheItem* Get(const DBID& dbid, const Slice& key, KeyType type, bool createIfNoExist);
            void Recycle(CacheItem* item);
            uint64 GetEstimateMemorySize()
            {
                return m_estimate_mem_size;
            }
            uint32 GetEntrySize()
            {
                return m_cache.Size();
            }
            ~L1Cache();
    };
}

#endif /* CACHE_SERVICE_HPP_ */

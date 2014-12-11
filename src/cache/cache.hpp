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
#ifndef CACHE_HPP_
#define CACHE_HPP_

#include "common/common.hpp"
#include "util/lru.hpp"
#include "codec.hpp"
#include "options.hpp"
#include "concurrent.hpp"
#include "config.hpp"
#include "thread/thread.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/spin_mutex_lock.hpp"

OP_NAMESPACE_BEGIN

    struct CacheData
    {
            std::string meta;
            SpinRWLock lock;
            uint32 ref;
            uint8 type;
            uint8 state;
            CacheData() :
                    ref(1), type(0), state(1)
            {
            }
            bool IsMetaLoaded();
            uint32 AddRef();
            uint32 DecRef();
            virtual void DoClear()
            {
            }
            void Clear();
            virtual ~CacheData()
            {
            }
    };
    struct ZSetData
    {
            Data score;
            Data value;

            Location* loc;

            ZSetData() :
                    loc(NULL)
            {
            }
            ~ZSetData()
            {
                DELETE(loc);
            }
            void GetLocation(Location& location);
            bool operator <(const ZSetData& other) const
            {
                int cmp = score.Compare(other.score);
                if (cmp < 0)
                {
                    return true;
                }
                else if (cmp > 0)
                {
                    return false;
                }
                else
                {
                    return value < (other.value);
                }
            }
            bool operator ==(const ZSetData& other) const
            {
                return score == other.score && value == other.value;
            }

    };
    struct DataPointerLess
    {
            bool operator()(const Data* __x, const Data* __y) const
            {
                if (__x == __y)
                {
                    return false;
                }
                return __x->Compare(*__y) < 0;
            }
    };
    struct ZSetDataPointerLess
    {
            bool operator()(const ZSetData* __x, const ZSetData* __y) const
            {
                if (__x == __y)
                {
                    return false;
                }
                return (*__x) < (*__y);
            }
    };
    //typedef TreeSet<ZSetData*, ZSetDataPointerLess>::Type ZSetDataSet;
    typedef TreeSet<ZSetData*, ZSetDataPointerLess>::Type ZSetDataSet;
    //typedef std::deque<ZSetData*> ZSetDataDeque;
    typedef TreeMap<const Data*, ZSetData*, DataPointerLess>::Type ZSetDataMap;
    typedef TreeMap<const Data*, Location, DataPointerLess>::Type GeoDataMap;

    struct ZSetDataCache: public CacheData
    {
            ZSetDataSet data;
            ZSetDataMap scores;

            ZSetDataCache()
            {
            }

            void Add(const Data& element, const Data& score, bool decode_loc);
            void Del(const Data& element);

            ZSetDataSet::iterator LowerBound(const Data& score);
            void DoClear();
            ~ZSetDataCache()
            {
            }
    };

    struct ListDataCache: public CacheData
    {
            DataMap data;
            void Add(const Data& index, const Data& value);
            void Del(const Data& index);
            void DoClear()
            {
                data.clear();
            }
    };

    struct SetDataCache: public CacheData
    {
            DataSet data;
            void Add(const Data& element);
            void Del(const Data& element);
            void DoClear()
            {
                data.clear();
            }
    };

    struct HashDataCache: public CacheData
    {
            DataMap data;
            void Add(const Data& field, const Data& value);
            void Del(const Data& field);
            void DoClear()
            {
                data.clear();
            }
    };

    struct CacheStatistics
    {
            volatile uint64 hit_count;
            volatile uint64 query_count;
            CacheStatistics() :
                    hit_count(0), query_count(0)
            {
            }
            void IncQueryStat()
            {
                atomic_add_uint64(&query_count, 1);
            }
            void IncHitStat()
            {
                atomic_add_uint64(&hit_count, 1);
            }
            void Clear()
            {
                hit_count = 0;
                query_count = 0;
            }
    };
    typedef LRUCache<DBItemKey, CacheData*> CacheTable;
    typedef CacheTable::CacheEntry CacheTableEntry;
    struct CommonLRUCache
    {
            CacheTable cache;
            SpinMutexLock lock;
            CacheStatistics stat;
    };

    typedef TreeSet<uint8>::Type StateSet;
    struct CacheGetOptions
    {
            bool peek;
            bool create_if_notexist;
            bool delete_entry;
            StateSet expected_states;
            bool dirty_if_loading;
            CacheGetOptions() :
                    peek(false), create_if_notexist(false), delete_entry(false), dirty_if_loading(false)
            {
            }
    };

    struct CacheSetOptions
    {
            bool from_read_result;
            RedisCommandType cmd;
            CacheSetOptions() :
                    from_read_result(false), cmd(REDIS_CMD_INVALID)
            {
            }
    };
    struct CacheLoadOptions
    {
            bool deocode_geo;
            CacheLoadOptions() :
                    deocode_geo(false)
            {
            }
    };
    struct CacheResult
    {
            CommonLRUCache& cache;
            CacheData* data;
            CacheResult(CommonLRUCache& c) :
                    cache(c), data(NULL)
            {
            }
    };

    class L1Cache: public Thread
    {
        private:
            ChannelService m_serv;
            const ArdbConfig& m_cfg;
            CommonLRUCache m_string_cache;
            CommonLRUCache m_hash_cache;
            CommonLRUCache m_set_cache;
            CommonLRUCache m_list_cache;
            CommonLRUCache m_zset_cache;

            uint64 m_estimate_memory_size;

            CommonLRUCache& GetCacheByType(uint8 type);
            void ClearLRUCache(CommonLRUCache& cache, DBID dbid, bool withdb_limit);

            CacheResult GetCacheData(uint8 type, DBItemKey& key, const CacheGetOptions& options);

            bool IsCacheEnable(uint8 type);
            bool ShouldCreateCacheOrNot(uint8 type, KeyObject& key, bool read_result);
            void Run();
            static void LoadCache(Channel* ch, void* data);
        public:
            L1Cache(const ArdbConfig& cfg);
            int Init();
            int Get(KeyObject& key, ValueObject& v);
            int Put(KeyObject& key, ValueObject& v, const CacheSetOptions& opt);
            int Del(KeyObject& key, uint8 type);
            int Evict(DBID db, const std::string& key);
            int EvictDB(DBID db);
            int EvictAll();
            std::string Status(DBID db, const std::string& key);
            int Load(DBID db, const std::string& key, uint8 type, const CacheLoadOptions& options);
            CacheData* GetReadCache(DBID db, const std::string& key, uint8 type);
            void RecycleReadCache(CacheData* cache);
            const std::string& PrintStat(std::string& str);
            void StopSelf();
            ~L1Cache();
    };

OP_NAMESPACE_END

#endif /* CACHE_HPP_ */

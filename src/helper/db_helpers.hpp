/*
 * db_helpers.hpp
 *
 *  Created on: 2013-12-10
 *      Author: wqy
 */

#ifndef DB_HELPERS_HPP_
#define DB_HELPERS_HPP_
#include <stdint.h>
#include <float.h>
#include <map>
#include <vector>
#include <list>
#include <string>
#include <deque>
#include "common.hpp"
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "util/lru.hpp"
#include "ardb.hpp"
namespace ardb
{
    class ExpireCheck: public Runnable
    {
        private:
            DBID m_checking_db;
            Ardb* m_db;
            void Run();
        public:
            ExpireCheck(Ardb* db);
    };

//    class ZSetScoresCacheManager: public Runnable
//    {
//        private:
//            Ardb* m_db;
//            LRUCache<DBItemKey, DoubleDeque*> m_cache;
//            typedef LRUCache<DBItemKey, DoubleDeque*>::CacheEntry ScoreCacheEntry;
//            uint32 m_estimate_memory;
//            ThreadMutex m_cache_mutex;
//            DoubleDeque* GetEntry(const DBItemKey& key, ZSetMetaValue& meta, bool create_if_noexist);
//            void EraseOldestScores();
//            void Run();
//        public:
//            ZSetScoresCacheManager(Ardb* db);
//            int Replace(const DBID& db, const Slice& key, ZSetMetaValue& meta, double old_score, double new_score);
//            int Insert(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score);
//            int Remove(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score);
//            int RemoveScores(const DBID& db, const Slice& key, ZSetMetaValue& meta, DoubleDeque& score);
//            int RemoveAll(const DBID& db, const Slice& key);
//            int Rank(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score);
//            int RevRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score);
//            int Range(const DBID& db, const Slice& key, ZSetMetaValue& meta, double min, double max,
//                    DoubleDeque& result);
//            int RangeByRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, uint32 start, uint32 stop,
//                    DoubleDeque& result);
//            int GetScoresByRanks(const DBID& db, const Slice& key, ZSetMetaValue& meta, const UInt32Array& ranks,
//                    DoubleDeque& result);
//            int GetScoresByRevRanks(const DBID& db, const Slice& key, ZSetMetaValue& meta, const UInt32Array& ranks,
//                    DoubleDeque& result);
//            ~ZSetScoresCacheManager();
//    };

    class DBHelper: public Thread
    {
        private:
            ChannelService m_serv;
            Ardb* m_db;
            ExpireCheck m_expire_check;
//            ZSetScoresCacheManager m_zset_scores_cache;
            void Run();
        public:
            DBHelper(Ardb* db);
            void StopSelf();
    };
}

#endif /* DB_HELPERS_HPP_ */

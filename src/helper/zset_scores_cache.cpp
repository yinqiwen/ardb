/*
 * zset_scores_cache.cpp
 *
 *  Created on: 2013Äê12ÔÂ29ÈÕ
 *      Author: wangqiying
 */
#include "db_helpers.hpp"
#include <algorithm>

namespace ardb
{
    ZSetScoresCacheManager::ZSetScoresCacheManager(Ardb* db) :
            m_db(db), m_estimate_memory(0)
    {
    }

    void ZSetScoresCacheManager::Run()
    {
        //persist scores
        uint64 start = get_current_epoch_millis();

        LockGuard<ThreadMutex> guard(m_cache_mutex);
        LRUCache<DBItemKey, DoubleDeque*>::CacheList& cacheList = m_cache.GetCacheList();
        LRUCache<DBItemKey, DoubleDeque*>::CacheList::iterator it = cacheList.begin();
        while (it != cacheList.end())
        {
            int err = 0;
            bool createZset = false;
            ZSetMetaValue* meta = m_db->GetZSetMeta(it->first.db, it->first.key, -1, err, createZset);
            if (NULL == meta || createZset || meta->encoding || !meta->dirty)
            {
                DELETE(meta);
                DoubleDeque* scores = NULL;
                m_cache.Erase(it->first, scores);
                DELETE(scores);
                return;
            }
            if (meta->dirty)
            {
                meta->dirty = false;
                BatchWriteGuard guard(m_db->GetEngine());
                KeyObject scores_key(it->first.key, ZSET_SCORES, it->first.db);
                ZSetScoresObject scores;
                scores.scores = it->second;
                m_db->SetKeyValueObject(scores_key, scores);
                m_db->SetMeta(it->first.db, it->first.key, *meta);
                scores.scores = NULL;
            }
            DELETE(meta);
            uint64 now = get_current_epoch_millis();
            if (now - start >= 50)  //max wait 50ms
            {
                return;
            }
            it++;
        }
    }

    void ZSetScoresCacheManager::EraseOldestScores()
    {
        while (m_estimate_memory >= m_db->m_config.zset_max_score_cache_size && m_cache.Size() > 1)
        {
            ScoreCacheEntry erased_scores;
            if (m_cache.PeekFront(erased_scores))
            {
                int err = 0;
                bool createZset = false;
                ZSetMetaValue* meta = m_db->GetZSetMeta(erased_scores.first.db, erased_scores.first.key, -1, err,
                        createZset);
                if (NULL != meta && !meta->encoding && !createZset)
                {
                    meta->dirty = false;
                    BatchWriteGuard guard(m_db->GetEngine());
                    m_db->SetMeta(erased_scores.first.db, erased_scores.first.key, *meta);
                    KeyObject scores_key(erased_scores.first.key, ZSET_SCORES, erased_scores.first.db);
                    ZSetScoresObject scores;
                    scores.scores = erased_scores.second;
                    m_db->SetKeyValueObject(scores_key, scores);
                    scores.scores = NULL;
                }
                DELETE(meta);
                m_estimate_memory -= erased_scores.first.key.size();
                m_estimate_memory -= sizeof(DBID);
                m_estimate_memory -= erased_scores.second->size() * sizeof(double);
                DELETE(erased_scores.second);
                m_cache.PopFront();
            }
        }
    }

    DoubleDeque* ZSetScoresCacheManager::GetEntry(const DBItemKey& key, ZSetMetaValue& meta, bool create_if_noexist)
    {
        DoubleDeque* entry = NULL;
        if (!m_cache.Get(key, entry))
        {
            ZSetScoresObject scores;
            DoubleDeque* insert_scores = NULL;
            KeyObject scores_key(key.key, ZSET_SCORES, key.db);
            if (!meta.dirty)
            {
                if (0 == m_db->GetKeyValueObject(scores_key, scores))
                {
                    insert_scores = scores.scores;
                    scores.scores = NULL;
                }
            }
            if (NULL == insert_scores)
            {
                insert_scores = new DoubleDeque;
                Slice empty;
                ZSetKeyObject tmp(key.key, empty, -DBL_MAX, key.db);
                struct ZWalk: public WalkHandler
                {
                        DoubleDeque* z_scores;
                        int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                        {
                            ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                            z_scores->push_back(zsk->e.score.NumberValue());
                            return 0;
                        }
                        ZWalk(DoubleDeque* s) :
                                z_scores(s)
                        {
                        }
                } walk(insert_scores);
                m_db->Walk(tmp, false, false, &walk);
                meta.dirty = false;
                scores.scores = insert_scores;
                BatchWriteGuard guard(m_db->GetEngine());
                m_db->SetKeyValueObject(scores_key, scores);
                m_db->SetMeta(key.db, key.key, meta);
                scores.scores = NULL;
            }

            m_estimate_memory = m_estimate_memory + insert_scores->size() * sizeof(double);
            m_estimate_memory = m_estimate_memory + sizeof(DBID) + key.key.size();
            ScoreCacheEntry erased_scores;
            if (m_cache.Insert(key, insert_scores, erased_scores))
            {
                m_estimate_memory -= erased_scores.first.key.size();
                m_estimate_memory -= sizeof(DBID);
                m_estimate_memory -= erased_scores.second->size() * sizeof(double);
                DELETE(erased_scores.second);
            }
            return insert_scores;
        }
        return entry;
    }

    int ZSetScoresCacheManager::Replace(const DBID& db, const Slice& key, ZSetMetaValue& meta, double old_score,
            double new_score)
    {
        if (0 == Remove(db, key, meta, old_score))
        {
            return Insert(db, key, meta, new_score);
        }
        return ERR_NOT_EXIST;
    }
    int ZSetScoresCacheManager::Insert(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, true);
        if (NULL == scores)
        {
            return ERR_NOT_EXIST;
        }
        DoubleDeque::iterator fit = std::lower_bound(scores->begin(), scores->end(), score);
        if (fit != scores->end())
        {
            scores->insert(fit, score);
            return 0;
        }
        else
        {
            scores->push_back(score);
        }
        m_estimate_memory += sizeof(double);
        if (!meta.dirty)
        {
            meta.dirty = true;
            m_db->SetMeta(db, key, meta);
        }
        EraseOldestScores();
        return 0;
    }
    int ZSetScoresCacheManager::RemoveScores(const DBID& db, const Slice& key, ZSetMetaValue& meta, DoubleDeque& scores)
    {
        DoubleDeque::iterator it = scores.begin();
        while (it != scores.end())
        {
            Remove(db, key, meta, *it);
            it++;
        }
        return 0;
    }

    int ZSetScoresCacheManager::Remove(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        if (NULL == scores)
        {
            return ERR_NOT_EXIST;
        }
        int err = ERR_NOT_EXIST;
        DoubleDeque::iterator fit = std::lower_bound(scores->begin(), scores->end(), score);
        if (fit != scores->end() && *fit == score)
        {
            scores->erase(fit);
            m_estimate_memory -= sizeof(double);
            if (!meta.dirty)
            {
                meta.dirty = true;
                m_db->SetMeta(db, key, meta);
            }
            err = 0;
        }
        EraseOldestScores();
        return err;
    }
    int ZSetScoresCacheManager::RemoveAll(const DBID& db, const Slice& key)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = NULL;
        if (m_cache.Erase(entry_key, scores))
        {
            m_estimate_memory = m_estimate_memory - sizeof(double) * scores->size() - sizeof(DBID) - key.size();
            DELETE(scores);
            return 0;
        }
        KeyObject k(key, ZSET_SCORES, db);
        m_db->DelValue(k);
        return -1;
    }
    int ZSetScoresCacheManager::Rank(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        DoubleDeque::iterator fit = std::lower_bound(scores->begin(), scores->end(), score);
        EraseOldestScores();
        if (fit != scores->end() && *fit == score)
        {
            return fit - scores->begin();
        }
        return ERR_NOT_EXIST;
    }
    int ZSetScoresCacheManager::RevRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        DoubleDeque::iterator fit = std::lower_bound(scores->begin(), scores->end(), score);
        EraseOldestScores();
        if (fit != scores->end() && *fit == score)
        {
            return scores->end() - fit;
        }
        return ERR_NOT_EXIST;
    }

    int ZSetScoresCacheManager::Range(const DBID& db, const Slice& key, ZSetMetaValue& meta, double min, double max,
            DoubleDeque& result)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        DoubleDeque::iterator min_it = std::lower_bound(scores->begin(), scores->end(), min);
        EraseOldestScores();
        if (min_it == scores->end())
        {
            return -1;
        }
        DoubleDeque::iterator max_it = std::lower_bound(scores->begin(), scores->end(), max);
        if (max_it < min_it)
        {
            return -1;
        }
        if (max_it != scores->end())
        {
            max_it++;
        }
        result.resize(max_it - min_it);
        std::copy(min_it, max_it, result.begin());
        return 0;
    }

    int ZSetScoresCacheManager::RangeByRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, uint32 start,
            uint32 stop, DoubleDeque& result)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        EraseOldestScores();
        if (start > scores->size())
        {
            return ERR_NOT_EXIST;
        }
        if (stop > scores->size())
        {
            stop = scores->size();
        }
        DoubleDeque::iterator min_it = scores->begin() + start;
        DoubleDeque::iterator max_it = scores->begin() + stop;
        if (max_it != scores->end())
        {
            max_it++;
        }
        result.resize(max_it - min_it);
        std::copy(min_it, max_it, result.begin());
        return 0;
    }

    int ZSetScoresCacheManager::GetScoresByRanks(const DBID& db, const Slice& key, ZSetMetaValue& meta,
            const UInt32Array& ranks, DoubleDeque& result)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        EraseOldestScores();
        UInt32Array::const_iterator cit = ranks.begin();
        while (cit != ranks.end())
        {
            uint32 step = *cit;
            if (step > scores->size())
            {
                step = scores->size() - 1;
            }
            result.push_back(scores->at(step));
            cit++;
        }
        return 0;
    }

    int ZSetScoresCacheManager::GetScoresByRevRanks(const DBID& db, const Slice& key, ZSetMetaValue& meta,
            const UInt32Array& ranks, DoubleDeque& result)
    {
        DBItemKey entry_key(db, key);
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        DoubleDeque* scores = GetEntry(entry_key, meta, false);
        EraseOldestScores();
        UInt32Array::const_iterator cit = ranks.begin();
        while (cit != ranks.end())
        {
            uint32 step = *cit;
            if (step > scores->size())
            {
                step = scores->size();
            }
            if (step >= scores->size())
            {
                return ERR_NOT_EXIST;
            }
            result.push_back(scores->at(scores->size() - step));
            cit++;
        }
        return 0;
    }

    ZSetScoresCacheManager::~ZSetScoresCacheManager()
    {
        LockGuard<ThreadMutex> guard(m_cache_mutex);
        while (m_cache.Size() > 0)
        {
            ScoreCacheEntry erased_scores;
            if (m_cache.PeekFront(erased_scores))
            {
                DELETE(erased_scores.second);
            }
        }
    }
}


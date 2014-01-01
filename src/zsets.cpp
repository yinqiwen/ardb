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

#include "ardb.hpp"
#include "helper/db_helpers.hpp"

#define MAX_ZSET_RANGE_NUM  1000

namespace ardb
{
    static int parse_score(const std::string& score_str, double& score, bool& contain)
    {
        contain = true;
        const char* str = score_str.c_str();
        if (score_str.at(0) == '(')
        {
            contain = false;
            str++;
        }
        if (strcasecmp(str, "-inf") == 0)
        {
            score = -DBL_MAX;
        }
        else if (strcasecmp(str, "+inf") == 0)
        {
            score = DBL_MAX;
        }
        else
        {
            if (!str_todouble(str, score))
            {
                return ERR_INVALID_STR;
            }
        }
        return 0;
    }

    static bool less_by_zset_score(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.score < v2.score;
    }

    static bool less_by_zset_value(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.value < v2.value;
    }

    ZSetMetaValue* Ardb::GetZSetMeta(const DBID& db, const Slice& key, int zipsort_by_score, int& err, bool& created)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != ZSET_META)
        {
            DELETE(meta);
            err = ERR_INVALID_TYPE;
            return NULL;
        }
        if (NULL == meta)
        {
            meta = new ZSetMetaValue;
            created = true;
            err = ERR_NOT_EXIST;
        }
        else
        {
            ZSetMetaValue* zmeta = (ZSetMetaValue*) meta;
            if (zmeta->ziped)
            {
                zmeta->size = zmeta->zipvs.size();
                if (zipsort_by_score >= 0)
                {
                    if (zmeta->zip_sort_by_score != zipsort_by_score)
                    {
                        std::sort(zmeta->zipvs.begin(), zmeta->zipvs.end(),
                                zipsort_by_score ? less_by_zset_score : less_by_zset_value);
                    }
                }
            }
            created = false;
        }
        return (ZSetMetaValue*) meta;
    }

    int Ardb::InsertZSetZipEntry(ZSetMetaValue* meta, ZSetElement& entry, bool sort_by_score)
    {
        if (meta->zip_sort_by_score != sort_by_score)
        {
            std::sort(meta->zipvs.begin(), meta->zipvs.end(), sort_by_score ? less_by_zset_score : less_by_zset_value);
        }
        ZSetElementDeque::iterator it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), entry,
                sort_by_score ? less_by_zset_score : less_by_zset_value);
        if (it != meta->zipvs.end())
        {
            meta->zipvs.insert(it, entry);
        }
        else
        {
            meta->zipvs.push_back(entry);
        }
        return 0;
    }

    int Ardb::GetZSetZipEntry(ZSetMetaValue* meta, const ValueData& value, ZSetElement& entry, bool remove)
    {
        if (meta->zip_sort_by_score)
        {
            meta->zip_sort_by_score = false;
            std::sort(meta->zipvs.begin(), meta->zipvs.end(), less_by_zset_value);
        }
        ZSetElement serachEntry;
        serachEntry.value = value;
        ZSetElementDeque::iterator it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), serachEntry,
                less_by_zset_value);
        if (it != meta->zipvs.end())
        {
            if (it->value.Compare(value) == 0)
            {
                entry = *it;
                if (remove)
                {
                    meta->zipvs.erase(it);
                }
                return 0;
            }
        }
        return -1;
    }

    int Ardb::FindZSetMinMaxScore(const DBID& db, const Slice& key, ZSetMetaValue* meta, double& min, double& max)
    {
        if (meta->ziped)
        {
            if (!meta->zipvs.empty())
            {
                min = meta->zipvs.begin()->score;
                max = meta->zipvs.rbegin()->score;
            }
            else
            {
                min = max = 0;
            }
        }
        else
        {
            //TODO
        }
        return 0;
    }

    int Ardb::ZAddLimit(const DBID& db, const Slice& key, DoubleArray& scores, const SliceArray& svs, int setlimit,
            ValueArray& pops)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 0, err, createZset);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        if (setlimit <= 0)
        {
            setlimit = meta->size + scores.size();
        }
        for (uint32 i = 0; i < scores.size(); i++)
        {
            TryZAdd(db, key, *meta, scores[i], svs[i], true);
        }
        SetMeta(db, key, *meta);
        if (meta->size > (uint32) setlimit)
        {
            ZPop(db, key, false, meta->size - setlimit, pops);
        }
        DELETE(meta);
        return 0;
    }

    /*
     * returns  0:no meta change
     *          1:meta change
     */
    int Ardb::TryZAdd(const DBID& db, const Slice& key, ZSetMetaValue& meta, double score, const Slice& value,
            bool check_value)
    {
        bool zip_save = meta.ziped;
        ZSetElement element(value, score);
        if (zip_save)
        {
            ZSetElement entry;
            if (check_value && 0 == GetZSetZipEntry(&meta, element.value, entry, true))
            {
                if (entry.score == score)
                {
                    return 0;
                }
                else
                {
                    InsertZSetZipEntry(&meta, element, false);
                    return 1;
                }
            }
            else
            {
                if (element.value.type == BYTES_VALUE
                        && element.value.bytes_value.size() >= (uint32) m_config.zset_max_ziplist_value)
                {
                    zip_save = false;
                }
                if (meta.zipvs.size() >= (uint32) m_config.zset_max_ziplist_entries)
                {
                    zip_save = false;
                }
            }
        }
        if (zip_save)
        {
            meta.size++;
            InsertZSetZipEntry(&meta, element, false);
            return 1;
        }
        if (meta.ziped)
        {
            BatchWriteGuard guard(GetEngine());
            ZSetElementDeque::iterator it = meta.zipvs.begin();
            while (it != meta.zipvs.end())
            {
                ZSetScoreKeyObject zk(key, it->value, db);
                CommonValueObject zv;
                zv.data.SetValue(it->score);
                SetKeyValueObject(zk, zv);
                ZSetKeyObject zsk(key, it->value, it->score, db);
                EmptyValueObject zsv;
                SetKeyValueObject(zsk, zsv);
                m_db_helper->GetZSetScoresCache().Insert(db, key, meta, it->score);
                it++;
            }
            ZSetScoreKeyObject nzk(key, element.value, db);
            CommonValueObject nzv;
            nzv.data.SetValue(score);
            SetKeyValueObject(nzk, nzv);
            ZSetKeyObject zsk(key, element.value, score, db);
            EmptyValueObject zsv;
            SetKeyValueObject(zsk, zsv);
            meta.ziped = false;
            meta.zipvs.clear();
            meta.dirty = false;
            meta.size++;
            m_db_helper->GetZSetScoresCache().Insert(db, key, meta, score);
            return 1;
        }

        ZSetScoreKeyObject zk(key, value, db);
        CommonValueObject zv;
        if (!check_value || 0 != GetKeyValueObject(zk, zv))
        {
            zv.data.SetValue(score);
            SetKeyValueObject(zk, zv);
            ZSetKeyObject zsk(key, value, score, db);
            EmptyValueObject zsv;
            SetKeyValueObject(zsk, zsv);
            meta.dirty = true;
            meta.size++;
            m_db_helper->GetZSetScoresCache().Insert(db, key, meta, score);
            return 1;
        }
        else
        {
            if (zv.data.double_value != score)
            {
                m_db_helper->GetZSetScoresCache().Remove(db, key, meta, zv.data.double_value);
                ZSetKeyObject zsk(key, value, zv.data.double_value, db);
                DelValue(zsk);
                zsk.e.score = score;
                EmptyValueObject zsv;
                SetKeyValueObject(zsk, zsv);
                zv.data.SetValue(score);
                SetKeyValueObject(zk, zv);
                m_db_helper->GetZSetScoresCache().Insert(db, key, meta, score);
                return 1;
            }
        }
        return 0;
    }
    int Ardb::ZAdd(const DBID& db, const Slice& key, double score, const Slice& value)
    {
        DoubleArray scores;
        SliceArray svs;
        scores.push_back(score);
        svs.push_back(value);
        return ZAdd(db, key, scores, svs);
    }
    int Ardb::ZAdd(const DBID& db, const Slice& key, DoubleArray& scores, const SliceArray& svs)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 0, err, createZset);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        int count = meta->size;
        bool metachange = false;
        for (uint32 i = 0; i < scores.size(); i++)
        {
            int tryret = TryZAdd(db, key, *meta, scores[i], svs[i], true);
            if (tryret > 0)
            {
                metachange = true;
            }
        }
        if (metachange)
        {
            SetMeta(db, key, *meta);
        }
        int len = meta->size - count;
        DELETE(meta);
        return len;
    }

    int Ardb::ZCard(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, -1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        int size = meta->size;
        DELETE(meta);
        return size;
    }

    int Ardb::ZScore(const DBID& db, const Slice& key, const Slice& value, double& score)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, -1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            ZSetElement e;
            e.value.SetValue(value, true);
            if (!meta->zip_sort_by_score)
            {
                ZSetElementDeque::iterator fit = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), e,
                        less_by_zset_value);
                if (fit != meta->zipvs.end() && fit->value.Compare(e.value) == 0)
                {
                    score = fit->score;
                }
                else
                {
                    err = ERR_NOT_EXIST;
                }
            }
            else
            {
                bool found = false;
                ZSetElementDeque::iterator zit = meta->zipvs.begin();
                while (zit != meta->zipvs.end())
                {
                    if (zit->value.Compare(e.value) == 0)
                    {
                        score = zit->score;
                        found = true;
                        break;
                    }
                    zit++;
                }
                if (!found)
                {
                    err = ERR_NOT_EXIST;
                }
            }
        }
        else
        {
            ZSetScoreKeyObject zk(key, value, db);
            CommonValueObject zv;
            if (0 != GetKeyValueObject(zk, zv))
            {
                err = ERR_NOT_EXIST;
            }
            else
            {
                score = zv.data.double_value;
            }
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::ZIncrby(const DBID& db, const Slice& key, double increment, const Slice& value, double& score)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 0, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            ValueData v(value);
            ZSetElement element;
            if (0 == GetZSetZipEntry(meta, v, element, true))
            {
                element.score += increment;
                score = element.score;
                InsertZSetZipEntry(meta, element, false);
                SetMeta(db, key, *meta);
            }
            else
            {
                err = ERR_NOT_EXIST;
            }
            DELETE(meta);
            return err;
        }
        ZSetScoreKeyObject zk(key, value, db);
        CommonValueObject zv;
        if (0 == GetKeyValueObject(zk, zv))
        {
            BatchWriteGuard guard(GetEngine());
            ZSetKeyObject zsk(key, value, zv.data.double_value, db);
            DelValue(zsk);
            m_db_helper->GetZSetScoresCache().Remove(db, key, *meta, zv.data.double_value);
            zsk.e.score += increment;
            zv.data.SetValue(zsk.e.score);
            EmptyValueObject zsv;
            SetKeyValueObject(zsk, zsv);
            score = zsk.e.score;
            SetKeyValueObject(zk, zv);
            m_db_helper->GetZSetScoresCache().Insert(db, key, *meta, score);
            err = 0;
        }
        else
        {
            err = ERR_NOT_EXIST;
        }
        DELETE(meta);
        return err;
    }

    int Ardb::ZPop(const DBID& db, const Slice& key, bool reverse, uint32 num, ValueArray& pops)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            for (uint32 i = 0; i < num && meta->zipvs.size() > 0; i++)
            {
                pops.push_back(meta->zipvs.front().value);
                m_db_helper->GetZSetScoresCache().Remove(db, key, *meta, meta->zipvs.front().score);
                meta->zipvs.pop_front();
            }
            meta->size = meta->zipvs.size();
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 0;
        }
        Slice empty;
        ZSetKeyObject sk(key, empty, reverse ? DBL_MAX : -DBL_MAX, db);
        BatchWriteGuard guard(GetEngine());
        struct ZPopWalk: public WalkHandler
        {
                Ardb* z_db;
                uint32 count;
                ValueArray& vs;
                DoubleDeque poped_score;
                int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                {
                    ZSetKeyObject* sek = (ZSetKeyObject*) k;
                    ZSetScoreKeyObject tmp(sek->key, sek->e.value, sek->db);
                    vs.push_back(sek->e.value);
                    count--;
                    poped_score.push_back(sek->e.score);
                    z_db->DelValue(*sek);
                    z_db->DelValue(tmp);
                    if (count == 0)
                    {
                        return -1;
                    }
                    return 0;
                }
                ZPopWalk(Ardb* db, uint32 i, ValueArray& v) :
                        z_db(db), count(i), vs(v), poped_score(0)
                {
                }
        } walk(this, num, pops);
        Walk(sk, reverse, false, &walk);
        if (walk.count < num)
        {
            meta->size -= (num - walk.count);
            meta->dirty = true;
            m_db_helper->GetZSetScoresCache().RemoveScores(db, key, *meta, walk.poped_score);
            SetMeta(db, key, *meta);
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::ZClear(const DBID& db, const Slice& key)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, -1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        if (!meta->ziped)
        {
            Slice empty;
            ZSetKeyObject sk(key, empty, -DBL_MAX, db);
            struct ZClearWalk: public WalkHandler
            {
                    Ardb* z_db;
                    int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        ZSetScoreKeyObject tmp(sek->key, sek->e.value, sek->db);
                        z_db->DelValue(*sek);
                        z_db->DelValue(tmp);
                        return 0;
                    }
                    ZClearWalk(Ardb* db) :
                            z_db(db)
                    {
                    }
            } walk(this);
            Walk(sk, false, false, &walk);
            m_db_helper->GetZSetScoresCache().RemoveAll(db, key);
        }
        DelMeta(db, key, meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::ZRem(const DBID& db, const Slice& key, const Slice& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, false, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ZSetScoreKeyObject zk(key, value, db);
        if (meta->ziped)
        {
            ZSetElement entry;
            if (0 == GetZSetZipEntry(meta, zk.value, entry, true))
            {
                meta->size--;
                SetMeta(db, key, *meta);
            }
            DELETE(meta);
            return 0;
        }

        CommonValueObject zv;
        if (0 == GetKeyValueObject(zk, zv))
        {
            BatchWriteGuard guard(GetEngine());
            DelValue(zk);
            ZSetKeyObject zsk(key, value, zv.data.double_value, db);
            DelValue(zsk);
            meta->size--;
            SetMeta(db, key, *meta);
            DELETE(meta);
            m_db_helper->GetZSetScoresCache().Remove(db, key, *meta, zv.data.double_value);
            return 0;
        }
        DELETE(meta);
        return ERR_NOT_EXIST;
    }

    int Ardb::ZCount(const DBID& db, const Slice& key, const std::string& min, const std::string& max)
    {
        bool containmin = true;
        bool containmax = true;
        double min_score, max_score;
        if (parse_score(min, min_score, containmin) < 0 || parse_score(max, max_score, containmax) < 0)
        {
            return ERR_INVALID_ARGS;
        }
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        int count = 0;
        if (meta->ziped)
        {
            ZSetElement min_ele(Slice(), min_score);
            ZSetElement max_ele(Slice(), max_score);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            while (!containmin && min_it != meta->zipvs.end() && min_it->score == min_score)
            {
                min_it++;
            }
            while (!containmax && max_it != meta->zipvs.end() && max_it->score == max_score)
            {
                max_it--;
            }
            if (min_it != meta->zipvs.end())
            {
                if (max_it != meta->zipvs.end())
                {
                    count = (max_it - min_it) + 1;
                }
                else
                {
                    count = meta->zipvs.size() - (min_it - meta->zipvs.begin());
                }
            }
        }
        else
        {
            DoubleDeque result;
            m_db_helper->GetZSetScoresCache().Range(db, key, *meta, min_score, max_score, result);
            count = result.size();
        }
        DELETE(meta);
        return count;
    }

    int Ardb::ZRank(const DBID& db, const Slice& key, const Slice& member)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ValueData element(member);
        int32 rank = 0;
        if (meta->ziped)
        {
            bool found = false;
            ZSetElementDeque::iterator it = meta->zipvs.begin();
            while (it != meta->zipvs.end())
            {
                if (it->value.Compare(element) == 0)
                {
                    found = true;
                    break;
                }
                it++;
                rank++;
            }
            if (!found)
            {
                rank = ERR_NOT_EXIST;
            }
        }
        else
        {
            double score;
            if (0 == ZScore(db, key, member, score))
            {
                rank = m_db_helper->GetZSetScoresCache().Rank(db, key, *meta, score);
            }
            else
            {
                rank = ERR_NOT_EXIST;
            }
        }
        DELETE(meta);
        return rank;
    }

    int Ardb::ZRevRank(const DBID& db, const Slice& key, const Slice& member)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ValueData element(member);
        int32 rank = 0;
        if (meta->ziped)
        {
            bool found = false;
            ZSetElementDeque::reverse_iterator it = meta->zipvs.rbegin();
            while (it != meta->zipvs.rend())
            {
                if (it->value.Compare(element) == 0)
                {
                    found = true;
                    break;
                }
                it++;
                rank++;
            }
            if (!found)
            {
                rank = ERR_NOT_EXIST;
            }
        }
        else
        {
            double score;
            if (0 == ZScore(db, key, member, score))
            {
                rank = m_db_helper->GetZSetScoresCache().RevRank(db, key, *meta, score);
            }
            else
            {
                rank = ERR_NOT_EXIST;
            }
        }
        DELETE(meta);
        return rank;
    }

    int Ardb::ZRemRangeByRank(const DBID& db, const Slice& key, int start, int stop)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (start < 0)
        {
            start = start + meta->size;
        }
        if (stop < 0)
        {
            stop = stop + meta->size;
        }
        if (start < 0 || stop < 0 || (uint32) start >= meta->size)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        int count = 0;
        if (meta->ziped)
        {
            ZSetElementDeque::iterator esit = meta->zipvs.begin() + start;
            ZSetElementDeque::iterator eeit = meta->zipvs.end();
            if ((uint32) stop < meta->size)
            {
                eeit = meta->zipvs.begin() + stop + 1;
            }
            count = meta->zipvs.size();
            meta->zipvs.erase(esit, eeit);
            meta->size = meta->zipvs.size();
            count -= meta->zipvs.size();
            SetMeta(db, key, *meta);
            DELETE(meta);
            return count;
        }
        DoubleDeque result;
        UInt32Array ranks;
        ranks.push_back(start);
        ranks.push_back(stop);

        if (0 == m_db_helper->GetZSetScoresCache().GetScoresByRanks(db, key, *meta, ranks, result))
        {
            Slice empty;
            ZSetKeyObject tmp(key, empty, result[0], db);
            BatchWriteGuard guard(GetEngine());
            struct ZRemRangeByRankWalk: public WalkHandler
            {
                    Ardb* z_db;
                    double z_stop;
                    ZSetMetaValue& z_meta;
                    int z_count;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                        if (zsk->e.score > z_stop)
                        {
                            return -1;
                        }
                        ZSetScoreKeyObject zk(zsk->key, zsk->e.value, zsk->db);
                        z_db->DelValue(zk);
                        z_db->DelValue(*zsk);
                        z_db->m_db_helper->GetZSetScoresCache().Remove(zsk->db, zsk->key, z_meta, zsk->e.score);
                        z_meta.size--;
                        z_count++;
                        return 0;
                    }
                    ZRemRangeByRankWalk(Ardb* db, double stop, ZSetMetaValue& meta) :
                            z_db(db), z_stop(stop), z_meta(meta), z_count(0)
                    {
                    }
            } walk(this, result[1], *meta);
            Walk(tmp, false, false, &walk);
            SetMeta(db, key, *meta);
            count = walk.z_count;
        }
        DELETE(meta);
        return count;
    }

    int Ardb::ZRemRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        bool containmin = true;
        bool containmax = true;
        double min_score, max_score;
        if (parse_score(min, min_score, containmin) < 0 || parse_score(max, max_score, containmax) < 0)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (meta->ziped)
        {
            ZSetElement min_ele(Slice(), min_score);
            ZSetElement max_ele(Slice(), max_score);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            int count = meta->zipvs.size();

            if (min_it != meta->zipvs.end())
            {
                while (!containmin && min_it != meta->zipvs.end() && min_it->score == min_score)
                {
                    min_it++;
                }
                while (!containmax && max_it != meta->zipvs.end() && max_it->score == max_score)
                {
                    max_it--;
                }
                if (max_it != meta->zipvs.end())
                {
                    max_it++;
                }
                meta->zipvs.erase(min_it, max_it);
                meta->size = meta->zipvs.size();
                SetMeta(db, key, *meta);
            }
            count -= meta->zipvs.size();
            DELETE(meta);
            return count;
        }
        Slice empty;
        ZSetKeyObject tmp(key, empty, min_score, db);
        BatchWriteGuard guard(GetEngine());
        struct ZRemRangeByScoreWalk: public WalkHandler
        {
                Ardb* z_db;
                double z_min_score;
                double z_max_score;
                ZSetMetaValue& z_meta;
                bool z_contains_min;
                bool z_contains_max;
                int z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    if (zsk->e.score > z_max_score)
                    {
                        return -1;
                    }
                    if (!z_contains_min && zsk->e.score == z_min_score)
                    {
                        return 0;
                    }
                    if (!z_contains_max && zsk->e.score == z_max_score)
                    {
                        return -1;
                    }
                    ZSetScoreKeyObject zk(zsk->key, zsk->e.value, zsk->db);
                    z_db->DelValue(zk);
                    z_db->DelValue(*zsk);
                    z_db->m_db_helper->GetZSetScoresCache().Remove(zsk->db, zsk->key, z_meta, zsk->e.score);
                    z_meta.size--;
                    return 0;
                }
                ZRemRangeByScoreWalk(Ardb* db, ZSetMetaValue& meta, double min, double max, bool contains_min,
                        bool contains_max) :
                        z_db(db), z_min_score(min), z_max_score(max), z_meta(meta), z_contains_min(contains_min), z_contains_max(
                                contains_max), z_count(0)
                {
                }
        } walk(this, *meta, min_score, max_score, containmin, containmax);
        Walk(tmp, false, false, &walk);
        SetMeta(db, key, *meta);
        DELETE(meta);
        return walk.z_count;
    }

    int Ardb::ZRange(const DBID& db, const Slice& key, int start, int stop, ValueArray& values,
            ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (start < 0)
        {
            start = start + meta->size;
        }
        if (stop < 0)
        {
            stop = stop + meta->size;
        }
        if (start < 0 || stop < 0 || (uint32) start >= meta->size)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (meta->ziped)
        {
            ZSetElementDeque::iterator esit = meta->zipvs.begin() + start;
            ZSetElementDeque::iterator eeit = meta->zipvs.end();
            if ((uint32) stop < meta->size)
            {
                eeit = meta->zipvs.begin() + stop;
            }
            while (esit <= eeit)
            {
                values.push_back(esit->value);
                if (options.withscores)
                {
                    ValueData scorev;
                    scorev.SetValue(eeit->score);
                    values.push_back(scorev);
                }
                esit++;
            }
            DELETE(meta);
            return options.withscores ? (values.size() / 2) : values.size();
        }
        DoubleDeque result;
        UInt32Array ranks;
        ranks.push_back(start);
        ranks.push_back(stop);
        if (0 == m_db_helper->GetZSetScoresCache().GetScoresByRanks(db, key, *meta, ranks, result))
        {
            Slice empty;
            ZSetKeyObject tmp(key, empty, result[0], db);
            struct ZRangeWalk: public WalkHandler
            {
                    double z_stop;
                    ValueArray& z_values;
                    ZSetQueryOptions& z_options;
                    int z_count;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                        if (zsk->e.score > z_stop)
                        {
                            return -1;
                        }
                        z_values.push_back(zsk->e.value);
                        if (z_options.withscores)
                        {
                            ValueData scorev;
                            scorev.SetValue(zsk->e.score);
                            z_values.push_back(scorev);
                        }
                        z_count++;
                        return 0;
                    }
                    ZRangeWalk(double stop, ValueArray& v, ZSetQueryOptions& options) :
                            z_stop(stop), z_values(v), z_options(options), z_count(0)
                    {
                    }
            } walk(result[1], values, options);
            Walk(tmp, false, false, &walk);
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::ZRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max,
            ValueArray& values, ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        bool containmin = true;
        bool containmax = true;
        double min_score, max_score;
        if (parse_score(min, min_score, containmin) < 0 || parse_score(max, max_score, containmax) < 0)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (meta->ziped)
        {
            ZSetElement min_ele(Slice(), min_score);
            ZSetElement max_ele(Slice(), max_score);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            if (min_it != meta->zipvs.end())
            {
                while (!containmin && min_it != meta->zipvs.end() && min_it->score == min_score)
                {
                    min_it++;
                }
                while (!containmax && max_it != meta->zipvs.end() && max_it->score == max_score)
                {
                    max_it--;
                }
                while (min_it <= max_it && min_it != meta->zipvs.end())
                {
                    values.push_back(min_it->value);
                    if (options.withscores)
                    {
                        ValueData scorev;
                        scorev.SetValue(min_it->score);
                        values.push_back(scorev);
                    }
                    min_it++;
                }
            }
            DELETE(meta);
            return 0;
        }
        Slice empty;
        ZSetKeyObject tmp(key, empty, min_score, db);
        struct ZRangeByScoreWalk: public WalkHandler
        {
                ValueArray& z_values;
                ZSetQueryOptions& z_options;
                double z_min_score;
                double z_max_score;
                bool z_containmin;
                bool z_containmax;
                int z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    bool inrange = false;
                    inrange = z_containmin ? zsk->e.score >= z_min_score : zsk->e.score > z_min_score;
                    if (inrange)
                    {
                        inrange = z_containmax ? zsk->e.score <= z_max_score : zsk->e.score < z_max_score;
                    }
                    if (inrange)
                    {
                        if (z_options.withlimit)
                        {
                            if (z_count >= z_options.limit_offset
                                    && z_count <= (z_options.limit_count + z_options.limit_offset))
                            {
                                inrange = true;
                            }
                            else
                            {
                                inrange = false;
                            }
                        }
                        z_count++;
                        if (inrange)
                        {
                            z_values.push_back(zsk->e.value);
                            if (z_options.withscores)
                            {
                                z_values.push_back(ValueData(zsk->e.score));
                            }
                        }
                    }
                    if (zsk->e.score == z_max_score
                            || (z_options.withlimit && z_count > (z_options.limit_count + z_options.limit_offset)))
                    {
                        return -1;
                    }
                    return 0;
                }
                ZRangeByScoreWalk(ValueArray& v, ZSetQueryOptions& options) :
                        z_values(v), z_options(options), z_min_score(0), z_max_score(0), z_containmin(true), z_containmax(
                                true), z_count(0)
                {
                }
        } walk(values, options);
        walk.z_containmax = containmax;
        walk.z_containmin = containmin;
        walk.z_max_score = max_score;
        walk.z_min_score = min_score;
        Walk(tmp, false, false, &walk);
        DELETE(meta);
        return walk.z_count;
    }

    int Ardb::ZRevRange(const DBID& db, const Slice& key, int start, int stop, ValueArray& values,
            ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (start < 0)
        {
            start = start + meta->size;
        }
        if (stop < 0)
        {
            stop = stop + meta->size;
        }
        if (start < 0 || stop < 0 || (uint32) start >= meta->size)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (meta->ziped)
        {
            ZSetElementDeque::reverse_iterator esit = meta->zipvs.rbegin() + start;
            ZSetElementDeque::reverse_iterator eeit = meta->zipvs.rend();
            if ((uint32) stop < meta->size)
            {
                eeit = meta->zipvs.rbegin() + stop;
            }
            while (esit <= eeit)
            {
                values.push_back(esit->value);
                if (options.withscores)
                {
                    ValueData scorev;
                    scorev.SetValue(eeit->score);
                    values.push_back(scorev);
                }
                esit++;
            }
            DELETE(meta);
            return 0;
        }
        DoubleDeque result;
        UInt32Array ranks;
        ranks.push_back(start);
        ranks.push_back(stop);
        if (0 == m_db_helper->GetZSetScoresCache().GetScoresByRevRanks(db, key, *meta, ranks, result))
        {
            Slice empty;
            ZSetKeyObject tmp(key, empty, result[0], db);
            struct ZRangeWalk: public WalkHandler
            {
                    double z_stop;
                    ValueArray& z_values;
                    ZSetQueryOptions& z_options;
                    int z_count;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                        if (zsk->e.score > z_stop)
                        {
                            return -1;
                        }
                        z_values.push_back(zsk->e.value);
                        if (z_options.withscores)
                        {
                            ValueData scorev;
                            scorev.SetValue(zsk->e.score);
                            z_values.push_back(scorev);
                        }
                        z_count++;
                        return 0;
                    }
                    ZRangeWalk(double stop, ValueArray& v, ZSetQueryOptions& options) :
                            z_stop(stop), z_values(v), z_options(options), z_count(0)
                    {
                    }
            } walk(result[1], values, options);
            Walk(tmp, true, false, &walk);
        }
        DELETE(meta);
        return options.withscores ? (values.size() / 2) : values.size();
    }

    int Ardb::ZRevRangeByScore(const DBID& db, const Slice& key, const std::string& max, const std::string& min,
            ValueArray& values, ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        bool containmin = true;
        bool containmax = true;
        double min_score, max_score;
        if (parse_score(min, min_score, containmin) < 0 || parse_score(max, max_score, containmax) < 0)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (meta->ziped)
        {
            ZSetElement min_ele(Slice(), min_score);
            ZSetElement max_ele(Slice(), max_score);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            if (min_it != meta->zipvs.end())
            {
                if (!containmin && min_it->score == min_score)
                {
                    min_it++;
                }
                if (!containmax && max_it != meta->zipvs.end() && max_it->score == max_score)
                {
                    max_it--;
                }
                while (min_it <= max_it && min_it != meta->zipvs.end())
                {
                    if (options.withscores)
                    {
                        ValueData scorev;
                        scorev.SetValue(min_it->score);
                        values.push_front(scorev);
                    }
                    values.push_front(min_it->value);
                    min_it++;
                }
            }
            DELETE(meta);
            return 0;
        }

        Slice empty;
        ZSetKeyObject tmp(key, empty, max_score, db);
        struct ZRangeByScoreWalk: public WalkHandler
        {
                ValueArray& z_values;
                ZSetQueryOptions& z_options;
                double z_min_score;
                bool z_containmin;
                bool z_containmax;
                double z_max_score;
                int z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    bool inrange = false;
                    inrange = z_containmin ? zsk->e.score >= z_min_score : zsk->e.score > z_min_score;
                    if (inrange)
                    {
                        inrange = z_containmax ? zsk->e.score <= z_max_score : zsk->e.score < z_max_score;
                    }
                    if (inrange)
                    {
                        if (z_options.withlimit)
                        {
                            if (z_count >= z_options.limit_offset
                                    && z_count <= (z_options.limit_count + z_options.limit_offset))
                            {
                                inrange = true;
                            }
                            else
                            {
                                inrange = false;
                            }
                        }
                        z_count++;
                        if (inrange)
                        {
                            z_values.push_back(zsk->e.value);
                            if (z_options.withscores)
                            {
                                z_values.push_back(ValueData(zsk->e.score));
                            }
                        }
                    }
                    if (zsk->e.score == z_min_score
                            || (z_options.withlimit && z_count > (z_options.limit_count + z_options.limit_offset)))
                    {
                        return -1;
                    }
                    return 0;
                }
                ZRangeByScoreWalk(ValueArray& v, ZSetQueryOptions& options) :
                        z_values(v), z_options(options), z_count(0)
                {
                }
        } walk(values, options);
        walk.z_containmax = containmax;
        walk.z_containmin = containmin;
        walk.z_max_score = max_score;
        walk.z_min_score = min_score;
        Walk(tmp, true, false, &walk);
        DELETE(meta);
        return walk.z_count;
    }

    int Ardb::ZSetRange(const DBID& db, const Slice& key, ZSetMetaValue* meta, const ValueData& value_begin,
            const ValueData& value_end, uint32 limit, bool with_begin, ZSetElementDeque& values)
    {
        if (limit == 0 || value_begin.type == MAX_VALUE_TYPE || value_begin.Compare(value_end) == 0)
        {
            return 0;
        }
        if (meta->ziped)
        {
            if (meta->zip_sort_by_score)
            {
                meta->zip_sort_by_score = false;
                std::sort(meta->zipvs.begin(), meta->zipvs.end(), less_by_zset_value);
            }
            ZSetElement entry;
            entry.value = value_begin;
            ZSetElementDeque::iterator it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), entry,
                    less_by_zset_value);
            while (it != meta->zipvs.end())
            {
                if (value_end.Compare(it->value) < 0 || values.size() >= limit)
                {
                    break;
                }
                if (!with_begin && it->value.Compare(value_begin) == 0)
                {
                    it++;
                }
                else
                {
                    values.push_back(*it);
                    it++;
                }
            }
            return 0;
        }
        else
        {
            ZSetScoreKeyObject start(key, value_begin, db);
            struct ZWalk: public WalkHandler
            {
                    ZSetElementDeque& z_vs;
                    const ValueData& end_obj;
                    uint32 z_limit;
                    ZWalk(ZSetElementDeque& vs, const ValueData& end, uint32 limit) :
                            z_vs(vs), end_obj(end), z_limit(limit)
                    {
                    }
                    int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                    {
                        ZSetScoreKeyObject* zsk = (ZSetScoreKeyObject*) k;
                        CommonValueObject* cv = (CommonValueObject*) value;
                        if (z_vs.size() >= z_limit || zsk->value.Compare(end_obj) > 0)
                        {
                            return -1;
                        }
                        ZSetElement element;
                        element.score = cv->data.double_value;
                        element.value = zsk->value;
                        z_vs.push_back(element);
                        return 0;
                    }
            } walk(values, value_end, limit);
            Walk(start, false, false, &walk);
            if (!with_begin && values.size() > 0)
            {
                if (values.begin()->value.Compare(value_begin) == 0)
                {
                    values.pop_front();
                }
            }
        }
        return 0;
    }

    int Ardb::ZUnionStore(const DBID& db, const Slice& dst, SliceArray& keys, WeightArray& weights, AggregateType type)
    {
        while (weights.size() < keys.size())
        {
            weights.push_back(1);
        }

        ZSetMetaValueArray metaArray;
        ValueDataArray cursors;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            int err = 0;
            bool createZset = false;
            ZSetMetaValue* meta = GetZSetMeta(db, keys[i], 0, err, createZset);
            if (NULL == meta)
            {
                delete_pointer_container(metaArray);
                return err;
            }
            cursors.push_back(ValueData());
            metaArray.push_back(meta);
        }

        ZClear(db, dst);
        ValueData max;
        max.type = MAX_VALUE_TYPE;
        ValueScoreMap vm;
        ZSetMetaValue zmeta;
        while (true)
        {
            ZSetElementDequeArray rs;
            ValueData min_result;
            for (uint32 i = 0; i < keys.size(); i++)
            {
                ZSetElementDeque result;
                ZSetRange(db, keys[i], metaArray[i], cursors[i], max,
                MAX_ZSET_RANGE_NUM, false, result);
                rs.push_back(result);
                if (!result.empty())
                {
                    cursors[i] = result.rbegin()->value;
                    if (min_result.type == EMPTY_VALUE || result.begin()->value.Compare(min_result) < 0)
                    {
                        min_result = result.begin()->value;
                    }
                }
                else
                {
                    if (cursors[i].type != MAX_VALUE_TYPE)
                    {
                        cursors[i] = max;
                    }
                }
                ZSetElementDeque::iterator it = result.begin();
                while (it != result.end())
                {
                    double score = weights[i] * it->score;
                    switch (type)
                    {
                        case AGGREGATE_MIN:
                        {
                            if (score < vm[it->value])
                            {
                                vm[it->value] = score;
                            }
                            break;
                        }
                        case AGGREGATE_MAX:
                        {
                            if (score > vm[it->value])
                            {
                                vm[it->value] = score;
                            }
                            break;
                        }
                        case AGGREGATE_SUM:
                        default:
                        {
                            score += vm[it->value];
                            vm[it->value] = score;
                            break;
                        }
                    }
                    it++;
                }

            }
            bool all_empty = true;
            bool all_cursor_end = true;
            for (uint32 i = 0; i < keys.size(); i++)
            {
                all_empty = all_empty && rs[i].empty();
                all_cursor_end = all_cursor_end && (cursors[i].type == MAX_VALUE_TYPE);
                if (!rs[i].empty())
                {
                    if (max.type == MAX_VALUE_TYPE || rs[i].rbegin()->value.Compare(max) > 0)
                    {
                        max = rs[i].rbegin()->value;
                    }
                }
            }
            /*
             * dump cached data when it's ready
             */
            while (!vm.empty())
            {
                if (vm.begin()->first.Compare(min_result) < 0)
                {
                    std::string val;
                    vm.begin()->first.ToString(val);
                    TryZAdd(db, dst, zmeta, vm.begin()->second, val, false);
                    vm.erase(vm.begin());
                }
                else
                {
                    break;
                }
            }

            if (all_empty)
            {
                if (all_cursor_end)
                {
                    break;
                }
                for (uint32 i = 0; i < keys.size(); i++)
                {
                    cursors[i] = max;
                }
                max.type = MAX_VALUE_TYPE;
                /*
                 * dump rest cache data
                 */
                ValueScoreMap::iterator vit = vm.begin();
                while (vit != vm.end())
                {
                    std::string val;
                    vit->first.ToString(val);
                    TryZAdd(db, dst, zmeta, vit->second, val, false);
                    vit++;
                }
                vm.clear();
            }
        }
        delete_pointer_container(metaArray);
        if (zmeta.size > 0)
        {
            SetMeta(db, dst, zmeta);
        }
        return zmeta.size;
    }

    int Ardb::ZInterStore(const DBID& db, const Slice& dst, SliceArray& keys, WeightArray& weights, AggregateType type)
    {
        while (weights.size() < keys.size())
        {
            weights.push_back(1);
        }

        ZSetMetaValueArray metaArray;
        ValueDataArray cursors;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            int err = 0;
            bool createZset = false;
            ZSetMetaValue* meta = GetZSetMeta(db, keys[i], 0, err, createZset);
            if (NULL == meta || createZset)
            {
                if (createZset)
                {
                    err = 0;
                }
                DELETE(meta);
                delete_pointer_container(metaArray);
                return err;
            }
            cursors.push_back(ValueData());
            metaArray.push_back(meta);
        }

        ZClear(db, dst);
        ValueData max;
        max.type = MAX_VALUE_TYPE;
        ValueScoreCountMap vm;
        ZSetMetaValue zmeta;
        while (true)
        {
            ZSetElementDequeArray rs;
            ValueData min_result;
            for (uint32 i = 0; i < keys.size(); i++)
            {
                ZSetElementDeque result;
                ZSetRange(db, keys[i], metaArray[i], cursors[i], max,
                MAX_ZSET_RANGE_NUM, false, result);
                rs.push_back(result);
                if (!result.empty())
                {
                    cursors[i] = result.rbegin()->value;
                    if (min_result.type == EMPTY_VALUE || result.begin()->value.Compare(min_result) < 0)
                    {
                        min_result = result.begin()->value;
                    }
                }
                else
                {
                    if (cursors[i].type != MAX_VALUE_TYPE)
                    {
                        cursors[i] = max;
                    }
                }
                ZSetElementDeque::iterator it = result.begin();
                while (it != result.end())
                {
                    vm[it->value].second++;
                    double score = weights[i] * it->score;
                    switch (type)
                    {
                        case AGGREGATE_MIN:
                        {
                            if (score < vm[it->value].first)
                            {
                                vm[it->value].first = score;
                            }
                            break;
                        }
                        case AGGREGATE_MAX:
                        {
                            if (score > vm[it->value].first)
                            {
                                vm[it->value].first = score;
                            }
                            break;
                        }
                        case AGGREGATE_SUM:
                        default:
                        {
                            vm[it->value].first += score;
                            break;
                        }
                    }
                    it++;
                }
            }
            bool all_empty = true;
            bool all_cursor_end = true;
            for (uint32 i = 0; i < keys.size(); i++)
            {
                all_empty = all_empty && rs[i].empty();
                all_cursor_end = all_cursor_end && (cursors[i].type == MAX_VALUE_TYPE);
                if (!rs[i].empty())
                {
                    if (max.type == MAX_VALUE_TYPE || rs[i].rbegin()->value.Compare(max) > 0)
                    {
                        max = rs[i].rbegin()->value;
                    }
                }
            }
            /*
             * dump cached data when it's ready
             */
            while (!vm.empty())
            {
                if (vm.begin()->first.Compare(min_result) < 0)
                {
                    if (vm.begin()->second.second == keys.size())
                    {
                        std::string val;
                        vm.begin()->first.ToString(val);
                        TryZAdd(db, dst, zmeta, vm.begin()->second.first, val, false);
                    }
                    vm.erase(vm.begin());
                }
                else
                {
                    break;
                }
            }

            if (all_empty)
            {
                if (all_cursor_end)
                {
                    break;
                }
                for (uint32 i = 0; i < keys.size(); i++)
                {
                    cursors[i] = max;
                }
                max.type = MAX_VALUE_TYPE;
                /*
                 * dump rest cache data
                 */
                ValueScoreCountMap::iterator vit = vm.begin();
                while (vit != vm.end())
                {
                    if (vit->second.second == keys.size())
                    {
                        std::string val;
                        vit->first.ToString(val);
                        TryZAdd(db, dst, zmeta, vit->second.first, val, false);
                    }
                    vit++;
                }
                vm.clear();
            }
        }
        delete_pointer_container(metaArray);
        if (zmeta.size > 0)
        {
            SetMeta(db, dst, zmeta);
        }
        return zmeta.size;
    }

    int Ardb::RenameZSet(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, ZSetMetaValue* meta)
    {
        BatchWriteGuard guard(GetEngine());
        if (meta->ziped)
        {
            DelMeta(db1, key1, meta);
            SetMeta(db2, key2, *meta);
        }
        else
        {
            Slice empty;
            ZSetKeyObject k(key1, Slice(), -DBL_MAX, db1);
            ZSetMetaValue zmeta;
            zmeta.ziped = false;
            zmeta.dirty = true;
            struct SetWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    ZSetMetaValue& zm;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        sek->key = dst;
                        sek->db = dstdb;
                        EmptyValueObject empty;
                        z_db->SetKeyValueObject(*sek, empty);
                        ZSetScoreKeyObject zsk(dst, sek->e.value, dstdb);
                        CommonValueObject score_obj;
                        score_obj.data.SetValue(sek->e.score);
                        z_db->SetKeyValueObject(zsk, score_obj);
                        z_db->m_db_helper->GetZSetScoresCache().Insert(dstdb, dst, zm, sek->e.score);
                        zm.size++;
                        return 0;
                    }
                    SetWalk(Ardb* db, const DBID& dbid, const Slice& dstkey, ZSetMetaValue& m) :
                            z_db(db), dstdb(dbid), dst(dstkey), zm(m)
                    {
                    }
            } walk(this, db2, key2, zmeta);
            Walk(k, false, true, &walk);
            SetMeta(db2, key2, zmeta);
            ZClear(db1, key1);
        }
        return 0;
    }
}


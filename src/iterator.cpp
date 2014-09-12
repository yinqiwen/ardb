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
#include "iterator.hpp"
#include "ardb.hpp"
#include "geo/geohash_helper.hpp"
#include <algorithm>

OP_NAMESPACE_BEGIN

    KVIterator::KVIterator() :
            m_meta(NULL), m_iter(NULL)
    {
    }
    KVIterator::~KVIterator()
    {
        DELETE(m_iter);
    }

    ListIterator::ListIterator() :
            m_zip_cursor(0)
    {

    }
    ListIterator::~ListIterator()
    {

    }
    Data* ListIterator::Element()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            return &(m_meta->meta.ziplist[m_zip_cursor]);
        }
        else
        {
            return &(m_current_value.element);
        }

    }
    Data* ListIterator::Score()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            static Data default_score;
            default_score.SetInt64(0);
            return &default_score;
        }
        else
        {
            return &(m_current_key.score);
        }
    }
    bool ListIterator::Prev()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            m_zip_cursor--;
        }
        else
        {
            m_iter->Prev();
        }
        return true;

    }
    bool ListIterator::Next()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            m_zip_cursor++;
        }
        else
        {
            m_iter->Next();
        }
        return true;
    }
    bool ListIterator::Valid()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            if (m_zip_cursor >= m_meta->meta.ziplist.size())
            {
                return false;
            }
            return true;
        }
        if (NULL == m_iter || !m_iter->Valid())
        {
            return false;
        }
        m_current_raw_key = m_iter->Key();
        m_current_raw_value = m_iter->Value();
        if (decode_key(m_current_raw_key, m_current_key) && decode_value(m_current_raw_value, m_current_value))
        {
            if (m_current_key.db == m_meta->key.db && m_current_key.key == m_meta->key.key)
            {
                return true;
            }
        }
        return false;
    }

    int Ardb::ListIter(Context& ctx, ValueObject& meta, ListIterator& iter, bool reverse)
    {
        iter.SetMeta(&meta);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            iter.m_zip_cursor = reverse ? meta.meta.ziplist.size() - 1 : 0;
            return 0;
        }
        KeyObject kk;
        kk.type = LIST_ELEMENT;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        if (reverse)
        {
            kk.score = meta.meta.max_index;
        }
        else
        {
            kk.score = meta.meta.min_index;
        }
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

    int Ardb::SequencialListIter(Context& ctx, ValueObject& meta, ListIterator& iter, int64 index)
    {
        if (!meta.meta.IsSequentialList() || meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            ERROR_LOG("Can NOT create sequence list iterator.");
            return -1;
        }
        iter.SetMeta(&meta);
        KeyObject kk;
        kk.type = LIST_ELEMENT;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.score = meta.meta.min_index;
        if(index >= 0)
        {
            kk.score.IncrBy(index);
        }else
        {
            kk.score.IncrBy(meta.meta.Length() + index);
        }
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

    const Data* HashIterator::Field()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            return &(m_zip_iter->first);
        }
        else
        {
            return &(m_current_key.element);
        }
    }
    Data* HashIterator::Value()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            return &(m_zip_iter->second);
        }
        else
        {
            return &(m_current_value.element);
        }
    }
    bool HashIterator::Next()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            m_zip_iter++;
        }
        else
        {
            m_iter->Next();
        }
        return true;
    }
    bool HashIterator::Valid()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            if (m_zip_iter == m_meta->meta.zipmap.end())
            {
                return false;
            }
            return true;
        }
        if (NULL == m_iter || !m_iter->Valid())
        {
            return false;
        }
        m_current_raw_key = m_iter->Key();
        m_current_raw_value = m_iter->Value();
        if (decode_key(m_current_raw_key, m_current_key) && decode_value(m_current_raw_value, m_current_value))
        {
            if (m_current_key.db == m_meta->key.db && m_current_key.key == m_meta->key.key)
            {
                return true;
            }
        }
        return false;
    }

    int Ardb::HashIter(Context& ctx, ValueObject& meta, const std::string& from, HashIterator& iter, bool readonly)
    {
        iter.SetMeta(&meta);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            if (from == "")
            {
                iter.m_zip_iter = meta.meta.zipmap.begin();
            }
            else
            {
                Data f(from);
                iter.m_zip_iter = meta.meta.zipmap.lower_bound(f);
            }
            return 0;
        }

        KeyObject kk;
        kk.type = HASH_FIELD;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.element.SetString(from, true);
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

    Data* SetIterator::Element()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            return &(*m_zip_iter);
        }
        else
        {
            return &(m_current_key.element);
        }
    }
    bool SetIterator::Next()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            m_zip_iter++;
        }
        else
        {
            m_iter->Next();
        }
        return true;
    }
    bool SetIterator::Valid()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            if (m_zip_iter == m_meta->meta.zipset.end())
            {
                return false;
            }
            return true;
        }
        if (NULL == m_iter || !m_iter->Valid())
        {
            return false;
        }
        m_current_raw_key = m_iter->Key();
        if (decode_key(m_current_raw_key, m_current_key))
        {
            if (m_current_key.db == m_meta->key.db && m_current_key.key == m_meta->key.key)
            {
                return true;
            }
        }
        return false;
    }

    int Ardb::SetIter(Context& ctx, ValueObject& meta, Data& from, SetIterator& iter, bool readonly)
    {
        iter.SetMeta(&meta);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            if (from.IsNil())
            {
                iter.m_zip_iter = meta.meta.zipset.begin();
            }
            else
            {
                Data f(from);
                iter.m_zip_iter = meta.meta.zipset.lower_bound(f);
            }
            return 0;
        }

        KeyObject kk;
        kk.type = SET_ELEMENT;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.element = from;
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

    const Data* ZSetIterator::Element()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (m_iter_by_value)
            {
                return &(m_zip_iter->first);
            }
            else
            {
                return &(m_ziparray[m_zip_cursor].first);
            }
        }
        else
        {
            if (NULL != m_cache)
            {
                return &((*m_cache_iter)->value);
            }
            else
            {
                return &(m_current_key.element);
            }

        }
    }
    const Data* ZSetIterator::Score()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (m_iter_by_value)
            {
                return &(m_zip_iter->second);
            }
            else
            {
                return &(m_ziparray[m_zip_cursor].second);
            }
        }
        else
        {
            if (NULL != m_cache)
            {
                return &((*m_cache_iter)->score);
            }
            if (m_iter_by_value)
            {
                return &(m_current_value.score);
            }
            else
            {
                return &(m_current_key.score);
            }
        }
    }

    void ZSetIterator::GeoLocation(Location& loc)
    {
        if (NULL != m_cache)
        {
            if ((*m_cache_iter)->loc != NULL)
            {
                loc = *((*m_cache_iter)->loc);
                return;
            }

        }
        const Data* sc = Score();
        if (NULL != sc)
        {
            GeoHashHelper::GetMercatorXYByHash(sc->value.iv, loc.x, loc.y);
        }

    }
    bool ZSetIterator::Next()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (m_iter_by_value)
            {
                m_zip_iter++;
            }
            else
            {
                m_zip_cursor++;
            }
        }
        else
        {
            if (NULL != m_cache)
            {
                m_cache_iter++;
            }
            else
            {
                m_iter->Next();
            }
        }
        return true;
    }
    bool ZSetIterator::Prev()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (m_iter_by_value)
            {
                m_zip_iter--;
            }
            else
            {
                m_zip_cursor--;
            }
        }
        else
        {
            if (NULL != m_cache)
            {
                m_cache_iter--;
            }
            else
            {
                m_iter->Prev();
            }

        }
        return true;
    }
    void ZSetIterator::Skip(uint32 step)
    {
        if (!m_iter_by_value)
        {
            if (NULL != m_cache)
            {
                m_cache_iter.increment_by(step);
                return;
            }
            for (uint32 i = 0; i < step; i++)
            {
                m_iter->Next();
            }
        }
    }

    void ZSetIterator::SeekScore(const Data& score)
    {
        if (!m_iter_by_value)
        {
            if (NULL != m_cache)
            {
                m_cache_iter = m_cache->LowerBound(score);
                return;
            }
            KeyObject kk;
            kk.type = ZSET_ELEMENT_SCORE;
            kk.db = m_meta->key.db;
            kk.key = m_meta->key.key;
            kk.score = score;
            g_db->IteratorSeek(m_iter, kk);
        }
    }
    bool ZSetIterator::Valid()
    {
        if (m_meta->meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (m_iter_by_value)
            {
                if (m_zip_iter == GetZipMap().end())
                {
                    return false;
                }
                return true;
            }
            return m_zip_cursor < m_ziparray.size();
        }
        if (NULL != m_cache)
        {
            if (m_cache->data.end() == m_cache_iter)
            {
                return false;
            }
            return true;
        }
        if (NULL == m_iter || !m_iter->Valid())
        {
            return false;
        }
        m_current_raw_key = m_iter->Key();
        m_current_raw_value = m_iter->Value();
        if (m_iter_by_value)
        {
            decode_value(m_current_raw_value, m_current_value);
        }
        if (decode_key(m_current_raw_key, m_current_key))
        {
            if (m_current_key.db == m_meta->key.db && m_current_key.key == m_meta->key.key)
            {
                if (m_iter_by_value)
                {
                    return m_current_key.type == ZSET_ELEMENT_VALUE;
                }
                else
                {
                    return m_current_key.type == ZSET_ELEMENT_SCORE;
                }

            }
        }
        return false;
    }

    ZSetIterator::~ZSetIterator()
    {
        if (NULL != m_cache)
        {
            g_db->m_cache.RecycleReadCache(m_cache);
        }
    }
    static bool less_by_zset_score(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.second.NumberValue() < v2.second.NumberValue();
    }
    int Ardb::ZSetScoreIter(Context& ctx, ValueObject& meta, const Data& from, ZSetIterator& iter, bool readonly)
    {
        iter.SetMeta(&meta);
        iter.m_iter_by_value = false;
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            iter.m_ziparray.assign(meta.meta.zipmap.begin(), meta.meta.zipmap.end());
            std::sort(iter.m_ziparray.begin(), iter.m_ziparray.end(), less_by_zset_score);
            if (from.IsNil())
            {
                iter.m_zip_cursor = 0;
            }
            else
            {
                ZSetElement e;
                e.second = from;
                iter.m_zip_cursor = std::lower_bound(iter.m_ziparray.begin(), iter.m_ziparray.end(), e,
                        less_by_zset_score) - iter.m_ziparray.begin();
            }
            return 0;
        }
        if (readonly)
        {
            std::string k(meta.key.key.data(), meta.key.key.size());
            CacheData* cache = m_cache.GetReadCache(ctx.currentDB, k, ZSET_ELEMENT_SCORE);
            if (NULL != cache)
            {
                iter.m_cache = (ZSetDataCache*) cache;
                if (from.IsNil())
                {
                    iter.m_cache_iter = iter.m_cache->data.begin();
                }
                else
                {
                    iter.SeekScore(from);
                }
                return 0;
            }
            else
            {
                if (m_cfg.L1_zset_seek_load_cache)
                {
                    CacheLoadOptions opt;
                    opt.deocode_geo = ctx.current_cmd_type == REDIS_CMD_GEO_SEARCH;
                    m_cache.Load(ctx.currentDB, k, ZSET_META, opt);
                }
            }
        }

        KeyObject kk;
        kk.type = ZSET_ELEMENT_SCORE;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.score = from;
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }
    int Ardb::ZSetValueIter(Context& ctx, ValueObject& meta, Data& from, ZSetIterator& iter, bool readonly)
    {
        iter.SetMeta(&meta);
        iter.m_iter_by_value = true;
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPZSET)
        {
            if (!readonly)
            {
                iter.m_zipmap_clone = meta.meta.zipmap;
            }
            iter.m_readonly = readonly;
            if (from.IsNil())
            {
                iter.m_zip_iter = iter.GetZipMap().begin();
            }
            else
            {
                iter.m_zip_iter = iter.GetZipMap().lower_bound(from);
            }
            return 0;
        }
        KeyObject kk;
        kk.type = ZSET_ELEMENT_VALUE;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.element = from;
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

    int64 BitsetIterator::Index()
    {
        return m_current_key.score.value.iv;
    }
    ValueObject& BitsetIterator::Value()
    {
        return m_current_value;
    }
    std::string BitsetIterator::Bits()
    {
        std::string str;
        if (NULL != m_current_value.element.value.sv)
        {
            str.assign(m_current_value.element.value.sv, sdslen(m_current_value.element.value.sv));
        }
        return str;
    }
    int64 BitsetIterator::Count()
    {
        return m_current_value.score.value.iv;
    }
    bool BitsetIterator::Next()
    {
        m_iter->Next();
        return true;
    }
    bool BitsetIterator::Valid()
    {
        if (NULL == m_iter || !m_iter->Valid())
        {
            return false;
        }
        m_current_raw_key = m_iter->Key();
        m_current_raw_value = m_iter->Value();
        if (decode_key(m_current_raw_key, m_current_key) && decode_value(m_current_raw_value, m_current_value))
        {
            if (m_current_key.db == m_meta->key.db && m_current_key.key == m_meta->key.key)
            {
                return true;
            }
        }
        return false;
    }

    int Ardb::BitsetIter(Context& ctx, ValueObject& meta, int64 index, BitsetIterator& iter)
    {
        iter.SetMeta(&meta);
        KeyObject kk;
        kk.type = BITSET_ELEMENT;
        kk.db = ctx.currentDB;
        kk.key = meta.key.key;
        kk.score.SetInt64(index);
        Iterator* it = IteratorKeyValue(kk, false);
        iter.SetIter(it);
        return 0;
    }

OP_NAMESPACE_END


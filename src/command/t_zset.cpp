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
#include "ardb.hpp"
#include <fnmatch.h>
#include <float.h>
#include "geo/geohash_helper.hpp"

OP_NAMESPACE_BEGIN

    static bool less_by_zset_score(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.second.NumberValue() < v2.second.NumberValue();
    }
    static bool greate_by_zset_score(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.second.NumberValue() > v2.second.NumberValue();
    }

//    int Ardb::ZSetUpdateScoreRangeTable(Context& ctx, ValueObject& meta, double score, DataOperation op)
//    {
//        if (op != OP_SET && op != OP_DELETE)
//        {
//            ERROR_LOG("Invalid operation.");
//            return -1;
//        }
//        ScoreRanges::iterator fit = meta.meta.zset_score_range.lower_bound(score);
//        if (fit == meta.meta.zset_score_range.end())
//        {
//            if (op == OP_SET)
//            {
//                if (meta.meta.zset_score_range.size() > 0)
//                {
//                    double last = meta.meta.zset_score_range.rbegin()->first;
//                    uint32 count = meta.meta.zset_score_range.rbegin()->second;
//                    meta.meta.zset_score_range.erase(last);
//                    meta.meta.zset_score_range[score] = count + 1;
//                }
//                else
//                {
//                    meta.meta.zset_score_range[score] = 1;
//                }
//                fit = meta.meta.zset_score_range.find(score);
//            }
//            else
//            {
//                return 0;
//            }
//        }
//        else
//        {
//            if (op == OP_SET)
//            {
//                fit->second++;
//            }
//            else
//            {
//                fit->second--;
//            }
//        }
//        double range_score = fit->first;
//        uint32 range_size = fit->second;
//
//        if (range_size > m_cfg.zset_max_score_range_size)
//        {
//            //merge small range
//            ScoreRanges::iterator bit = meta.meta.zset_score_range.begin();
//            uint32 prev_range_size;
//            double prev_range_score;
//            while (bit != meta.meta.zset_score_range.end())
//            {
//                if (bit != meta.meta.zset_score_range.begin())
//                {
//                    if (prev_range_size + bit->second < m_cfg.zset_max_score_range_size)
//                    {
//                        bit->second = prev_range_size + bit->second;
//                        meta.meta.zset_score_range.erase(prev_range_score);
//                        break;
//                    }
//                }
//                prev_range_size = bit->second;
//                prev_range_score = bit->first;
//                bit++;
//            }
//
//            if (meta.meta.zset_score_range.size() >= m_cfg.zset_max_range_table_size)
//            {
//                //do nothing since score range table size exceed limit
//                return 1;
//            }
//
//            //split range
//            uint32 split_range_size = range_size / 2;
//            uint32 new_range_size = 0;
//            bool splitd = false;
//            ZSetIterator iter;
//            Data from;
//            from.SetDouble(range_score);
//            ZSetScoreIter(ctx, meta, from, iter, true);
//            while (iter.Valid())
//            {
//                if (iter.Score()->Compare(from) <= 0)
//                {
//                    if (splitd)
//                    {
//                        meta.meta.zset_score_range[iter.Score()->NumberValue()] = range_size - new_range_size;
//                        break;
//                    }
//                    new_range_size++;
//                    if (new_range_size < range_size / 2)
//                    {
//                        fit->second = new_range_size;
//                        splitd = true;
//                    }
//                }
//                iter.Next();
//            }
//        }
//        else if (range_size == 0)
//        {
//            meta.meta.zset_score_range.erase(fit);
//        }
//        return 1;
//    }

    int Ardb::ZSetAdd(Context& ctx, ValueObject& meta, const Data& element, const Data& score, Data* old_score)
    {
        uint32 count = 0;
        int64 oldlen = meta.meta.Length();
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            meta.meta.zipmap[element] = score;
            if (meta.meta.zipmap.size() > m_cfg.zset_max_ziplist_entries
                    || element.StringLength() >= m_cfg.zset_max_ziplist_value)
            {
                meta.meta.encoding = COLLECTION_ECODING_RAW;
                DataMap::iterator it = meta.meta.zipmap.begin();
                BatchWriteGuard guard(GetKeyValueEngine());
                while (it != meta.meta.zipmap.end())
                {
                    if (meta.meta.min_index.IsNil() || meta.meta.min_index > it->second)
                    {
                        meta.meta.min_index = it->second;
                    }
                    if (meta.meta.max_index.IsNil() || meta.meta.max_index < it->second)
                    {
                        meta.meta.max_index = it->second;
                    }
                    ValueObject v(ZSET_ELEMENT_VALUE);
                    v.score = it->second;
                    v.key.type = ZSET_ELEMENT_VALUE;
                    v.key.key = meta.key.key;
                    v.key.db = ctx.currentDB;
                    v.key.element = it->first;
                    SetKeyValue(ctx, v);

                    ValueObject sv(ZSET_ELEMENT_SCORE);
                    sv.key.type = ZSET_ELEMENT_SCORE;
                    sv.key.key = meta.key.key;
                    sv.key.db = ctx.currentDB;
                    sv.key.element = it->first;
                    sv.key.score = it->second;
                    SetKeyValue(ctx, sv);
                    it++;
                }
                meta.meta.len = meta.meta.zipmap.size();
                //meta.meta.zset_score_range[meta.meta.max_index.NumberValue()] = meta.meta.zipmap.size();
                meta.meta.zipmap.clear();
            }
        }
        else
        {
            ValueObject v(ZSET_ELEMENT_VALUE);
            v.key.type = ZSET_ELEMENT_VALUE;
            v.key.key = meta.key.key;
            v.key.db = ctx.currentDB;
            v.key.element = element;
            bool found = false;
            if (NULL != old_score)
            {
                found = true;
                v.score = *old_score;
            }
            else
            {
                found = (0 == GetKeyValue(ctx, v.key, &v));
            }

            if (found && v.score == score)
            {
                return 0;
            }
            if (found)
            {
                KeyObject score_key;
                score_key.db = ctx.currentDB;
                score_key.key = meta.key.key;
                score_key.type = ZSET_ELEMENT_SCORE;
                score_key.element = v.key.element;
                score_key.score = v.score;
                DelKeyValue(ctx, score_key);
                //ZSetUpdateScoreRangeTable(ctx, meta, old_score, OP_DELETE);
            }
            else
            {
                meta.meta.len++;
            }
            if (meta.meta.min_index > score)
            {
                meta.meta.min_index = score;
            }
            if (meta.meta.max_index < score)
            {
                meta.meta.max_index = score;
            }
            v.score = score;
            SetKeyValue(ctx, v);
            ValueObject sv(ZSET_ELEMENT_SCORE);
            sv.key.type = ZSET_ELEMENT_SCORE;
            sv.key.key = meta.key.key;
            sv.key.db = ctx.currentDB;
            sv.key.element = element;
            sv.key.score = score;
            SetKeyValue(ctx, sv);
            //ZSetUpdateScoreRangeTable(ctx, meta, score.NumberValue(), OP_SET);
        }
        count = meta.meta.Length() - oldlen;
        return count;
    }
    int Ardb::ZAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for ZAdd");
            return 0;
        }
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        uint32 count = 0;
        BatchWriteGuard guard(GetKeyValueEngine(),
                meta.meta.encoding != COLLECTION_ECODING_RAW || cmd.GetArguments().size() > 3);
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            Data score;
            if (!score.SetNumber(cmd.GetArguments()[i]))
            {
                guard.MarkFailed();
                fill_error_reply(ctx.reply, "value is not a float or out of range");
                return 0;
            }
            Data element;
            element.SetString(cmd.GetArguments()[i + 1], true);
            count += ZSetAdd(ctx, meta, element, score, NULL);
        }
        SetKeyValue(ctx, meta);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::ZSetLen(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        fill_int_reply(ctx.reply, meta.meta.Length());
        return 0;
    }

    int Ardb::ZCard(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetLen(ctx, cmd.GetArguments()[0]);
        return 0;
    }

    int Ardb::ZSetRankByScore(Context& ctx, ValueObject& meta, Data& sc, uint32& rank)
    {
        rank = 0;
        if (meta.meta.min_index.Compare(sc) > 0)
        {
            return -1;
        }
        if (meta.meta.max_index.Compare(sc) < 0)
        {
            rank = meta.meta.Length() - 1;
            return -1;
        }
        if (sc == meta.meta.min_index)
        {
            rank = 0;
            return 0;
        }
        if (sc == meta.meta.max_index)
        {
            rank = meta.meta.Length() - 1;
            return 0;
        }

        ZSetIterator iter;
        ZSetScoreIter(ctx, meta, meta.meta.min_index, iter, true);
        bool match_score = false;
        while (iter.Valid())
        {
            int cmp = iter.Score()->Compare(sc);
            if (cmp > 0)
            {
                break;
            }
            else if (cmp == 0)
            {
                match_score = true;
                break;
            }
            else
            {
                rank++;
                iter.Next();
            }
        }
        return match_score ? 0 : -1;
    }

    int Ardb::ZCount(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        ZRangeSpec range;
        if (!range.Pasrse(cmd.GetArguments()[1], cmd.GetArguments()[2]))
        {
            fill_error_reply(ctx.reply, "Invalid arguments.");
            return 0;
        }
        if (range.min.NumberValue() == -DBL_MAX && range.max.NumberValue() == DBL_MAX)
        {
            fill_int_reply(ctx.reply, meta.meta.Length());
            return 0;
        }
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            uint32 count = 0;
            DataMap::iterator it = meta.meta.zipmap.begin();
            while (it != meta.meta.zipmap.end())
            {
                if (range.InRange(it->second))
                {
                    count++;
                }
                it++;
            }
            fill_int_reply(ctx.reply, count);
        }
        else
        {
            ZSetIterator iter;
            ZSetScoreIter(ctx, meta, range.min, iter, true);
            uint32 count = 0;
            while (iter.Valid())
            {
                if (range.InRange(*iter.Score()))
                {
                    count++;
                }
                else
                {
                    if (iter.Score()->Compare(range.max) > 0)
                    {
                        break;
                    }
                }
                iter.Next();
            }
            fill_int_reply(ctx.reply, count);
        }
        return 0;
    }

    int Ardb::ZSetScore(Context& ctx, ValueObject& meta, const Data& value, Data& score, Location* loc)
    {
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            DataMap::iterator found = meta.meta.zipmap.find(value);
            if (found == meta.meta.zipmap.end())
            {
                return -1;
            }
            score = found->second;
            if (NULL != loc)
            {
                GeoHashHelper::GetMercatorXYByHash(score.value.iv, loc->x, loc->y);
            }
            return 0;
        }
        else
        {
            ValueObject vv;
            vv.key.type = ZSET_ELEMENT_VALUE;
            vv.key.db = ctx.currentDB;
            vv.key.key = meta.key.key;
            vv.key.element = value;
            vv.attach.fetch_loc = true;
            int err = GetKeyValue(ctx, vv.key, &vv);
            if (0 == err)
            {
                score = vv.score;
                if (NULL != loc)
                {
                    *loc = vv.attach.loc;
                }
            }
            return err;
        }
    }

    int Ardb::ZIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data incr;
        if (!incr.SetNumber(cmd.GetArguments()[1]))
        {
            fill_error_reply(ctx.reply, "Invalid arguments.");
            return 0;
        }
        Data oldscore;
        oldscore.SetInt64(0);
        Data element;
        element.SetString(cmd.GetArguments()[2], true);
        int ret = ZSetScore(ctx, meta, element, oldscore);
        Data newscore = oldscore;
        newscore.IncrBy(incr);
        ZSetAdd(ctx, meta, element, newscore, ret == 0 ? (&oldscore) : NULL);
        SetKeyValue(ctx, meta);
        fill_str_reply(ctx.reply, newscore.ToString());
        return 0;
    }

//    int Ardb::ZGetPrevRangeByRank(Context& ctx, ValueObject& meta, uint32 rank, double& prev_range_score,
//            uint32& prev_rank)
//    {
//        ScoreRanges::iterator it = meta.meta.zset_score_range.begin();
//        prev_range_score = meta.meta.min_index.NumberValue();
//        prev_rank = 0;
//        while (it != meta.meta.zset_score_range.end())
//        {
//            prev_rank += it->second;
//            if (prev_rank >= rank)
//            {
//                prev_rank -= it->second;
//                break;
//            }
//            prev_range_score = it->first;
//            it++;
//        }
//        return 0;
//    }

    int Ardb::ZSetRange(Context& ctx, const Slice& key, int64 start, int64 end, bool withscores, bool reverse,
            DataOperation op)
    {
        if (op != OP_GET && op != OP_DELETE)
        {
            ERROR_LOG("Invalid operation");
            return -1;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, key, ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (op == OP_GET)
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_INTEGER;
        }
        if (0 != err)
        {
            return 0;
        }
        if (start < 0)
            start = meta.meta.Length() + start;
        if (end < 0)
            end = meta.meta.Length() + end;
        if (start < 0)
            start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= meta.meta.Length())
        {
            return 0;
        }
        if (end >= meta.meta.Length())
            end = meta.meta.Length() - 1;
        uint32 count = 0;
        BatchWriteGuard guard(GetKeyValueEngine(), op == OP_DELETE);
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            ZSetElementArray array(meta.meta.zipmap.begin(), meta.meta.zipmap.end());
            std::sort(array.begin(), array.end(), less_by_zset_score);
            bool loop = true;
            int64 cursor = reverse ? end : start;
            while (loop)
            {
                if (op == OP_DELETE)
                {
                    meta.meta.zipmap.erase(array[start].first);
                    count++;
                }
                else
                {
                    RedisReply& r1 = ctx.reply.AddMember();
                    fill_str_reply(r1, array[cursor].first.ToString());
                    if (withscores)
                    {
                        RedisReply& r2 = ctx.reply.AddMember();
                        fill_str_reply(r2, array[cursor].second.ToString());
                    }
                }
                if (reverse)
                {
                    end--;
                    cursor = end;
                }
                else
                {
                    start++;
                    cursor = start;
                }
                loop = (start <= end);
            }
        }
        else
        {
            uint32 rank_cursor = 0;
            ZSetIterator iter;
            ZSetScoreIter(ctx, meta, meta.meta.min_index, iter, true);
            while (iter.Valid())
            {
                bool match_value = false;
                if (rank_cursor >= start && rank_cursor <= end)
                {
                    match_value = true;
                }
                if (rank_cursor > end)
                {
                    break;
                }
                rank_cursor++;
                if (match_value)
                {
                    if (op == OP_DELETE)
                    {
                        ZSetDeleteElement(ctx, meta, *(iter.Element()), *(iter.Score()));
                        count++;
                    }
                    else
                    {
                        if (!reverse)
                        {
                            RedisReply& r = ctx.reply.AddMember();
                            std::string tmp;
                            fill_str_reply(r, iter.Element()->GetDecodeString(tmp));
                            if (withscores)
                            {
                                RedisReply& r1 = ctx.reply.AddMember();
                                r1.type = REDIS_REPLY_STRING;
                                iter.Score()->GetDecodeString(r1.str);
                            }
                        }
                        else
                        {
                            if (withscores)
                            {
                                RedisReply& r1 = ctx.reply.AddMember(false);
                                r1.type = REDIS_REPLY_STRING;
                                iter.Score()->GetDecodeString(r1.str);
                            }
                            RedisReply& r = ctx.reply.AddMember(false);
                            std::string tmp;
                            fill_str_reply(r, iter.Element()->GetDecodeString(tmp));
                        }
                    }
                }
                iter.Next();
            }
        }
        if (op == OP_DELETE)
        {
            if (meta.meta.Length() <= 0)
            {
                DelKeyValue(ctx, meta.key);
            }
            else
            {
                SetKeyValue(ctx, meta);
            }
            fill_int_reply(ctx.reply, count);
        }
        return 0;
    }

    int Ardb::ZRange(Context& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (strcasecmp(cmd.GetArguments()[3].c_str(), "withscores") != 0)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
            withscores = true;
        }
        int64 start, stop;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], stop))
        {
            return 0;
        }
        ZSetRange(ctx, cmd.GetArguments()[0], start, stop, withscores, cmd.GetType() == REDIS_CMD_ZREVRANGE, OP_GET);
        return 0;
    }
    int Ardb::ZRevRange(Context& ctx, RedisCommandFrame& cmd)
    {
        ZRange(ctx, cmd);
        return 0;
    }

    int Ardb::ZSetDeleteElement(Context& ctx, ValueObject& meta, const Data& element, const Data& score)
    {
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            return 0;
        }
        meta.meta.len--;
        KeyObject vk;
        vk.element = element;
        vk.db = ctx.currentDB;
        vk.type = ZSET_ELEMENT_VALUE;
        vk.key = meta.key.key;
        KeyObject sk;
        sk.db = ctx.currentDB;
        sk.type = ZSET_ELEMENT_SCORE;
        sk.key = meta.key.key;
        sk.element = element;
        sk.score = score;
        DelKeyValue(ctx, sk);
        DelKeyValue(ctx, vk);
        return 0;
    }

    int Ardb::ZSetRangeByScore(Context& ctx, ValueObject& meta, const ZRangeSpec& range,
            ZSetRangeByScoreOptions& options, ZSetIterator*& iter)
    {
        uint32 count = 0, retrived = 0;
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            ZSetElementArray array(meta.meta.zipmap.begin(), meta.meta.zipmap.end());
            std::sort(array.begin(), array.end(), options.reverse ? greate_by_zset_score : less_by_zset_score);
            for (uint32 i = 0; i < array.size(); i++)
            {
                if (range.InRange(array[i].second))
                {
                    if (OP_GET == options.op)
                    {
                        if (count >= options.limit_option.offset)
                        {
                            if (options.fill_reply)
                            {
                                RedisReply& r = ctx.reply.AddMember();
                                fill_str_reply(r, array[i].first.ToString());
                                if (options.withscores)
                                {
                                    RedisReply& r1 = ctx.reply.AddMember();
                                    fill_str_reply(r1, array[i].second.ToString());
                                }
                            }
                            else
                            {
                                options.results.push_back(array[i].first);
                                if (options.withscores)
                                {
                                    options.results.push_back(array[i].second);
                                }
                                if (options.fetch_geo_location)
                                {
                                    Location loc;
                                    Data& score = array[i].second;
                                    GeoHashHelper::GetMercatorXYByHash(score.value.iv, loc.x, loc.y);
                                    options.locs.push_back(loc);
                                }
                            }
                            retrived++;
                            if (options.limit_option.limit > 0 && retrived >= options.limit_option.limit)
                            {
                                break;
                            }
                        }
                    }
                    else if (OP_DELETE == options.op)
                    {
                        meta.meta.zipmap.erase(array[i].first);
                    }
                    count++;
                }
            }
        }
        else
        {
            if (NULL == iter)
            {
                NEW(iter, ZSetIterator);
                ZSetScoreIter(ctx, meta, options.reverse ? range.max : range.min, *iter, true);
                if (options.reverse && !iter->Valid())
                {
                    iter->Prev();
                }
            }
            else
            {
                iter->SeekScore(options.reverse ? range.max : range.min);
            }
            uint32 cursor = 0;
            while (iter->Valid())
            {
                if (range.InRange(*(iter->Score())))
                {
                    if (options.op == OP_DELETE)
                    {
                        ZSetDeleteElement(ctx, meta, *(iter->Element()), *(iter->Score()));
                        count++;
                    }
                    else
                    {
                        if (cursor >= options.limit_option.offset)
                        {
                            if (options.fill_reply)
                            {
                                RedisReply& r = ctx.reply.AddMember();
                                std::string tmp;
                                fill_str_reply(r, iter->Element()->GetDecodeString(tmp));
                                if (options.withscores)
                                {
                                    RedisReply& r1 = ctx.reply.AddMember();
                                    r1.type = REDIS_REPLY_STRING;
                                    iter->Score()->GetDecodeString(r1.str);
                                }
                            }
                            else
                            {
                                options.results.push_back(*(iter->Element()));
                                if (options.withscores)
                                {
                                    options.results.push_back(*(iter->Score()));
                                }
                                if (options.fetch_geo_location)
                                {
                                    Location loc;
                                    iter->GeoLocation(loc);
                                    options.locs.push_back(loc);
                                }
                            }
                            retrived++;
                            if (options.limit_option.limit > 0 && retrived >= options.limit_option.limit)
                            {
                                break;
                            }
                        }
                    }
                    cursor++;
                }
                else
                {
                    if (options.reverse)
                    {
                        if (iter->Score()->Compare(range.min) <= 0)
                        {
                            break;
                        }
                    }
                    else
                    {
                        if (iter->Score()->Compare(range.max) >= 0)
                        {
                            break;
                        }
                    }
                }
                if (options.reverse)
                {
                    iter->Prev();
                }
                else
                {
                    iter->Next();
                }
            }
        }
        if (options.op == OP_DELETE)
        {
            if (meta.meta.Length() <= 0)
            {
                DelKeyValue(ctx, meta.key);
            }
            else
            {
                SetKeyValue(ctx, meta);
            }
            fill_int_reply(ctx.reply, count);
        }
        else if (options.op == OP_COUNT)
        {
            fill_int_reply(ctx.reply, count);
        }
        return 0;
    }

    int Ardb::ZSetRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            return 0;
        }
        ZSetRangeByScoreOptions options;
        ZRangeSpec range;

        /* Parse the range arguments. */
        if (cmd.GetType() == REDIS_CMD_ZREVRANGEBYSCORE)
        {
            if (!range.Pasrse(cmd.GetArguments()[2], cmd.GetArguments()[1]))
            {
                fill_error_reply(ctx.reply, "Invalid arguments");
                return 0;
            }
            options.reverse = true;
        }
        else
        {
            if (!range.Pasrse(cmd.GetArguments()[1], cmd.GetArguments()[2]))
            {
                fill_error_reply(ctx.reply, "Invalid arguments");
                return 0;
            }
        }

        if (cmd.GetType() == REDIS_CMD_ZREMRANGEBYSCORE)
        {
            options.op = OP_DELETE;
        }

        if (!options.Parse(cmd.GetArguments(), 3))
        {
            fill_error_reply(ctx.reply, "Invalid arguments");
            return 0;
        }
        ZSetIterator* iter = NULL;
        ZSetRangeByScore(ctx, meta, range, options, iter);
        DELETE(iter);
        return 0;
    }

    int Ardb::ZRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByScore(ctx, cmd);
        return 0;
    }

    int Ardb::ZRevRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByScore(ctx, cmd);
        return 0;
    }

    int Ardb::ZSetRank(Context& ctx, const std::string& key, const std::string& value, bool reverse)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (err != 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
            return 0;
        }
        Data element, score;
        element.SetString(value, true);
        err = ZSetScore(ctx, meta, element, score);
        if (err != 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
            return 0;
        }
        uint32 rank = 0;
        if (meta.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            DataMap::iterator it = meta.meta.zipmap.begin();
            while (it != meta.meta.zipmap.end())
            {
                if (it->second < score)
                {
                    rank++;
                }
                it++;
            }
        }
        else
        {
            if (0 != ZSetRankByScore(ctx, meta, score, rank))
            {
                ctx.reply.type = REDIS_REPLY_NIL;
                return 0;
            }
        }
        if (reverse)
        {
            rank = meta.meta.Length() - rank - 1;
        }
        fill_int_reply(ctx.reply, rank);
        return 0;
    }

    int Ardb::ZRank(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRank(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1], false);
        return 0;
    }

    int Ardb::ZRevRank(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRank(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1], true);
        return 0;
    }
    int Ardb::ZSetRem(Context& ctx, ValueObject& meta, const std::string& value)
    {
        Data element;
        element.SetString(value, true);
        if (COLLECTION_ECODING_ZIPZSET == meta.meta.encoding)
        {
            return meta.meta.zipmap.erase(element);
        }
        else
        {
            ValueObject vv;
            vv.key.element = element;
            vv.key.db = ctx.currentDB;
            vv.key.type = ZSET_ELEMENT_VALUE;
            vv.key.key = meta.key.key;
            if (0 == GetKeyValue((ctx), vv.key, &vv))
            {
                ZSetDeleteElement(ctx, meta, element, vv.score);
                return 1;
            }
            return 0;
        }
    }
    int Ardb::ZRem(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (err != 0)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        uint32 count = 0;
        BatchWriteGuard guard(GetKeyValueEngine(), COLLECTION_ECODING_ZIPZSET != meta.meta.encoding);
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            count += ZSetRem(ctx, meta, cmd.GetArguments()[i]);
        }
        if (meta.meta.Length() == 0)
        {
            DelKeyValue((ctx), meta.key);
        }
        else
        {
            SetKeyValue((ctx), meta);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::ZRemRangeByRank(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, stop;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], stop))
        {
            return 0;
        }
        ZSetRange(ctx, cmd.GetArguments()[0], start, stop, false, false, OP_DELETE);
        return 0;
    }

    int Ardb::ZRemRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByScore(ctx, cmd);
        return 0;
    }

    int Ardb::ZInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetMergeOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid argument");
            return 0;
        }
        DeleteKey(ctx, cmd.GetArguments()[0]);
        ValueObject meta;
        meta.type = ZSET_META;
        meta.key.db = ctx.currentDB;
        meta.key.type = KEY_META;
        meta.key.key = cmd.GetArguments()[0];
        meta.meta.encoding = COLLECTION_ECODING_ZIPZSET;

        fill_int_reply(ctx.reply, 0); //default reply value

        ValueObjectArray metas;
        StringArray::iterator sit = options.keys.begin();
        while (sit != options.keys.end())
        {
            ValueObject kmeta;
            int err = GetMetaValue(ctx, *sit, ZSET_META, kmeta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (0 == err)
            {
                metas.push_back(kmeta);
            }
            else
            {
                metas.clear();
                break;
            }
            sit++;
        }
        if (metas.empty())
        {
            return 0;
        }
        ZSetIterator iter;
        Data empty;
        ZSetValueIter(ctx, metas[0], empty, iter, true);

        DataMap tmpmap;
        while (iter.Valid())
        {
            bool match = true;
            Data new_score = *(iter.Score());
            new_score.SetDouble(new_score.NumberValue() * options.weights[0]);
            for (uint32 i = 1; i < metas.size(); i++)
            {
                Data score;
                if (0 != ZSetScore(ctx, metas[i], *(iter.Element()), score))
                {
                    match = false;
                    break;
                }
                score.SetDouble(score.NumberValue() * options.weights[i]);
                switch (options.aggregate)
                {
                    case AGGREGATE_MAX:
                    {
                        if (score > new_score)
                        {
                            new_score = score;
                        }
                        break;
                    }
                    case AGGREGATE_MIN:
                    {
                        if (score < new_score)
                        {
                            new_score = score;
                        }
                        break;
                    }
                    case AGGREGATE_SUM:
                    {
                        new_score.IncrBy(score);
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
            }
            if (match)
            {
                ZSetAdd(ctx, meta, *(iter.Element()), new_score, NULL);
            }
            iter.Next();
        }
        if (meta.meta.Length() > 0)
        {
            SetKeyValue(ctx, meta);
        }
        fill_int_reply(ctx.reply, meta.meta.Length());
        return 0;
    }

    int Ardb::ZUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetMergeOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid argument");
            return 0;
        }
        DeleteKey(ctx, cmd.GetArguments()[0]);
        ValueObject meta;
        meta.type = ZSET_META;
        meta.meta.encoding = COLLECTION_ECODING_ZIPZSET;
        meta.key.db = ctx.currentDB;
        meta.key.type = KEY_META;
        meta.key.key = cmd.GetArguments()[0];

        ValueObjectArray metas;
        StringArray::iterator sit = options.keys.begin();
        while (sit != options.keys.end())
        {
            ValueObject kmeta;
            int err = GetMetaValue(ctx, *sit, ZSET_META, kmeta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (0 == err)
            {
                metas.push_back(kmeta);
            }
            sit++;
        }

        DataMap tmpmap;
        for (uint32 i = 0; i < metas.size(); i++)
        {
            ZSetIterator iter;
            ZSetScoreIter(ctx, metas[i], metas[i].meta.min_index, iter, true);
            while (iter.Valid())
            {
                Data new_score;
                double tmp = iter.Score()->NumberValue() * options.weights[i];
                new_score.SetDouble(tmp);
                DataMap::iterator found = tmpmap.find(*iter.Element());
                if (found != tmpmap.end())
                {
                    switch (options.aggregate)
                    {
                        case AGGREGATE_MAX:
                        {
                            if (new_score > found->second)
                            {
                                found->second = new_score;
                            }
                            break;
                        }
                        case AGGREGATE_MIN:
                        {
                            if (new_score < found->second)
                            {
                                found->second = new_score;
                            }
                            break;
                        }
                        case AGGREGATE_SUM:
                        {
                            found->second.IncrBy(new_score);
                            break;
                        }
                        default:
                        {
                            break;
                        }
                    }
                }
                else
                {
                    tmpmap[*iter.Element()] = new_score;
                }
                iter.Next();
            }
        }
        BatchWriteGuard guard(GetKeyValueEngine());
        DataMap::iterator tit = tmpmap.begin();
        while (tit != tmpmap.end())
        {
            ZSetAdd(ctx, meta, tit->first, tit->second, NULL);
            tit++;
        }
        SetKeyValue(ctx, meta);
        fill_int_reply(ctx.reply, meta.meta.Length());
        return 0;
    }

    int Ardb::ZScan(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string pattern;
        uint32 limit = 10000; //return max 10000 keys one time
        if (cmd.GetArguments().size() > 2)
        {
            for (uint32 i = 2; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "count"))
                {
                    if (i + 1 >= cmd.GetArguments().size() || !string_touint32(cmd.GetArguments()[i + 1], limit))
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    i++;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "match"))
                {
                    if (i + 1 >= cmd.GetArguments().size())
                    {
                        fill_error_reply(ctx.reply, "'MATCH' need one args followed");
                        return 0;
                    }
                    pattern = cmd.GetArguments()[i + 1];
                    i++;
                }
                else
                {
                    fill_error_reply(ctx.reply, "Syntax error, try scan 0 ");
                    return 0;
                }
            }
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);

        const std::string& cursor = cmd.GetArguments()[1];
        ZSetIterator iter;
        Data st;
        if (cursor == "0")
        {
            st.SetDouble(-DBL_MAX);
        }
        else
        {
            st.SetNumber(cursor);
        }
        err = ZSetScoreIter(ctx, meta, st, iter, true);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        RedisReply& r1 = ctx.reply.AddMember();
        RedisReply& r2 = ctx.reply.AddMember();
        r2.type = REDIS_REPLY_ARRAY;
        if (err != 0)
        {
            fill_str_reply(r1, "0");
        }
        else
        {
            while (iter.Valid())
            {
                const Data* field = iter.Element();
                std::string tmp;
                field->GetDecodeString(tmp);
                if ((pattern.empty() || fnmatch(pattern.c_str(), tmp.c_str(), 0) == 0))
                {
                    RedisReply& rr1 = r2.AddMember();
                    fill_str_reply(rr1, tmp);
                    RedisReply& rr2 = r2.AddMember();
                    rr2.type = REDIS_REPLY_STRING;
                    iter.Score()->GetDecodeString(rr2.str);
                }
                if (r2.MemberSize() >= (limit * 2))
                {
                    break;
                }
                iter.Next();
            }
            if (iter.Valid())
            {
                iter.Next();
                const Data* next_field = iter.Score();
                std::string tmp;
                fill_str_reply(r1, next_field->GetDecodeString(tmp));
            }
            else
            {
                fill_str_reply(r1, "0");
            }
        }
        return 0;
    }

    int Ardb::ZScore(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (err != 0)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        Data element, score;
        element.SetString(cmd.GetArguments()[1], true);
        err = ZSetScore(ctx, meta, element, score);
        if (0 == err)
        {
            //fill_double_reply(ctx.reply, score.NumberValue());
            fill_str_reply(ctx.reply, score.ToString());
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        return 0;
    }

    int Ardb::ZClear(Context& ctx, ValueObject& meta)
    {
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.encoding != COLLECTION_ECODING_ZIPZSET);
        if (meta.meta.encoding != COLLECTION_ECODING_ZIPZSET)
        {
            ZSetIterator iter;
            meta.meta.len = 0;
            Data min = meta.meta.min_index;
            ZSetScoreIter(ctx, meta, min, iter, false);
            while (iter.Valid())
            {
                KeyObject fk;
                fk.db = ctx.currentDB;
                fk.key = meta.key.key;
                fk.type = ZSET_ELEMENT_VALUE;
                fk.element = *(iter.Element());
                DelKeyValue(ctx, fk);
                KeyObject sk;
                sk.db = ctx.currentDB;
                sk.key = meta.key.key;
                sk.type = ZSET_ELEMENT_SCORE;
                sk.element = *(iter.Element());
                sk.score = *(iter.Score());
                DelKeyValue((ctx), sk);
                iter.Next();
            }
        }
        DelKeyValue((ctx), meta.key);
        return 0;
    }

    int Ardb::ZSetRangeByLex(Context& ctx, const std::string& key, const ZSetRangeByLexOptions& options,
            DataOperation op)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        switch (op)
        {
            case OP_DELETE:
            case OP_COUNT:
            {
                fill_int_reply(ctx.reply, 0);
                break;
            }
            case OP_GET:
            {
                ctx.reply.type = REDIS_REPLY_ARRAY;
                break;
            }
            default:
            {
                break;
            }
        }
        if (0 != err)
        {
            return 0;
        }
        Data from;
        from.SetString(options.range_option.min, false);
        ZSetIterator iter;
        ZSetValueIter(ctx, meta, from, iter, op != OP_DELETE);
        BatchWriteGuard guard(GetKeyValueEngine(), op == OP_DELETE);
        uint32 cursor = 0;
        while (iter.Valid())
        {
            std::string tmp;
            if (options.range_option.InRange(iter.Element()->GetDecodeString(tmp)))
            {
                switch (op)
                {
                    case OP_DELETE:
                    {
                        ctx.reply.integer++;
                        ZSetRem(ctx, meta, tmp);
                        break;
                    }
                    case OP_COUNT:
                    {
                        ctx.reply.integer++;
                        break;
                    }
                    case OP_GET:
                    default:
                    {
                        if (cursor >= options.limit_option.offset)
                        {
                            RedisReply& r = ctx.reply.AddMember();
                            fill_str_reply(r, tmp);
                            if (options.limit_option.limit > 0)
                            {
                                if (ctx.reply.MemberSize() >= options.limit_option.limit)
                                {
                                    return 0;
                                }
                            }
                        }
                        break;
                    }
                }
                cursor++;
            }
            else
            {
                if (tmp >= options.range_option.max)
                {
                    break;
                }
            }
            iter.Next();
        }
        if (op == OP_DELETE)
        {
            SetKeyValue(ctx, meta);
        }
        return 0;
    }

    int Ardb::ZLexCount(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByLexOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid arguments");
            return 0;
        }
        ZSetRangeByLex(ctx, cmd.GetArguments()[0], options, OP_COUNT);
        return 0;
    }

    int Ardb::ZRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByLexOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid arguments");
            return 0;
        }
        ZSetRangeByLex(ctx, cmd.GetArguments()[0], options, OP_GET);
        return 0;
    }

    int Ardb::ZRemRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        ZSetRangeByLexOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid arguments");
            return 0;
        }
        ZSetRangeByLex(ctx, cmd.GetArguments()[0], options, OP_DELETE);
        return 0;
    }

    int Ardb::RenameZSet(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey)
    {
        Context tmpctx;
        tmpctx.currentDB = srcdb;
        ValueObject v;
        int err = GetMetaValue(tmpctx, srckey, ZSET_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            return 0;
        }
        if (v.meta.encoding == COLLECTION_ECODING_ZIPZSET)
        {
            DelKeyValue(tmpctx, v.key);
            v.key.encode_buf.Clear();
            v.key.db = dstdb;
            v.meta.expireat = 0;
            SetKeyValue(ctx, v);
        }
        else
        {
            ZSetIterator iter;
            Data from;
            ZSetScoreIter(ctx, v, from, iter, false);
            tmpctx.currentDB = dstdb;
            ValueObject dstmeta;
            dstmeta.key.type = KEY_META;
            dstmeta.key.key = dstkey;
            dstmeta.type = ZSET_META;
            dstmeta.meta.encoding = COLLECTION_ECODING_ZIPZSET;
            BatchWriteGuard guard(GetKeyValueEngine());
            while (iter.Valid())
            {
                ZSetAdd(tmpctx, dstmeta, *(iter.Element()), *(iter.Score()), NULL);
                iter.Next();
            }
            SetKeyValue(tmpctx, dstmeta);
            tmpctx.currentDB = srcdb;
            DeleteKey(tmpctx, srckey);
        }
        ctx.data_change = true;
        return 0;
    }

OP_NAMESPACE_END

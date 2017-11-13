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
#include "db/db.hpp"
#include <float.h>
#include <cmath>

/* This generic command implements both ZADD and ZINCRBY. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */
#define ZADD_CH (1<<3)      /* Return num of elements added or updated. */

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3

OP_NAMESPACE_BEGIN

    int Ardb::ZAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        int flags = cmd.GetType() == REDIS_CMD_ZINCRBY ? ZADD_INCR : ZADD_NONE;
        int added = 0; /* Number of new elements added. */
        int updated = 0; /* Number of elements with updated score. */
        int processed = 0; /* Number of elements processed, may remain zero with
         options like XX. */
        size_t scoreidx = 1;
        int err = 0;
        while (scoreidx < cmd.GetArguments().size())
        {
            const char* opt = cmd.GetArguments()[scoreidx].c_str();
            if (!strcasecmp(opt, "nx"))
                flags |= ZADD_NX;
            else if (!strcasecmp(opt, "xx"))
                flags |= ZADD_XX;
            else if (!strcasecmp(opt, "ch"))
                flags |= ZADD_CH;
            else if (!strcasecmp(opt, "incr"))
                flags |= ZADD_INCR;
            else
                break;
            scoreidx++;
        }
        /* Turn options into simple to check vars. */
        int incr = (flags & ZADD_INCR) != 0;
        int nx = (flags & ZADD_NX) != 0;
        int xx = (flags & ZADD_XX) != 0;
        int ch = (flags & ZADD_CH) != 0;
        RedisReply& reply = ctx.GetReply();
        size_t elements = cmd.GetArguments().size() - scoreidx;
        if (elements % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        if (nx && xx)
        {
            reply.SetErrorReason("XX and NX options at the same time are not compatible");
            return 0;
        }
        elements /= 2; /* Now this holds the number of score-element pairs. */
        if (incr && elements > 1)
        {
            reply.SetErrorReason("INCR option supports a single increment-element pair");
            return 0;
        }
        std::vector<double> scores;
        for (size_t i = 0; i < elements; i++)
        {
            const std::string& scorestr = cmd.GetArguments()[scoreidx + i * 2];
            double score;
            if (!string_todouble(scorestr, score))
            {
                reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
                return 0;
            }
            scores.push_back(score);
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(ctx, key);
        if (!CheckMeta(ctx, key, KEY_ZSET, meta))
        {
            return 0;
        }

        if (meta.GetType() == 0)
        {
            if (xx)
            {
                return 0;
            }
            else
            {
                meta.SetType(KEY_ZSET);
                meta.SetObjectLen(0);
            }
        }
        double score = 0;
        {
            WriteBatchGuard batch(ctx, m_engine);
            for (size_t i = 0; i < elements; i++)
            {
                KeyObject ele(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
                ele.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
                score = scores[i];
                double current_score = 0;
                ValueObject ele_value;
                if (0 == m_engine->Get(ctx, ele, ele_value))
                {
                    if (nx)
                    {
                        continue;
                    }
                    current_score = ele_value.GetZSetScore();
                    if (incr)
                    {
                        score += current_score;
                        if (std::isnan(score))
                        {
                            batch.MarkFailed(ERR_SCORE_NAN);
                            break;
                        }
                    }
                    processed++;
                    if (score != current_score)
                    {
                        KeyObject old_sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
                        old_sort_key.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
                        old_sort_key.SetZSetScore(current_score);
                        RemoveKey(ctx, old_sort_key);
                        updated++;
                    }
                    else
                    {
                        continue;
                    }
                }
                else
                {
                    if (xx)
                    {
                        continue;
                    }
                    added++;
                    processed++;
                }
                KeyObject new_sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
                new_sort_key.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
                new_sort_key.SetZSetScore(score);
                ValueObject empty;
                empty.SetType(KEY_ZSET_SORT);
                SetKeyValue(ctx, new_sort_key, empty);
                ele_value.SetType(KEY_ZSET_SCORE);
                ele_value.SetZSetScore(score);
                SetKeyValue(ctx, ele, ele_value);
                meta.SetMinMaxData(new_sort_key.GetZSetMember());
            }
            meta.SetObjectLen(meta.GetObjectLen() + added);
            SetKeyValue(ctx, key, meta);
        }

        if (ctx.transc_err != 0)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            if (incr)
            {
                if (processed)
                {
                    reply.SetDouble(score);
                }
                else
                {
                    reply.Clear();
                }
            }
            else
            {
                reply.SetInteger(ch ? added + updated : added);
            }
        }
        return 0;
    }

    int Ardb::ZCard(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_ZSET, cmd.GetArguments()[0]);
    }

    int Ardb::ZCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByScore(ctx, cmd);
    }

    int Ardb::ZIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZAdd(ctx, cmd);
    }

    int Ardb::ZIterateByRank(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (strcasecmp(cmd.GetArguments()[3].c_str(), "withscores") != 0)
            {
                reply.SetErrorReason("syntax error");
                return 0;
            }
            withscores = true;
        }
        bool reverse = cmd.GetType() == REDIS_CMD_ZREVRANGE;
        bool toremove = cmd.GetType() == REDIS_CMD_ZREMRANGEBYRANK;
        int64_t removed = 0;
        int64 start, end;
        if (!string_toint64(cmd.GetArguments()[1], start) || !string_toint64(cmd.GetArguments()[2], end))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        if (toremove)
        {
            reply.SetInteger(0);
        }
        else
        {
            reply.ReserveMember(0); //default response
        }

        if (!CheckMeta(ctx, key, KEY_ZSET, meta) || meta.GetType() == 0)
        {
            return 0;
        }
        if (start < 0)
            start = meta.GetObjectLen() + start;
        if (end < 0)
            end = meta.GetObjectLen() + end;
        if (start < 0)
            start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= meta.GetObjectLen())
        {
            return 0;
        }
        if (end >= meta.GetObjectLen())
            end = meta.GetObjectLen() - 1;
        KeyObject sort_key(ctx.ns, KEY_ZSET_SORT, key.GetKey());
        if(reverse)
        {
            ctx.flags.iterate_total_order = 1;
        }
        Iterator* iter = m_engine->Find(ctx, sort_key);
        if (reverse)
        {
            iter->JumpToLast();
        }
        int64_t rank = 0;
        while (iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_ZSET_SORT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
            {
                break;
            }
            if (rank > end)
            {
                break;
            }
            if (rank >= start && rank <= end)
            {
                if (toremove)
                {
                    KeyObject score_key(ctx.ns, KEY_ZSET_SCORE, key.GetKey());
                    score_key.SetZSetMember(field.GetZSetMember());
                    //RemoveKey(ctx, field);
                    RemoveKey(ctx, score_key);
                    iter->Del();
                    removed++;
                }
                else
                {
                    RedisReply& r1 = reply.AddMember();
                    r1.SetString(field.GetZSetMember());
                    if (withscores)
                    {
                        RedisReply& r2 = reply.AddMember();
                        r2.SetDouble(field.GetZSetScore());
                    }
                }
            }

            if (reverse)
            {
                iter->Prev();
            }
            else
            {
                iter->Next();

            }
            rank++;
        }
        DELETE(iter);
        if (toremove)
        {
            if (removed > 0)
            {
                meta.SetObjectLen(meta.GetObjectLen() - removed);
                if (meta.GetObjectLen() == 0)
                {
                    RemoveKey(ctx, key);
                }
                else
                {
                    SetKeyValue(ctx, key, meta);
                }
            }
            reply.SetInteger(removed);
        }
        return 0;
    }

    int Ardb::ZRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByRank(ctx, cmd);
    }
    int Ardb::ZRevRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByRank(ctx, cmd);
    }

    int Ardb::ZIterateByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool reverse = cmd.GetType() == REDIS_CMD_ZREVRANGEBYSCORE;
        bool toremove = cmd.GetType() == REDIS_CMD_ZREMRANGEBYSCORE;
        bool countrange = cmd.GetType() == REDIS_CMD_ZCOUNT;
        bool withscores = false;
        ZRangeSpec range;
        bool range_parse_success = false;
        if (reverse)
        {
            range_parse_success = range.Parse(cmd.GetArguments()[2], cmd.GetArguments()[1]);
        }
        else
        {
            range_parse_success = range.Parse(cmd.GetArguments()[1], cmd.GetArguments()[2]);
        }
        if (!range_parse_success)
        {
            reply.SetErrorReason("min or max is not a float");
            return 0;
        }
        int64_t removed = 0;
        bool with_limit = false;
        int64_t limit_offset = 0, limit_count = -1;
        if (cmd.GetArguments().size() >= 4)
        {
            for (size_t i = 3; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "withscores"))
                {
                    withscores = true;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit") && (i + 2) < cmd.GetArguments().size())
                {
                    with_limit = true;
                    if (!string_toint64(cmd.GetArguments()[i + 1], limit_offset) || !string_toint64(cmd.GetArguments()[i + 2], limit_count))
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    i += 2;
                }
                else
                {
                    reply.SetErrorReason("syntax error");
                    return 0;
                }
            }
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        if (toremove || countrange)
        {
            reply.SetInteger(0);
        }
        else
        {
            reply.ReserveMember(0); //default response
        }
        if (!CheckMeta(ctx, key, KEY_ZSET, meta) || meta.GetType() == 0)
        {
            return 0;
        }
        KeyObject sort_key(ctx.ns, KEY_ZSET_SORT, key.GetKey());
        sort_key.SetZSetScore(reverse ? range.max.GetFloat64() : range.min.GetFloat64());
        if(reverse)
        {
            ctx.flags.iterate_total_order = 1;
        }
        Iterator* iter = m_engine->Find(ctx, sort_key);
        if (reverse && !iter->Valid())
        {
            iter->JumpToLast();
        }
        int64_t range_cursor = 0;
        int64_t range_count = 0;
        while (iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_ZSET_SORT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
            {
                break;
            }
            int inrange = range.InRange(field.GetZSetScore());
            if (reverse)
            {
                if (inrange < 0)
                {
                    break;
                }
            }
            else
            {
                if (inrange > 0)
                {
                    break;
                }
            }
            if (0 == inrange)
            {
                if (!with_limit || (range_cursor >= limit_offset))
                {
                    if (toremove)
                    {
                        KeyObject score_key(ctx.ns, KEY_ZSET_SCORE, key.GetKey());
                        score_key.SetZSetMember(field.GetZSetMember());
                        //RemoveKey(ctx, field);
                        RemoveKey(ctx, score_key);
                        iter->Del();
                        removed++;
                    }
                    else if (!countrange)
                    {
                        RedisReply& r1 = reply.AddMember();
                        r1.SetString(field.GetZSetMember());
                        if (withscores)
                        {
                            RedisReply& r2 = reply.AddMember();
                            r2.SetDouble(field.GetZSetScore());
                        }
                    }
                    range_count++;
                    if (with_limit && range_count >= limit_count)
                    {
                        break;
                    }
                }
                range_cursor++;
            }
            if (reverse)
            {
                iter->Prev();
            }
            else
            {
                iter->Next();
            }
        }
        DELETE(iter);
        if (toremove)
        {
            if (removed > 0)
            {
                meta.SetObjectLen(meta.GetObjectLen() - removed);
                if (meta.GetObjectLen() == 0)
                {
                    RemoveKey(ctx, key);
                }
                else
                {
                    SetKeyValue(ctx, key, meta);
                }
            }
            reply.SetInteger(removed);
        }
        else if (countrange)
        {
            reply.SetInteger(range_count);
        }
        return 0;
    }

    int Ardb::ZRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByScore(ctx, cmd);
    }

    int Ardb::ZRevRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByScore(ctx, cmd);
    }

    int Ardb::ZRemRangeByRank(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByRank(ctx, cmd);
    }

    int Ardb::ZRemRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByScore(ctx, cmd);
    }

    int Ardb::ZRank(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ZScore(ctx, cmd);
        if (reply.type == REDIS_REPLY_DOUBLE)
        {
            double score = reply.GetDouble();
            Data member;
            member.SetString(cmd.GetArguments()[1], false);
            KeyObject sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
            if (cmd.GetType() == REDIS_CMD_ZREVRANK)
            {
                ctx.flags.iterate_total_order = 1;
            }
            Iterator* iter = m_engine->Find(ctx, sort_key);
            if (cmd.GetType() == REDIS_CMD_ZREVRANK)
            {
                iter->JumpToLast();
            }
            int64_t rank = 0;
            bool found = false;
            while (iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_ZSET_SORT || field.GetNameSpace() != sort_key.GetNameSpace() || field.GetKey() != sort_key.GetKey())
                {
                    break;
                }
                if (field.GetZSetMember() == member)
                {
                    found = true;
                    break;
                }
                rank++;
                if (cmd.GetType() == REDIS_CMD_ZREVRANK)
                {
                    iter->Prev();
                }
                else
                {
                    iter->Next();
                }
            }
            DELETE(iter);
            if (!found)
            {
                reply.Clear();
            }
            else
            {
                reply.SetInteger(rank);
            }
        }
        return 0;
    }

    int Ardb::ZRevRank(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZRank(ctx, cmd);
    }

    int Ardb::ZRem(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(0); //default response
        KeyObjectArray keys;
        ValueObjectArray vs;
        ErrCodeArray errs;
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        keys.push_back(key);
        for (size_t i = 1; i < cmd.GetArguments().size(); i++)
        {
            KeyObject score_key(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
            score_key.SetZSetMember(cmd.GetArguments()[i]);
            keys.push_back(score_key);
        }
        m_engine->MultiGet(ctx, keys, vs, errs);
        if (!CheckMeta(ctx, keys[0], KEY_ZSET, vs[0], false))
        {
            return 0;
        }
        if (vs[0].GetType() == 0)
        {
            return 0;
        }
        int64_t removed = 0;
        {
            WriteBatchGuard batch(ctx, m_engine);
            for (size_t i = 1; i < vs.size(); i++)
            {
                if (vs[i].GetType() == KEY_ZSET_SCORE)
                {
                    KeyObject sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
                    sort_key.SetZSetMember(keys[i].GetZSetMember());
                    sort_key.SetZSetScore(vs[i].GetZSetScore());
                    RemoveKey(ctx, sort_key);
                    RemoveKey(ctx, keys[i]);
                    removed++;
                }
            }
            if (removed > 0)
            {
                vs[0].SetObjectLen(vs[0].GetObjectLen() - removed);
                SetKeyValue(ctx, keys[0], vs[0]);
            }
        }
        if (0 != ctx.transc_err)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetInteger(removed);
        }
        return 0;
    }

    int Ardb::ZScore(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject score_key(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
        score_key.SetZSetMember(cmd.GetArguments()[1]);
        ValueObject score;
        RedisReply& reply = ctx.GetReply();
        int err = m_engine->Get(ctx, score_key, score);
        if (0 != err)
        {
            if (err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.Clear();
            }
        }
        else
        {
            reply.SetDouble(score.GetZSetScore());
        }
        return 0;
    }

    int Ardb::ZIterateByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool reverse = cmd.GetType() == REDIS_CMD_ZREVRANGEBYLEX;
        bool toremove = cmd.GetType() == REDIS_CMD_ZREMRANGEBYLEX;
        bool countrange = cmd.GetType() == REDIS_CMD_ZLEXCOUNT;
        ZLexRangeSpec range;
        bool range_parse_success = false;
        if (reverse)
        {
            range_parse_success = range.Parse(cmd.GetArguments()[2], cmd.GetArguments()[1]);
        }
        else
        {
            range_parse_success = range.Parse(cmd.GetArguments()[1], cmd.GetArguments()[2]);
        }
        if (!range_parse_success)
        {
            reply.SetErrorReason("min or max is not valid");
            return 0;
        }
        int64_t removed = 0;
        bool with_limit = false;
        int64_t limit_offset = 0, limit_count = -1;
        if (cmd.GetArguments().size() >= 4)
        {
            for (size_t i = 3; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit") && (i + 2) < cmd.GetArguments().size())
                {
                    with_limit = true;
                    if (!string_toint64(cmd.GetArguments()[i + 1], limit_offset) || !string_toint64(cmd.GetArguments()[i + 2], limit_count))
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    i += 2;
                }
                else
                {
                    reply.SetErrorReason("syntax error");
                    return 0;
                }
            }
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        if (toremove || countrange)
        {
            reply.SetInteger(0);
        }
        else
        {
            reply.ReserveMember(0); //default response
        }
        if (!CheckMeta(ctx, key, KEY_ZSET, meta) || meta.GetType() == 0)
        {
            return 0;
        }
        KeyObject sort_key(ctx.ns, KEY_ZSET_SCORE, key.GetKey());
        sort_key.SetZSetMember(reverse ? range.max : range.min);
        if(reverse)
        {
            ctx.flags.iterate_total_order = 1;
        }
        Iterator* iter = m_engine->Find(ctx, sort_key);
        if (reverse && !iter->Valid())
        {
            iter->JumpToLast();
        }
        int64_t range_cursor = 0;
        int64_t range_count = 0;
        while (iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_ZSET_SCORE || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
            {
                break;
            }
            std::string member_str;
            field.GetZSetMember().ToString(member_str);
            int inrange = range.InRange(member_str);
            if (reverse)
            {
                if (inrange < 0)
                {
                    break;
                }
            }
            else
            {
                if (inrange > 0)
                {
                    break;
                }
            }
            if (0 == inrange)
            {
                if (!with_limit || (range_cursor >= limit_offset))
                {
                    if (toremove)
                    {
                        KeyObject sort_key(ctx.ns, KEY_ZSET_SORT, key.GetKey());
                        sort_key.SetZSetMember(field.GetZSetMember());
                        sort_key.SetZSetScore(iter->Value().GetZSetScore());
                        RemoveKey(ctx, sort_key);
                        iter->Del();
                        removed++;
                    }
                    else if (!countrange)
                    {
                        RedisReply& r1 = reply.AddMember();
                        r1.SetString(field.GetZSetMember());
                    }
                    range_count++;
                    if (with_limit && range_count >= limit_count)
                    {
                        break;
                    }
                }
                range_cursor++;
            }
            if (reverse)
            {
                iter->Prev();
            }
            else
            {
                iter->Next();
            }
        }
        DELETE(iter);
        if (toremove)
        {
            if (removed > 0)
            {
                meta.SetObjectLen(meta.GetObjectLen() - removed);
                if (meta.GetObjectLen() == 0)
                {
                    RemoveKey(ctx, key);
                }
                else
                {
                    SetKeyValue(ctx, key, meta);
                }
            }
            reply.SetInteger(removed);
        }
        else if (countrange)
        {
            reply.SetInteger(range_count);
        }
        return 0;
    }

    int Ardb::ZLexCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByLex(ctx, cmd);
    }

    int Ardb::ZRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByLex(ctx, cmd);
    }

    int Ardb::ZRemRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZIterateByLex(ctx, cmd);
    }

    inline static void zunionInterAggregate(double *target, double val, int aggregate)
    {
        if (aggregate == REDIS_AGGR_SUM)
        {
            *target = *target + val;
            /* The result of adding two doubles is NaN when one variable
             * is +inf and the other is -inf. When these numbers are added,
             * we maintain the convention of the result being 0.0. */
            if (std::isnan(*target))
                *target = 0.0;
        }
        else if (aggregate == REDIS_AGGR_MIN)
        {
            *target = val < *target ? val : *target;
        }
        else if (aggregate == REDIS_AGGR_MAX)
        {
            *target = val > *target ? val : *target;
        }
        else
        {
            /* safety net */
            //serverPanic("Unknown ZUNION/INTER aggregate type");
        }
    }
    int Ardb::ZInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        uint32 setnum;
        if (!string_touint32(cmd.GetArguments()[1], setnum))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        if (setnum < 1)
        {
            reply.SetErrorReason("at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
            return 0;
        }
        if (setnum > cmd.GetArguments().size() - 2)
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        int aggregate = REDIS_AGGR_SUM;
        std::vector<double> weights;
        weights.assign(setnum, 1.0);
        size_t arg_cursor = setnum + 2;
        if (cmd.GetArguments().size() > arg_cursor)
        {
            int remaining = cmd.GetArguments().size() - arg_cursor;
            while (remaining)
            {
                if (remaining >= (setnum + 1) && !strcasecmp(cmd.GetArguments()[arg_cursor].c_str(), "weights"))
                {
                    arg_cursor++;
                    remaining--;
                    for (size_t i = 0; i < setnum; i++, arg_cursor++, remaining--)
                    {
                        if (!string_todouble(cmd.GetArguments()[arg_cursor], weights[i]))
                        {
                            reply.SetErrorReason("weight value is not a float");
                            return 0;
                        }
                    }
                }
                else if (remaining >= 2 && !strcasecmp(cmd.GetArguments()[arg_cursor].c_str(), "aggregate"))
                {
                    arg_cursor++;
                    remaining--;
                    if (!strcasecmp(cmd.GetArguments()[arg_cursor].c_str(), "sum"))
                    {
                        aggregate = REDIS_AGGR_SUM;
                    }
                    else if (!strcasecmp(cmd.GetArguments()[arg_cursor].c_str(), "min"))
                    {
                        aggregate = REDIS_AGGR_MIN;
                    }
                    else if (!strcasecmp(cmd.GetArguments()[arg_cursor].c_str(), "max"))
                    {
                        aggregate = REDIS_AGGR_MAX;
                    }
                    else
                    {
                        reply.SetErrCode(ERR_INVALID_SYNTAX);
                        return 0;
                    }
                    arg_cursor++;
                    remaining--;
                }
                else
                {
                    reply.SetErrCode(ERR_INVALID_SYNTAX);
                    return 0;
                }
            }
        }

        KeyObjectArray keys;
        ValueObjectArray vs;
        ErrCodeArray errs;
        KeyObject destkey(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        for (size_t i = 0; i < setnum; i++)
        {
            KeyObject zkey(ctx.ns, KEY_META, cmd.GetArguments()[i + 2]);
            keys.push_back(zkey);
        }
        keys.push_back(destkey);
        KeysLockGuard guard(ctx, keys);
        m_engine->MultiGet(ctx, keys, vs, errs);
        for (size_t i = 0; i < setnum; i++)
        {
            if (!CheckMeta(ctx, keys[i], KEY_ZSET, vs[i], false) && !CheckMeta(ctx, keys[i], KEY_SET, vs[i], false))
            {
                return 0;
            }
        }
        DataScoreMap inter_union_result[2];
        size_t result_cursor = 1;
        if (cmd.GetType() == REDIS_CMD_ZINTERSTORE)
        {
            bool empty_inter_result = false;
            PointerArray<Iterator*> iters;
            iters.resize(setnum);
            Data min, max;
            for (size_t i = 0; !empty_inter_result && i < setnum; i++)
            {
                if (vs[i].GetType() == 0)
                {
                    empty_inter_result = true;
                    break;
                }
                GetMinMax(ctx, keys[i], vs[i], iters[i]);
                if (min.IsNil() || vs[i].GetMin() > min)
                {
                    min = vs[i].GetMin();
                }
                if (max.IsNil() || vs[i].GetMax() < max)
                {
                    max = vs[i].GetMax();
                }
                if (min > max)
                {
                    empty_inter_result = true;
                    break;
                }
            }
            for (size_t i = 0; !empty_inter_result && i < setnum; i++)
            {
                KeyObject start(ctx.ns, (KeyType) element_type((KeyType) vs[i].GetType()), keys[i].GetKey());
                start.SetSetMember(min);
                iters[i]->Jump(start);
                result_cursor = 1 - result_cursor;
                inter_union_result[result_cursor].clear();
                while (iters[i]->Valid())
                {
                    KeyObject& k = iters[i]->Key(true);
                    if (k.GetType() != start.GetType() || k.GetKey() != keys[i].GetKey() || k.GetNameSpace() != keys[i].GetNameSpace()
                            || k.GetSetMember() > max)
                    {
                        break;
                    }
                    double score = 1.0;
                    if (k.GetType() == KEY_ZSET_SCORE)
                    {
                        score = iters[i]->Value().GetZSetScore();
                    }
                    score = weights[i] * score;
                    if (i == 0)
                    {
                        inter_union_result[result_cursor][k.GetElement(0)] = score;
                    }
                    else
                    {
                        DataScoreMap& last_result = inter_union_result[1 - result_cursor];
                        DataScoreMap::iterator found = last_result.find(k.GetElement(0));
                        if (found != last_result.end())
                        {
                            zunionInterAggregate(&score, found->second, aggregate);
                            inter_union_result[result_cursor][k.GetElement(0)] = score;
                            last_result.erase(found);
                        }
                    }
                    iters[i]->Next();
                }
            }
        }
        else
        {
            Iterator* iter = NULL;
            ctx.flags.iterate_no_upperbound = 1;
            for (size_t i = 0; i < setnum; i++)
            {
                if (vs[i].GetType() == 0)
                {
                    continue;
                }
                KeyObject ele(ctx.ns, (KeyType) element_type((KeyType) vs[i].GetType()), keys[i].GetKey());
                if (NULL != iter)
                {
                    iter->Jump(ele);
                }
                else
                {
                    iter = m_engine->Find(ctx, ele);
                }
                while (NULL != iter && iter->Valid())
                {
                    KeyObject& k = iter->Key(true);
                    if (k.GetType() != ele.GetType() || k.GetKey() != keys[i].GetKey() || k.GetNameSpace() != keys[i].GetNameSpace())
                    {
                        break;
                    }
                    double score = 1.0;
                    if (k.GetType() == KEY_ZSET_SCORE)
                    {
                        score = iter->Value().GetZSetScore();
                    }
                    score = weights[i] * score;
                    DataScoreMap& result_map = inter_union_result[result_cursor];
                    std::pair<DataScoreMap::iterator, bool> ret = result_map.insert(DataScoreMap::value_type(k.GetElement(0), score));
                    if (!ret.second)
                    {
                        zunionInterAggregate(&score, ret.first->second, aggregate);
                        ret.first->second = score;
                    }
                    iter->Next();
                }
            }
            DELETE(iter);
        }
        if (vs[vs.size() - 1].GetType() > 0)
        {
            DelKey(ctx, destkey);
        }

        if (!inter_union_result[result_cursor].empty())
        {
            DataScoreMap::iterator it = inter_union_result[result_cursor].begin();
            ValueObject dest_meta;
            dest_meta.SetType(KEY_ZSET);
            dest_meta.SetObjectLen(inter_union_result[result_cursor].size());
            while (it != inter_union_result[result_cursor].end())
            {
                KeyObject element(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
                element.SetSetMember(it->first);
                ValueObject score;
                score.SetType(KEY_ZSET_SCORE);
                score.SetZSetScore(it->second);
                SetKeyValue(ctx, element, score);
                KeyObject sort(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
                sort.SetZSetMember(it->first);
                sort.SetZSetScore(it->second);
                ValueObject sort_value;
                sort_value.SetType(KEY_ZSET_SORT);
                SetKeyValue(ctx, sort, sort_value);
                it++;
            }
            dest_meta.SetMinData(inter_union_result[result_cursor].begin()->first);
            dest_meta.SetMaxData(inter_union_result[result_cursor].rbegin()->first);
            SetKeyValue(ctx, destkey, dest_meta);
        }
        reply.SetInteger(inter_union_result[result_cursor].size());
        return 0;
    }

    int Ardb::ZUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZInterStore(ctx, cmd);
    }

    int Ardb::ZScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }

OP_NAMESPACE_END

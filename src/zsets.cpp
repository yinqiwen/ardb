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

#include "db.hpp"
#include "ardb_server.hpp"
#include "helper/db_helpers.hpp"
#include <fnmatch.h>
#include <algorithm>

#define MAX_ZSET_RANGE_NUM  1000

#define SORT_BY_NOSORT 0
#define SORT_BY_VALUE 1
#define SORT_BY_SCORE 2

#define MAX_ZSET_RANGE_COUNT_TABLE_SIZE  4096
#define MAX_ZSET_RANGE_SIZE  512

namespace ardb
{
    int ArdbServer::ZAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "ERR wrong number of arguments for ZAdd");
            return 0;
        }
        ValueDataArray scores;
        SliceArray svs;
        SliceArray attrs;
        bool with_limit = false;
        uint32 limit = 0;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit"))
            {
                if (!string_touint32(cmd.GetArguments()[i + 1], limit))
                {
                    fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
                    return 0;
                }
                with_limit = true;
                break;
            }
            ValueData score(cmd.GetArguments()[i]);
            if (score.type != INTEGER_VALUE && score.type != DOUBLE_VALUE)
            {
            	if(!string_todouble(score.bytes_value, score.double_value))
            	{
                    fill_error_reply(ctx.reply, "ERR value is not a float or out of range");
                    return 0;
            	}
            	score.SetDoubleValue(score.double_value);
            }
            scores.push_back(score);
            svs.push_back(cmd.GetArguments()[i + 1]);
            attrs.push_back(Slice());
        }

        if (with_limit)
        {
            ValueDataArray vs;
            m_db->ZAddLimit(ctx.currentDB, cmd.GetArguments()[0], scores, svs, attrs, limit, vs);
            fill_array_reply(ctx.reply, vs);
        }
        else
        {
            int count = m_db->ZAdd(ctx.currentDB, cmd.GetArguments()[0], scores, svs, attrs);
            fill_int_reply(ctx.reply, count);
        }
        return 0;
    }

    int ArdbServer::ZCard(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->ZCard(ctx.currentDB, cmd.GetArguments()[0]);
        if (ret < 0)
        {
            ret = 0;
        }
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::ZCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->ZCount(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::ZIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ValueData increment(cmd.GetArguments()[1]);
        if (increment.type != INTEGER_VALUE && increment.type != DOUBLE_VALUE)
        {
            fill_error_reply(ctx.reply, "ERR value is not a float or out of range");
            return 0;
        }
        ValueData value;
        m_db->ZIncrby(ctx.currentDB, cmd.GetArguments()[0], increment, cmd.GetArguments()[2], value);
        fill_value_reply(ctx.reply, value);
        return 0;
    }

    int ArdbServer::ZRange(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (strcasecmp(cmd.GetArguments()[3].c_str(), "withscores") != 0)
            {
                fill_error_reply(ctx.reply, "ERR syntax error");
                return 0;
            }
            withscores = true;
        }
        int start, stop;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], stop))
        {
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        ZSetQueryOptions options;
        options.withscores = withscores;
        ValueDataArray vs;
        m_db->ZRange(ctx.currentDB, cmd.GetArguments()[0], start, stop, vs, options);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    static bool process_query_options(const ArgumentArray& cmd, uint32 idx, ZSetQueryOptions& options)
    {
        for (uint32 i = idx; i < cmd.size(); i++)
        {
            if (!strcasecmp(cmd[i].c_str(), "withscores"))
            {
                options.withscores = true;
            }
            else if (!strcasecmp(cmd[i].c_str(), "limit"))
            {
                if (i + 2 >= cmd.size())
                {
                    return false;
                }
                if (!string_toint32(cmd[i + 1], options.limit_offset)
                        || !string_toint32(cmd[i + 2], options.limit_count))
                {
                    return false;
                }
                options.withlimit = true;
                i += 2;
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    int ArdbServer::ZRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ZSetQueryOptions options;
        if (cmd.GetArguments().size() >= 4)
        {
            bool ret = process_query_options(cmd.GetArguments(), 3, options);
            if (!ret)
            {
                fill_error_reply(ctx.reply, "ERR syntax error");
                return 0;
            }
        }

        ValueDataArray vs;
        m_db->ZRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2], vs,
                options);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::ZRank(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->ZRank(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (ret < 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            fill_int_reply(ctx.reply, ret);
        }
        return 0;
    }

    int ArdbServer::ZPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 num = 1;
        if (cmd.GetArguments().size() == 2)
        {
            if (!string_touint32(cmd.GetArguments()[1], num))
            {
                fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
                return 0;
            }
        }
        ValueDataArray vs;
        bool reverse = (strcasecmp(cmd.GetCommand().c_str(), "zrpop") == 0);
        m_db->ZPop(ctx.currentDB, cmd.GetArguments()[0], reverse, num, vs);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::ZRem(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int count = 0;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            count += m_db->ZRem(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[i]);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::ZRemRangeByRank(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int start, stop;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], stop))
        {
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        int count = m_db->ZRemRangeByRank(ctx.currentDB, cmd.GetArguments()[0], start, stop);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::ZRemRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int count = m_db->ZRemRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2]);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::ZRevRange(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (string_tolower(cmd.GetArguments()[3]) != "withscores")
            {
                fill_error_reply(ctx.reply, "ERR syntax error");
                return 0;
            }
            withscores = true;
        }
        int start, stop;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], stop))
        {
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        ZSetQueryOptions options;
        options.withscores = withscores;
        ValueDataArray vs;
        m_db->ZRevRange(ctx.currentDB, cmd.GetArguments()[0], start, stop, vs, options);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::ZRevRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ZSetQueryOptions options;
        if (cmd.GetArguments().size() >= 4)
        {
            bool ret = process_query_options(cmd.GetArguments(), 3, options);
            if (!ret)
            {
                fill_error_reply(ctx.reply, "ERR syntax error");
                return 0;
            }
        }

        ValueDataArray vs;
        m_db->ZRevRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2], vs,
                options);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::ZRevRank(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->ZRevRank(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (ret < 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            fill_int_reply(ctx.reply, ret);
        }
        return 0;
    }

    static bool process_zstore_args(const ArgumentArray& cmd, SliceArray& keys, WeightArray& ws, AggregateType& type)
    {
        int numkeys;
        if (!string_toint32(cmd[1], numkeys) || numkeys <= 0)
        {
            return false;
        }
        if (cmd.size() < (uint32) numkeys + 2)
        {
            return false;
        }
        for (int i = 2; i < numkeys + 2; i++)
        {
            keys.push_back(cmd[i]);
        }
        if (cmd.size() > (uint32) numkeys + 2)
        {
            uint32 idx = numkeys + 2;
            std::string opstr = string_tolower(cmd[numkeys + 2]);
            if (opstr == "weights")
            {
                if (cmd.size() < ((uint32) numkeys * 2) + 3)
                {
                    return false;
                }
                uint32 weight;
                for (int i = idx + 1; i < (numkeys * 2) + 3; i++)
                {
                    if (!string_touint32(cmd[i], weight))
                    {
                        return false;
                    }
                    ws.push_back(weight);
                }
                idx = numkeys * 2 + 3;
                if (cmd.size() <= idx)
                {
                    return true;
                }
                opstr = string_tolower(cmd[idx]);
            }
            if (cmd.size() > (idx + 1) && opstr == "aggregate")
            {
                std::string typestr = string_tolower(cmd[idx + 1]);
                if (typestr == "sum")
                {
                    type = AGGREGATE_SUM;
                }
                else if (typestr == "max")
                {
                    type = AGGREGATE_MAX;
                }
                else if (typestr == "min")
                {
                    type = AGGREGATE_MIN;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    int ArdbServer::ZInterStore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        WeightArray ws;
        AggregateType type = AGGREGATE_SUM;
        if (!process_zstore_args(cmd.GetArguments(), keys, ws, type))
        {
            fill_error_reply(ctx.reply, "ERR syntax error");
            return 0;
        }
        int count = m_db->ZInterStore(ctx.currentDB, cmd.GetArguments()[0], keys, ws, type);
        if (count < 0)
        {
            count = 0;
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::ZUnionStore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        WeightArray ws;
        AggregateType type = AGGREGATE_SUM;
        if (!process_zstore_args(cmd.GetArguments(), keys, ws, type))
        {
            fill_error_reply(ctx.reply, "ERR syntax error");
            return 0;
        }
        int count = m_db->ZUnionStore(ctx.currentDB, cmd.GetArguments()[0], keys, ws, type);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::ZScan(ArdbConnContext& ctx, RedisCommandFrame& cmd)
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
                        fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
                        return 0;
                    }
                    i++;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "match"))
                {
                    if (i + 1 >= cmd.GetArguments().size())
                    {
                        fill_error_reply(ctx.reply, "ERR 'MATCH' need one args followed");
                        return 0;
                    }
                    pattern = cmd.GetArguments()[i + 1];
                    i++;
                }
                else
                {
                    fill_error_reply(ctx.reply, " Syntax error, try scan 0 ");
                    return 0;
                }
            }
        }
        std::string newcursor = "0";
        ValueDataArray vs;
        m_db->ZScan(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], pattern, limit, vs, newcursor);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        ctx.reply.elements.push_back(RedisReply(newcursor));
        RedisReply rs;
        fill_array_reply(rs, vs);
        ctx.reply.elements.push_back(rs);
        return 0;
    }

    int ArdbServer::ZScore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ValueData score;
        int ret = m_db->ZScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], score);
        if (ret < 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            fill_value_reply(ctx.reply, score);
        }
        return 0;
    }

    int ArdbServer::ZClear(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->ZClear(ctx.currentDB, cmd.GetArguments()[0]);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

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

    static int parse_zrange(const std::string& min, const std::string& max, ZRangeSpec& range)
    {
        range.max.type = DOUBLE_VALUE;
        range.min.type = DOUBLE_VALUE;
        int ret = parse_score(min, range.min.double_value, range.contain_min);
        if (0 == ret)
        {
            ret = parse_score(max, range.max.double_value, range.contain_max);
        }
        return ret;
    }

    static bool less_by_zset_score(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.score.NumberValue() < v2.score.NumberValue();
    }

    static bool less_by_zset_value(const ZSetElement& v1, const ZSetElement& v2)
    {
        return v1.value < v2.value;
    }

    ZSetMetaValue* Ardb::GetZSetMeta(const DBID& db, const Slice& key, uint8 sort_func, int& err, bool& created)
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
            if (zmeta->encoding == ZSET_ENCODING_ZIPLIST)
            {
                zmeta->size = zmeta->zipvs.size();
                if (sort_func > 0)
                {
                    if (zmeta->sort_func != sort_func)
                    {
                        zmeta->sort_func = sort_func;
                        std::sort(zmeta->zipvs.begin(), zmeta->zipvs.end(),
                                sort_func == SORT_BY_SCORE ? less_by_zset_score : less_by_zset_value);
                    }
                }
            }
            created = false;
        }
        return (ZSetMetaValue*) meta;
    }

    int Ardb::InsertZSetZipEntry(ZSetMetaValue* meta, ZSetElement& entry, uint8 sort_func)
    {
        if (sort_func > 0)
        {
            if (meta->sort_func != sort_func)
            {
                meta->sort_func = sort_func;
                std::sort(meta->zipvs.begin(), meta->zipvs.end(),
                        sort_func == SORT_BY_SCORE ? less_by_zset_score : less_by_zset_value);
            }
            ZSetElementDeque::iterator it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), entry,
                    sort_func == SORT_BY_SCORE ? less_by_zset_score : less_by_zset_value);
            if (it != meta->zipvs.end())
            {
                meta->zipvs.insert(it, entry);
            }
            else
            {
                meta->zipvs.push_back(entry);
            }
        }
        else
        {
            ERROR_LOG("Insert zset with sort func:0");
        }
        return 0;
    }

    int Ardb::GetZSetZipEntry(ZSetMetaValue* meta, const ValueData& value, ZSetElement& entry, bool remove)
    {
        if (meta->sort_func != SORT_BY_VALUE)
        {
            meta->sort_func = SORT_BY_VALUE;
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

    int Ardb::ZAddLimit(const DBID& db, const Slice& key, ValueDataArray& scores, const SliceArray& svs,
            const SliceArray& attrs, int setlimit, ValueDataArray& pops)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_VALUE, err, createZset);
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
            TryZAdd(db, key, *meta, scores[i], svs[i], attrs[i], true);
        }
        SetMeta(db, key, *meta);
        if (meta->size > (uint32) setlimit)
        {
            ZPop(db, key, false, meta->size - setlimit, pops);
        }
        DELETE(meta);
        return 0;
    }

    static bool less_by_zrange_score(const ZScoreRangeCounter& v1, const double& v2)
    {
        return v1.max < v2;
    }

    int Ardb::ZDeleteRangeScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score)
    {
        double v = score.NumberValue();
        ZScoreRangeCounterArray::iterator fit = std::lower_bound(meta.ranges.begin(), meta.ranges.end(), v,
                less_by_zrange_score);
        if (fit == meta.ranges.end())
        {
            ERROR_LOG("Failed to find score range.");
            return -1;
        }
        fit->count--;
        ZScoreRangeCounterArray::iterator before = fit - 1;
        if(fit == meta.ranges.begin())
        {
        	before = meta.ranges.end();
        }
        ZScoreRangeCounterArray::iterator after = fit + 1;
        if (fit->count == 0)
        {
            meta.ranges.erase(fit);
        }
        else
        {
            if (before != meta.ranges.end())
            {
                if (before->count + fit->count < MAX_ZSET_RANGE_SIZE)
                {
                    before->count = before->count + fit->count;
                    before->max = fit->max;
                    meta.ranges.erase(fit);
                    return 0;
                }
            }
            if (after != meta.ranges.end())
            {
                if (after->count + fit->count < MAX_ZSET_RANGE_SIZE)
                {
                    after->count = before->count + fit->count;
                    after->min = fit->min;
                    meta.ranges.erase(fit);
                    return 0;
                }
            }
        }
        return 0;
    }

    int Ardb::ZInsertRangeScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score)
    {
        double v = score.NumberValue();
        ZScoreRangeCounterArray::iterator fit = std::lower_bound(meta.ranges.begin(), meta.ranges.end(), v,
                less_by_zrange_score);
        if (fit == meta.ranges.end())
        {
            if (meta.ranges.size() > 0)
            {
                fit = meta.ranges.end();
                fit--;
                fit->max = v;
                fit->count++;
            }
            else
            {
                ZScoreRangeCounter range;
                range.min = range.max = v;
                range.count = 1;
                meta.ranges.push_back(range);
                return 0;
            }
        }
        else
        {
        	fit->count++;
        	if(fit->min > v)
        	{
        		fit->min = v;
        	}
        }
        if (meta.ranges.size() < MAX_ZSET_RANGE_COUNT_TABLE_SIZE && fit->count > MAX_ZSET_RANGE_SIZE
                && (fit->count) * meta.ranges.size() > meta.size && fit->min < fit->max)
        {
            //split range
            ZSetElement ele;
            ele.score.SetDoubleValue(fit->min);
            uint32 split_count = meta.ranges.size() / 2;
            ZSetKeyObject tmp(key, ele, db);
            struct ZRangeWalk: public WalkHandler
            {
                    int64 z_count;
                    double z_split_cur_max;
                    double z_split_next_min;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                        z_count--;
                        if (0 == z_count)
                        {
                            z_split_cur_max = zsk->score.NumberValue();
                            return 0;
                        }
                        else if (-1 == z_count)
                        {
                            z_split_next_min = zsk->score.NumberValue();
                            return -1;
                        }
                        return 0;
                    }
                    ZRangeWalk(uint32 count) :
                            z_count(count), z_split_cur_max(0), z_split_next_min(0)
                    {
                    }
            } walk(split_count);
            Walk(tmp, false, false, &walk);
            ZScoreRangeCounter next_range;
            next_range.min = walk.z_split_next_min;
            next_range.max = fit->max;
            fit->count = split_count;
            fit->max = walk.z_split_cur_max;
            meta.ranges.insert(fit, next_range);
        }
        return 0;
    }

    /*
     * returns  0:no meta change
     *          1:meta change
     */
    int Ardb::TryZAdd(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score, const Slice& value,
            const Slice& attr, bool check_value)
    {
        int cache_add_ret = -1;
        ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, true, false);
        if (NULL != cache)
        {
            cache_add_ret = cache->Add(score, value, attr);
            m_level1_cahce->Recycle(cache);
            if (cache_add_ret == ZSET_CACHE_NONEW_ELEMENT)
            {
                return 0;
            }
        }
        else
        {
            /*
             * only create cache if it's the first inserted element
             */
            if (meta.size == 0 && NULL != m_level1_cahce && m_config.zset_write_fill_cache)
            {
                cache = (ZSetCache*) m_level1_cahce->CreateCacheEntry(db, key, ZSET_META);
                if (NULL != cache)
                {
                    cache->Add(score, value, attr);
                    m_level1_cahce->Recycle(cache);
                }
            }
        }
        bool zip_save = meta.encoding == ZSET_ENCODING_ZIPLIST;
        ZSetElement element(value, score);
        element.attr.SetValue(attr, false);
        if (zip_save)
        {
            ZSetElement entry;
            if (check_value && 0 == GetZSetZipEntry(&meta, element.value, entry, true))
            {
                if (entry.score.NumberValue() == score.NumberValue() && entry.attr == element.attr)
                {
                    return 0;
                }
                else
                {
                    InsertZSetZipEntry(&meta, element, SORT_BY_VALUE);
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
                if (zip_save)
                {
                    meta.size++;
                    InsertZSetZipEntry(&meta, element, SORT_BY_VALUE);
                    return 1;
                }
            }
        }

        if (ZSET_ENCODING_ZIPLIST == meta.encoding)
        {
            BatchWriteGuard guard(GetEngine());
            ZSetElementDeque::iterator it = meta.zipvs.begin();
            meta.size = 0;
            while (it != meta.zipvs.end())
            {
                ZSetNodeKeyObject zk(key, it->value, db);
                ZSetNodeValueObject zv;
                zv.score = it->score;
                zv.attr = it->attr;
                SetKeyValueObject(zk, zv);
                ZSetKeyObject zsk(key, it->value, it->score, db);
                CommonValueObject zsv;
                zsv.data = it->attr;
                SetKeyValueObject(zsk, zsv);
                meta.size++;
                ZInsertRangeScore(db, key, meta, it->score);
                it++;
            }
            ZSetNodeKeyObject nzk(key, element.value, db);
            ZSetNodeValueObject nzv;
            nzv.score = score;
            nzv.attr = element.attr;
            SetKeyValueObject(nzk, nzv);
            ZSetKeyObject zsk(key, element.value, score, db);
            CommonValueObject zsv;
            zsv.data = element.attr;
            SetKeyValueObject(zsk, zsv);
            meta.encoding = ZSET_ENCODING_HASH;
            meta.zipvs.clear();
            meta.size++;
            ZInsertRangeScore(db, key, meta, score);
            return 1;
        }
        BatchWriteGuard guard(GetEngine());
        ZSetNodeKeyObject zk(key, value, db);
        ZSetNodeValueObject zv;
        if (!check_value || cache_add_ret == ZSET_CACHE_NEW_ELEMENT || cache_add_ret == ZSET_CACHE_ATTR_CHANGED
                || 0 != GetKeyValueObject(zk, zv))
        {
            zv.score = score;
            zv.attr = element.attr;
            SetKeyValueObject(zk, zv);
            ZSetKeyObject zsk(key, value, score, db);
            CommonValueObject zsv;
            zsv.data = element.attr;
            SetKeyValueObject(zsk, zsv);
            if (cache_add_ret != ZSET_CACHE_ATTR_CHANGED)
            {
                meta.size++;
                ZInsertRangeScore(db, key, meta, score);
                return 1;
            }
            return 0;
        }
        else
        {
            if (zv.score.NumberValue() != score.NumberValue())
            {
                ZSetKeyObject zsk(key, value, zv.score, db);
                DelValue(zsk);
                ZDeleteRangeScore(db, key, meta, zv.score);
                zsk.score = score;
                CommonValueObject zsv;
                zsv.data = element.attr;
                SetKeyValueObject(zsk, zsv);
                zv.score = score;
                SetKeyValueObject(zk, zv);
                ZInsertRangeScore(db, key, meta, score);
                return 1;
            }
        }
        return 0;
    }
    int Ardb::ZAdd(const DBID& db, const Slice& key, const ValueData& score, const Slice& value, const Slice& attr)
    {
        ValueDataArray scores;
        SliceArray svs, attrs;
        scores.push_back(score);
        svs.push_back(value);
        attrs.push_back(attr);
        return ZAdd(db, key, scores, svs, attrs);
    }
    int Ardb::ZAdd(const DBID& db, const Slice& key, ValueDataArray& scores, const SliceArray& svs,
            const SliceArray& attrs)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_VALUE, err, createZset);
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
            int tryret = TryZAdd(db, key, *meta, scores[i], svs[i], attrs[i], true);
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
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_NOSORT, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        int size = meta->size;
        DELETE(meta);
        return size;
    }

    int Ardb::ZGetNodeValue(const DBID& db, const Slice& key, const Slice& value, ValueData& score, ValueData& attr)
    {
        int err = 0;
        ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, false, false);
        if (NULL != cache)
        {
            err = cache->GetByValue(value, score, attr);
            m_level1_cahce->Recycle(cache);
            return err != 0 ? ERR_NOT_EXIST : 0;
        }
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_NOSORT, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElement e;
            e.value.SetValue(value, true);
            if (SORT_BY_VALUE == meta->sort_func)
            {
                ZSetElementDeque::iterator fit = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), e,
                        less_by_zset_value);
                if (fit != meta->zipvs.end() && fit->value.Compare(e.value) == 0)
                {
                    score = fit->score;
                    attr = fit->attr;
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
                        attr = zit->attr;
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
            ZSetNodeKeyObject zk(key, value, db);
            ZSetNodeValueObject zv;
            if (0 != GetKeyValueObject(zk, zv))
            {
                err = ERR_NOT_EXIST;
            }
            else
            {
                score = zv.score;
                attr = zv.attr;
            }
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::ZScore(const DBID& db, const Slice& key, const Slice& value, ValueData& score)
    {
        ValueData attr;
        return ZGetNodeValue(db, key, value, score, attr);
    }

    int Ardb::ZIncrby(const DBID& db, const Slice& key, const ValueData& increment, const Slice& value,
            ValueData& score)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_VALUE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ValueData v(value);
            ZSetElement element;
            if (0 == GetZSetZipEntry(meta, v, element, true))
            {
                element.score += increment;
                score = element.score;
                InsertZSetZipEntry(meta, element, SORT_BY_VALUE);
                SetMeta(db, key, *meta);
            }
            else
            {
                err = ERR_NOT_EXIST;
            }
        }
        else
        {
            ZSetNodeKeyObject zk(key, value, db);
            ZSetNodeValueObject zv;
            if (0 == GetKeyValueObject(zk, zv))
            {
                BatchWriteGuard guard(GetEngine());
                ZSetKeyObject zsk(key, value, zv.score, db);
                DelValue(zsk);
                ZDeleteRangeScore(db, key, *meta, zv.score);
                zsk.score += increment;
                zv.score = zsk.score;
                CommonValueObject zsv;
                zsv.data = zv.attr;
                SetKeyValueObject(zsk, zsv);
                score = zsk.score;
                SetKeyValueObject(zk, zv);
                ZInsertRangeScore(db, key, *meta, score);
                err = 0;
            }
            else
            {
                err = ERR_NOT_EXIST;
            }
        }
        DELETE(meta);
        ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, true, false);
        if (NULL != cache)
        {
            cache->Add(score, value, Slice());
            m_level1_cahce->Recycle(cache);
        }
        return err;
    }

    int Ardb::ZPop(const DBID& db, const Slice& key, bool reverse, uint32 num, ValueDataArray& pops)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }

        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            for (uint32 i = 0; i < num && meta->zipvs.size() > 0; i++)
            {
                pops.push_back(meta->zipvs.front().value);
                meta->zipvs.pop_front();
            }
            meta->size = meta->zipvs.size();
            SetMeta(db, key, *meta);
        }
        else
        {
            Slice empty;
            ZSetKeyObject sk(key, empty, reverse ? DBL_MAX : -DBL_MAX, db);
            BatchWriteGuard guard(GetEngine());
            struct ZPopWalk: public WalkHandler
            {
                    Ardb* z_db;
                    ZSetMetaValue& z_meta;
                    uint32 count;
                    ValueDataArray& vs;
                    int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        ZSetNodeKeyObject tmp(sek->key, sek->value, sek->db);
                        vs.push_back(sek->value);
                        count--;
                        z_db->DelValue(*sek);
                        z_db->DelValue(tmp);
                        z_db->ZDeleteRangeScore(sek->db, sek->key, z_meta, sek->score);
                        if (count == 0)
                        {
                            return -1;
                        }
                        return 0;
                    }
                    ZPopWalk(Ardb* db, ZSetMetaValue& meta, uint32 i, ValueDataArray& v) :
                            z_db(db), z_meta(meta), count(i), vs(v)
                    {
                    }
            } walk(this, *meta, num, pops);
            Walk(sk, reverse, false, &walk);
            if (walk.count < num)
            {
                meta->size -= (num - walk.count);
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, true, false);
        if (NULL != cache)
        {
            ValueDataArray::iterator pit = pops.begin();
            while (pit != pops.end())
            {
                cache->Rem(*pit);
                pit++;
            }
        }
        return 0;
    }

    int Ardb::ZClear(const DBID& db, const Slice& key)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_NOSORT, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        if (NULL != m_level1_cahce)
        {
            m_level1_cahce->Evict(db, key);
        }
        BatchWriteGuard guard(GetEngine());
        if (ZSET_ENCODING_ZIPLIST != meta->encoding)
        {
            Slice empty;
            ZSetKeyObject sk(key, empty, -DBL_MAX, db);
            struct ZClearWalk: public WalkHandler
            {
                    Ardb* z_db;
                    int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        ZSetNodeKeyObject tmp(sek->key, sek->value, sek->db);
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
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_VALUE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ZSetNodeKeyObject zk(key, value, db);
        ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, true, false);
        if (NULL != cache)
        {
            cache->Rem(zk.value);
            m_level1_cahce->Recycle(cache);
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
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

        ZSetNodeValueObject zv;
        if (0 == GetKeyValueObject(zk, zv))
        {
            BatchWriteGuard guard(GetEngine());
            DelValue(zk);
            ZSetKeyObject zsk(key, value, zv.score, db);
            DelValue(zsk);
            meta->size--;
            ZDeleteRangeScore(db, key, *meta, zv.score);
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 0;
        }
        DELETE(meta);
        return ERR_NOT_EXIST;
    }

    uint32 Ardb::ZRankByScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score,
            bool contains_score)
    {
        double v = score.NumberValue();
        ZScoreRangeCounterArray::iterator fit = std::lower_bound(meta.ranges.begin(), meta.ranges.end(), v,
                less_by_zrange_score);
        if (fit == meta.ranges.end())
        {
            return meta.size;
        }
        else
        {
            uint32 rank = 0;
            ZScoreRangeCounterArray::iterator bit = meta.ranges.begin();
            while (bit != fit)
            {
                rank += bit->count;
                bit++;
            }
            if (fit->max == score.NumberValue())
            {
                rank += fit->count;
                rank--;
                return rank;
            }
            if (fit->min == score.NumberValue())
            {
                return rank;
            }
            ZSetElement from;
            from.score.SetDoubleValue(fit->min);
            ZSetKeyObject zk(key, from, db);
            struct ZRankWalk: public WalkHandler
            {
                    uint32& z_rank;
                    const ValueData& z_score;
                    bool z_equal_score;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        if (sek->score >= z_score)
                        {
                            z_equal_score = sek->score.NumberValue() == z_score.NumberValue();
                            return -1;
                        }
                        z_rank++;
                        return 0;
                    }
                    ZRankWalk(uint32& r, const ValueData& s) :
                            z_rank(r), z_score(s), z_equal_score(false)
                    {
                    }
            } walk(rank, score);
            Walk(zk, false, true, &walk);
            if (walk.z_equal_score && !contains_score)
            {
                rank--;
            }
            return rank;
        }
    }

    int Ardb::ZCount(const DBID& db, const Slice& key, const std::string& min, const std::string& max)
    {
        ZRangeSpec range;
        if (parse_zrange(min, max, range))
        {
            return ERR_INVALID_ARGS;
        }
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        int count = 0;
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElement min_ele(Slice(), range.min);
            ZSetElement max_ele(Slice(), range.max);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            while (!range.contain_min && min_it != meta->zipvs.end()
                    && min_it->score.NumberValue() == range.min.NumberValue())
            {
                min_it++;
            }
            while (!range.contain_max && max_it != meta->zipvs.end()
                    && max_it->score.NumberValue() == range.max.NumberValue())
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
            uint32 min_rank = ZRankByScore(db, key, *meta, range.min, range.contain_min);
            uint32 max_rank = ZRankByScore(db, key, *meta, range.max, range.contain_max);
            count = max_rank - min_rank + 1;
        }
        DELETE(meta);
        return count;
    }

    int Ardb::ZRank(const DBID& db, const Slice& key, const Slice& member)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ValueData element(member);
        int32 rank = 0;
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
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
            ValueData score;
            if (0 == ZScore(db, key, member, score))
            {
                rank = ZRankByScore(db, key, *meta, score, true);
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
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ValueData element(member);
        int32 rank = 0;
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
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
            ValueData score;
            if (0 == ZScore(db, key, member, score))
            {
                rank = ZRankByScore(db, key, *meta, score, true);
                rank = meta->size - rank;
            }
            else
            {
                rank = ERR_NOT_EXIST;
            }
        }
        DELETE(meta);
        return rank;
    }

    int Ardb::ZGetByRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, uint32 rank, ZSetElement& e)
    {
        uint32 v_rank = 0;
        ZScoreRangeCounterArray::iterator bit = meta.ranges.begin();
        while (bit != meta.ranges.end())
        {
            if ((v_rank + bit->count - 1) >= rank)
            {
                break;
            }
            v_rank += bit->count;
            bit++;
        }
        if (bit == meta.ranges.end())
        {
            return -1;
        }
        ZSetElement from;
        from.score.SetDoubleValue(bit->min);
        ZSetKeyObject zk(key, from, db);
        struct ZGetWalk: public WalkHandler
        {
                uint32 z_rank_start;
                uint32 z_rank_stop;
                ZSetElement& z_ele;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* sek = (ZSetKeyObject*) k;
                    CommonValueObject* cv = (CommonValueObject*) v;
                    if (z_rank_start == z_rank_stop)
                    {
                        z_ele.score = sek->score;
                        z_ele.value = sek->value;
                        z_ele.attr = cv->data;
                        return -1;
                    }
                    z_rank_start++;
                    return 0;
                }
                ZGetWalk(uint32 start, uint32 end, ZSetElement& e) :
                        z_rank_start(start), z_rank_stop(end), z_ele(e)
                {
                }
        } walk(v_rank, rank, e);
        Walk(zk, false, true, &walk);
        return 0;
    }

    int Ardb::ZRemRangeByRank(const DBID& db, const Slice& key, int start, int stop)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
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
        if (NULL != m_level1_cahce)
        {
            m_level1_cahce->Evict(db, key);
        }
        int count = 0;
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
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
        ZSetElement ele;
        ZGetByRank(db, key, *meta, start, ele);
        ZSetKeyObject sk(key, ele, db);
        struct ZClearWalk: public WalkHandler
        {
                Ardb* z_db;
                ZSetMetaValue& z_meta;
                uint32 count;
                int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
                {
                    ZSetKeyObject* sek = (ZSetKeyObject*) k;
                    ZSetNodeKeyObject tmp(sek->key, sek->value, sek->db);
                    z_db->DelValue(*sek);
                    z_db->DelValue(tmp);
                    z_db->ZDeleteRangeScore(sek->db, sek->key, z_meta, sek->score);
                    count--;
                    if (count == 0)
                    {
                        return -1;
                    }
                    return 0;
                }
                ZClearWalk(Ardb* db, ZSetMetaValue& meta, uint32 c) :
                        z_db(db), z_meta(meta), count(c)
                {
                }
        } walk(this, *meta, stop - start + 1);
        Walk(sk, false, false, &walk);
        DELETE(meta);
        return stop - start + 1 - walk.count;
    }

    int Ardb::ZRemRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ZRangeSpec range;
        if (parse_zrange(min, max, range) < 0)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (NULL != m_level1_cahce)
        {
            m_level1_cahce->Evict(db, key);
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElement min_ele(Slice(), range.min);
            ZSetElement max_ele(Slice(), range.max);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            int count = meta->zipvs.size();

            if (min_it != meta->zipvs.end())
            {
                while (!range.contain_min && min_it != meta->zipvs.end()
                        && min_it->score.NumberValue() == range.min.NumberValue())
                {
                    min_it++;
                }
                while (!range.contain_max && max_it != meta->zipvs.end()
                        && max_it->score.NumberValue() == range.max.NumberValue())
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
        ZSetKeyObject tmp(key, empty, range.min, db);
        BatchWriteGuard guard(GetEngine());
        struct ZRemRangeByScoreWalk: public WalkHandler
        {
                Ardb* z_db;
                ZRangeSpec z_range;
                ZSetMetaValue& z_meta;
                int z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    if (zsk->score.NumberValue() > z_range.max.NumberValue())
                    {
                        return -1;
                    }
                    if (!z_range.contain_min && zsk->score.NumberValue() == z_range.min.NumberValue())
                    {
                        return 0;
                    }
                    if (!z_range.contain_max && zsk->score.NumberValue() == z_range.max.NumberValue())
                    {
                        return -1;
                    }
                    ZSetNodeKeyObject zk(zsk->key, zsk->value, zsk->db);
                    z_db->DelValue(zk);
                    z_db->DelValue(*zsk);
                    z_meta.size--;
                    return 0;
                }
                ZRemRangeByScoreWalk(Ardb* db, ZSetMetaValue& meta, ZRangeSpec& range) :
                        z_db(db), z_range(range), z_meta(meta), z_count(0)
                {
                }
        } walk(this, *meta, range);
        Walk(tmp, false, false, &walk);
        SetMeta(db, key, *meta);
        DELETE(meta);
        return walk.z_count;
    }

    int Ardb::ZRange(const DBID& db, const Slice& key, int start, int stop, ValueDataArray& values,
            ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
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
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElementDeque::iterator esit = meta->zipvs.begin() + start;
            ZSetElementDeque::iterator eeit = meta->zipvs.end();
            if ((uint32) stop < meta->size)
            {
                eeit = meta->zipvs.begin() + stop;
            }
            while (esit <= eeit && esit != meta->zipvs.end())
            {
                values.push_back(esit->value);
                if (options.withscores)
                {
                    values.push_back(esit->score);
                }
                esit++;
            }
            DELETE(meta);
            return options.withscores ? (values.size() / 2) : values.size();
        }
        ZSetElement ele;
        ZGetByRank(db, key, *meta, start, ele);
        ZSetKeyObject tmp(key, ele, db);

        struct ZRangeWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                ZSetQueryOptions& z_options;
                uint32 z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    z_values.push_back(zsk->value);
                    if (z_options.withscores)
                    {
                        z_values.push_back(zsk->score);
                    }
                    z_count--;
                    if (0 == z_count)
                    {
                        return -1;
                    }
                    return 0;
                }
                ZRangeWalk(ValueDataArray& v, ZSetQueryOptions& options, uint32 count) :
                        z_values(v), z_options(options), z_count(count)
                {
                }
        } walk(values, options, stop - start + 1);
        Walk(tmp, false, false, &walk);
        DELETE(meta);
        return 0;
    }

    int Ardb::ZRangeByScoreRange(const DBID& db, const Slice& key, const ZRangeSpec& range, Iterator*& iter,
            ZSetQueryOptions& options, bool check_cache, ValueVisitCallback* cb, void* cbdata)
    {
        if (check_cache)
        {
            ZSetCache* cache = (ZSetCache*) GetLoadedCache(db, key, ZSET_META, false, false);
            if (NULL != cache)
            {
                cache->GetRange(range, options.withscores, options.withattr, cb, cbdata);
                m_level1_cahce->Recycle(cache);
                return 0;
            }
        }
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        int cursor = 0;
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElement min_ele(Slice(), range.min);
            ZSetElement max_ele(Slice(), range.max);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            if (min_it != meta->zipvs.end())
            {
                while (!range.contain_min && min_it != meta->zipvs.end()
                        && min_it->score.NumberValue() == range.min.NumberValue())
                {
                    min_it++;
                }
                while (!range.contain_max && max_it != meta->zipvs.end()
                        && max_it->score.NumberValue() == range.max.NumberValue())
                {
                    max_it--;
                }
                while (min_it <= max_it && min_it != meta->zipvs.end())
                {
                    cb(min_it->value, cursor++, cbdata);
                    if (options.withscores)
                    {
                        cb(min_it->score, cursor++, cbdata);
                    }
                    if (options.withattr)
                    {
                        cb(min_it->attr, cursor++, cbdata);
                    }
                    min_it++;
                }
            }
            DELETE(meta);
            return 0;
        }
        Slice empty;
        ZSetKeyObject tmp(key, empty, range.min, db);
        if (NULL == iter)
        {
            iter = FindValue(tmp, false);
        }
        else
        {
            FindValue(iter, tmp);
        }
        int z_count = 0;
        while (true)
        {
            if (iter->Valid())
            {
                Slice tmpkey = iter->Key();
                ZSetKeyObject* zsk = (ZSetKeyObject*) decode_key(tmpkey, &tmp);
                if (NULL != zsk)
                {
                    bool inrange = false;
                    inrange =
                            range.contain_min ?
                                    zsk->score.NumberValue() >= range.min.NumberValue() :
                                    zsk->score.NumberValue() > range.min.NumberValue();
                    if (inrange)
                    {
                        inrange =
                                range.contain_max ?
                                        zsk->score.NumberValue() <= range.max.NumberValue() :
                                        zsk->score.NumberValue() < range.max.NumberValue();
                    }
                    if (inrange)
                    {
                        if (options.withlimit)
                        {
                            if (z_count >= options.limit_offset
                                    && z_count <= (options.limit_count + options.limit_offset))
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
                            cb(zsk->value, cursor++, cbdata);
                            if (options.withscores)
                            {
                                cb(zsk->score, cursor++, cbdata);
                            }
                            if (options.withattr)
                            {
                                Slice tmpvalue = iter->Value();
                                ValueObject* v = decode_value_obj(zsk->type, tmpvalue.data(), tmpvalue.size());
                                CommonValueObject* cv = (CommonValueObject*) v;
                                cb(cv->data, cursor++, cbdata);
                                DELETE(cv);
                            }
                        }
                    }
                    else
                    {
                        DELETE(zsk);
                        break;
                    }
                    if (zsk->score.NumberValue() == range.max.NumberValue()
                            || (options.withlimit && z_count > (options.limit_count + options.limit_offset)))
                    {
                        DELETE(zsk);
                        break;
                    }
                    DELETE(zsk);
                    iter->Next();
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
        DELETE(meta);
        return options.withscores ? cursor / 2 : cursor;
    }

    static int ZRangeValueStoreCallback(const ValueData& value, int cursor, void* cb)
    {
        ValueDataArray* s = (ValueDataArray*) cb;
        s->push_back(value);
        return 0;
    }

    int Ardb::ZRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max,
            ValueDataArray& values, ZSetQueryOptions& options)
    {
        ZRangeSpec range;
        if (parse_zrange(min, max, range) < 0)
        {
            return ERR_INVALID_ARGS;
        }
        Iterator* iter = NULL;
        int ret = ZRangeByScoreRange(db, key, range, iter, options, true, ZRangeValueStoreCallback, &values);
        DELETE(iter);
        return ret;
    }

    int Ardb::ZRevRange(const DBID& db, const Slice& key, int start, int stop, ValueDataArray& values,
            ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
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
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElementDeque::reverse_iterator esit = meta->zipvs.rbegin() + start;
            ZSetElementDeque::reverse_iterator eeit = meta->zipvs.rend();
            if ((uint32) stop < meta->size)
            {
                eeit = meta->zipvs.rbegin() + stop;
            }
            while (esit <= eeit && esit != meta->zipvs.rend())
            {
                values.push_back(esit->value);
                if (options.withscores)
                {
                    values.push_back(esit->score);
                }
                esit++;
            }
            DELETE(meta);
            return 0;
        }
        ZSetElement ele;
        ZGetByRank(db, key, *meta, stop, ele);
        ZSetKeyObject tmp(key, ele, db);
        struct ZRangeWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                ZSetQueryOptions& z_options;
                uint32 z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    z_values.push_back(zsk->value);
                    if (z_options.withscores)
                    {
                        z_values.push_back(zsk->score);
                    }
                    z_count--;
                    if (0 == z_count)
                    {
                        return -1;
                    }
                    return 0;
                }
                ZRangeWalk(ValueDataArray& v, ZSetQueryOptions& options, uint32 count) :
                        z_values(v), z_options(options), z_count(count)
                {
                }
        } walk(values, options, stop - start + 1);
        Walk(tmp, true, false, &walk);
        DELETE(meta);
        return options.withscores ? (values.size() / 2) : values.size();
    }

    int Ardb::ZRevRangeByScore(const DBID& db, const Slice& key, const std::string& max, const std::string& min,
            ValueDataArray& values, ZSetQueryOptions& options)
    {
        int err = 0;
        bool createZset = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, SORT_BY_SCORE, err, createZset);
        if (NULL == meta || createZset)
        {
            DELETE(meta);
            return err;
        }
        ZRangeSpec range;
        if (parse_zrange(min, max, range) < 0)
        {
            DELETE(meta);
            return ERR_INVALID_ARGS;
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElement min_ele(Slice(), range.min);
            ZSetElement max_ele(Slice(), range.max);
            ZSetElementDeque::iterator min_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), min_ele,
                    less_by_zset_score);
            ZSetElementDeque::iterator max_it = std::lower_bound(meta->zipvs.begin(), meta->zipvs.end(), max_ele,
                    less_by_zset_score);
            if (min_it != meta->zipvs.end())
            {
                while (!range.contain_min && min_it->score.NumberValue() == range.min.NumberValue())
                {
                    min_it++;
                }
                while (!range.contain_max && max_it != meta->zipvs.end()
                        && max_it->score.NumberValue() == range.max.NumberValue())
                {
                    max_it--;
                }
                if (max_it == meta->zipvs.end())
                {
                    max_it--;
                }
                while (min_it <= max_it && min_it != meta->zipvs.end())
                {
                    if (options.withscores)
                    {
                        values.push_front(min_it->score);
                    }
                    values.push_front(min_it->value);
                    min_it++;
                }
            }
            DELETE(meta);
            return 0;
        }

        Slice empty;
        ZSetKeyObject tmp(key, empty, range.max, db);
        struct ZRangeByScoreWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                ZSetQueryOptions& z_options;
                ZRangeSpec& z_range;
                int z_count;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ZSetKeyObject* zsk = (ZSetKeyObject*) k;
                    bool inrange = false;
                    inrange =
                            z_range.contain_min ?
                                    zsk->score.NumberValue() >= z_range.min.NumberValue() :
                                    zsk->score.NumberValue() > z_range.max.NumberValue();
                    if (inrange)
                    {
                        inrange =
                                z_range.contain_max ?
                                        zsk->score.NumberValue() <= z_range.max.NumberValue() :
                                        zsk->score.NumberValue() < z_range.max.NumberValue();
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
                            z_values.push_back(zsk->value);
                            if (z_options.withscores)
                            {
                                z_values.push_back(ValueData(zsk->score));
                            }
                        }
                    }
                    if (zsk->score.NumberValue() == z_range.min.NumberValue()
                            || (z_options.withlimit && z_count > (z_options.limit_count + z_options.limit_offset)))
                    {
                        return -1;
                    }
                    return 0;
                }
                ZRangeByScoreWalk(ValueDataArray& v, ZSetQueryOptions& options, ZRangeSpec& range) :
                        z_values(v), z_options(options), z_range(range), z_count(0)
                {
                }
        } walk(values, options, range);
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
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            if (meta->sort_func != SORT_BY_VALUE)
            {
                meta->sort_func = SORT_BY_VALUE;
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
            ZSetNodeKeyObject start(key, value_begin, db);
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
                        ZSetNodeKeyObject* zsk = (ZSetNodeKeyObject*) k;
                        CommonValueObject* cv = (CommonValueObject*) value;
                        if (z_vs.size() >= z_limit || zsk->value.Compare(end_obj) > 0)
                        {
                            return -1;
                        }
                        ZSetElement element;
                        element.score = cv->data;
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
                    double score = weights[i] * it->score.NumberValue();
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
                    TryZAdd(db, dst, zmeta, vm.begin()->second, val, "", false);
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
                    TryZAdd(db, dst, zmeta, vit->second, val, "", false);
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
                    double score = weights[i] * it->score.NumberValue();
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
                        TryZAdd(db, dst, zmeta, vm.begin()->second.first, val, "", false);
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
                        TryZAdd(db, dst, zmeta, vit->second.first, val, "", false);
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
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            DelMeta(db1, key1, meta);
            SetMeta(db2, key2, *meta);
        }
        else
        {
            Slice empty;
            ZSetKeyObject k(key1, Slice(), -DBL_MAX, db1);
            ZSetMetaValue zmeta;
            zmeta.encoding = meta->encoding;
            struct SetWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    ZSetMetaValue& zm;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        CommonValueObject* cv = (CommonValueObject*) v;
                        sek->key = dst;
                        sek->db = dstdb;
                        z_db->SetKeyValueObject(*sek, *cv);
                        ZSetNodeKeyObject zsk(dst, sek->value, dstdb);
                        ZSetNodeValueObject score_obj;
                        score_obj.score = sek->score;
                        score_obj.attr = cv->data;
                        z_db->SetKeyValueObject(zsk, score_obj);
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

    int Ardb::ZScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
            uint32 limit, ValueDataArray& vs, std::string& newcursor)
    {
        int err = 0;
        bool createHash = false;
        ZSetMetaValue* meta = GetZSetMeta(db, key, 1, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        ZSetElement fromobj;
        if (cursor == "0")
        {
            fromobj.score.SetDoubleValue(-DBL_MAX);
        }
        else
        {
            fromobj.score.SetDoubleValue(0);
            string_todouble(cursor, fromobj.score.double_value);
        }
        if (ZSET_ENCODING_ZIPLIST == meta->encoding)
        {
            ZSetElementDeque::iterator fit = std::upper_bound(meta->zipvs.begin(), meta->zipvs.end(), fromobj,
                    less_by_zset_score);
            while (fit != meta->zipvs.end())
            {
                std::string fstr;
                fit->value.ToString(fstr);
                if ((pattern.empty() || fnmatch(pattern.c_str(), fstr.c_str(), 0) == 0))
                {
                    vs.push_back(ValueData(fit->score));
                    vs.push_back(fit->value);
                }
                if (vs.size() >= (limit * 2))
                {
                    break;
                }
                fit++;
            }
        }
        else
        {
            ZSetKeyObject hk(key, fromobj, db);
            struct ZGetWalk: public WalkHandler
            {
                    ValueDataArray& h_vs;
                    const ZSetElement& first;
                    int l;
                    const std::string& h_pattern;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ZSetKeyObject* sek = (ZSetKeyObject*) k;
                        if (0 == cursor)
                        {
                            if (first.score == sek->score)
                            {
                                return 0;
                            }
                        }
                        std::string fstr;
                        sek->value.ToString(fstr);
                        if ((h_pattern.empty() || fnmatch(h_pattern.c_str(), fstr.c_str(), 0) == 0))
                        {
                            h_vs.push_back(ValueData(sek->score));
                            h_vs.push_back(sek->value);
                        }
                        if (l > 0 && h_vs.size() >= (uint32) l * 2)
                        {
                            return -1;
                        }
                        return 0;
                    }
                    ZGetWalk(ValueDataArray& fs, int count, const ZSetElement& s, const std::string& p) :
                            h_vs(fs), first(s), l(count), h_pattern(p)
                    {
                    }
            } walk(vs, limit, fromobj, pattern);
            Walk(hk, false, true, &walk);
        }
        if (vs.empty())
        {
            newcursor = "0";
        }
        else
        {
            vs[vs.size() - 2].ToString(newcursor);
        }
        DELETE(meta);
        return 0;
    }
}


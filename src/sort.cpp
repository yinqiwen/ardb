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
#include <algorithm>
#include <vector>
#include <fnmatch.h>

namespace ardb
{
    int ArdbServer::Sort(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ValueDataArray vs;
        std::string key = cmd.GetArguments()[0];
        cmd.GetMutableArguments().pop_front();
        int ret = m_db->Sort(ctx.currentDB, key, cmd.GetArguments(), vs);
        if (ret < 0)
        {
            fill_error_reply(ctx.reply, "Invalid SORT command or invalid state for SORT.");
        }
        else
        {
            fill_array_reply(ctx.reply, vs);
        }
        cmd.GetMutableArguments().push_front(key);
        return 0;
    }

    struct SortValue
    {
            ValueData* value;
            ValueData cmp;
            double score;
            SortValue(ValueData* v) :
                    value(v), score(0)
            {
            }
            int Compare(const SortValue& other) const
            {
                if (cmp.type == EMPTY_VALUE && other.cmp.type == EMPTY_VALUE)
                {
                    return value->Compare(*other.value);
                }
                return cmp.Compare(other.cmp);
            }
    };
    template<typename T>
    static bool greater_value(const T& v1, const T& v2)
    {
        return v1.Compare(v2) > 0;
    }
    template<typename T>
    static bool less_value(const T& v1, const T& v2)
    {
        return v1.Compare(v2) < 0;
    }

    static int parse_sort_options(SortOptions& options, const StringArray& args)
    {
        for (uint32 i = 0; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "asc"))
            {
                options.is_desc = false;
            }
            else if (!strcasecmp(args[i].c_str(), "desc"))
            {
                options.is_desc = true;
            }
            else if (!strcasecmp(args[i].c_str(), "alpha"))
            {
                options.with_alpha = true;
            }
            else if (!strcasecmp(args[i].c_str(), "limit") && i < args.size() - 2)
            {
                options.with_limit = true;
                if (!string_toint32(args[i + 1], options.limit_offset)
                        || !string_toint32(args[i + 2], options.limit_count))
                {
                    return -1;
                }
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "aggregate") && i < args.size() - 1)
            {
                if (!strcasecmp(args[i + 1].c_str(), "sum"))
                {
                    options.aggregate = AGGREGATE_SUM;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "max"))
                {
                    options.aggregate = AGGREGATE_MAX;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "min"))
                {
                    options.aggregate = AGGREGATE_MIN;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "count"))
                {
                    options.aggregate = AGGREGATE_COUNT;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "avg"))
                {
                    options.aggregate = AGGREGATE_AVG;
                }
                else
                {
                    return -1;
                }
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "store") && i < args.size() - 1)
            {
                options.store_dst = args[i + 1].c_str();
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "by") && i < args.size() - 1)
            {
                options.by = args[i + 1].c_str();
                if (!strcasecmp(options.by, "nosort"))
                {
                    options.by = NULL;
                    options.nosort = true;
                }
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "get") && i < args.size() - 1)
            {
                options.get_patterns.push_back(args[i + 1].c_str());
                i++;
            }
            else
            {
                DEBUG_LOG("Invalid sort option:%s", args[i].c_str());
                return -1;
            }
        }
        return 0;
    }

    int Ardb::MatchValueByPattern(const DBID& db, const Slice& key_pattern, const Slice& value_pattern,
            ValueData& subst, ValueData& value)
    {
        if (0 != GetValueByPattern(db, key_pattern, subst, value))
        {
            return -1;
        }
        std::string str;
        value.ToString(str);
        std::string vpattern(value_pattern.data(), value_pattern.size());
        return fnmatch(vpattern.c_str(), str.c_str(), 0) == 0 ? 0 : -1;
    }

    int Ardb::GetValueByPattern(const DBID& db, const Slice& pattern, ValueData& subst, ValueData& value)
    {
        const char *p, *f;
        const char* spat;
        /* If the pattern is "#" return the substitution object itself in order
         * to implement the "SORT ... GET #" feature. */
        spat = pattern.data();
        if (spat[0] == '#' && spat[1] == '\0')
        {
            value = subst;
            return 0;
        }

        /* If we can't find '*' in the pattern we return NULL as to GET a
         * fixed key does not make sense. */
        p = strchr(spat, '*');
        if (!p)
        {
            return -1;
        }
        std::string vstr;
        subst.ToString(vstr);

        f = strstr(spat, "->");
        if (NULL != f && (uint32) (f - spat) == (pattern.size() - 2))
        {
            f = NULL;
        }
        std::string keystr(pattern.data(), pattern.size());
        string_replace(keystr, "*", vstr);

        if (f == NULL)
        {
            /*
             * keystr = "len(...)"
             */
            if (keystr.find("len(") == 0 && keystr.rfind(")") == keystr.size() - 1)
            {
                keystr = keystr.substr(4, keystr.size() - 5);

                int keytype = Type(db, keystr);
                int len = -1;
                switch (keytype)
                {
                    case SET_META:
                    {
                        len = SCard(db, keystr);
                        break;
                    }
                    case LIST_META:
                    {
                        len = LLen(db, keystr);
                        break;
                    }
                    case ZSET_META:
                    {
                        len = ZCard(db, keystr);
                        break;
                    }
                    default:
                    {
                        return -1;
                    }
                }
                value.SetIntValue(len);
                return 0;
            }
            value.type = BYTES_VALUE;
            return Get(db, keystr, value.bytes_value);
        }
        else
        {
            size_t pos = keystr.find("->");
            std::string field = keystr.substr(pos + 2);
            keystr = keystr.substr(0, pos);
            value.type = BYTES_VALUE;
            return HGet(db, keystr, field, &value.bytes_value);
        }
    }

    int Ardb::Sort(const DBID& db, const Slice& key, const StringArray& args, ValueDataArray& values)
    {
        values.clear();
        SortOptions options;
        if (parse_sort_options(options, args) < 0)
        {
            DEBUG_LOG("Failed to parse sort options.");
            return ERR_INVALID_ARGS;
        }
        int type = Type(db, key);
        ValueDataArray sortvals;
        switch (type)
        {
            case LIST_META:
            {
                LRange(db, key, 0, -1, sortvals);
                break;
            }
            case SET_META:
            {
                SMembers(db, key, sortvals);
                break;
            }
            case ZSET_META:
            {
                ZSetQueryOptions tmp;
                ZRange(db, key, 0, -1, sortvals, tmp);
                if (NULL == options.by)
                {
                    options.nosort = true;
                }
                break;
            }
            default:
            {
                return ERR_INVALID_TYPE;
            }
        }

        if (sortvals.empty())
        {
            return 0;
        }
        if (options.with_limit)
        {
            if (options.limit_offset < 0)
            {
                options.limit_offset = 0;
            }
            if ((uint32) options.limit_offset > sortvals.size())
            {
                values.clear();
                return 0;
            }
            if (options.limit_count < 0)
            {
                options.limit_count = sortvals.size();
            }
        }

        std::vector<SortValue> sortvec;
        if (!options.nosort)
        {
            if (NULL != options.by)
            {
                sortvec.reserve(sortvals.size());
            }
            for (uint32 i = 0; i < sortvals.size(); i++)
            {
                if (NULL != options.by)
                {
                    sortvec.push_back(SortValue(&sortvals[i]));
                    if (GetValueByPattern(db, options.by, sortvals[i], sortvec[i].cmp) < 0)
                    {
                        DEBUG_LOG("Failed to get value by pattern:%s", options.by);
                        sortvec[i].cmp.Clear();
                        continue;
                    }
                }
                if (options.with_alpha)
                {
                    if (NULL != options.by)
                    {
                        sortvec[i].cmp.ToBytes();
                    }
                    else
                    {
                        sortvals[i].ToBytes();
                    }
                }
                else
                {
                    if (NULL != options.by)
                    {
                        sortvec[i].cmp.ToNumber();
                    }
                    else
                    {
                        sortvals[i].ToNumber();
                    }
                }
            }
            if (NULL != options.by)
            {
                if (!options.is_desc)
                {
                    std::sort(sortvec.begin(), sortvec.end(), less_value<SortValue>);
                }
                else
                {
                    std::sort(sortvec.begin(), sortvec.end(), greater_value<SortValue>);
                }

            }
            else
            {
                if (!options.is_desc)
                {
                    std::sort(sortvals.begin(), sortvals.end(), less_value<ValueData>);
                }
                else
                {
                    std::sort(sortvals.begin(), sortvals.end(), greater_value<ValueData>);
                }
            }
        }

        if (!options.with_limit)
        {
            options.limit_offset = 0;
            options.limit_count = sortvals.size();
        }

        uint32 count = 0;
        for (uint32 i = options.limit_offset; i < sortvals.size() && count < (uint32) options.limit_count; i++, count++)
        {
            ValueData* patternObj = NULL;
            if (NULL != options.by)
            {
                patternObj = sortvec[i].value;
            }
            else
            {
                patternObj = &(sortvals[i]);
            }
            if (options.get_patterns.empty())
            {
                values.push_back(*patternObj);
            }
            else
            {
                for (uint32 j = 0; j < options.get_patterns.size(); j++)
                {
                    ValueData vo;
                    if (GetValueByPattern(db, options.get_patterns[j], *patternObj, vo) < 0)
                    {
                        DEBUG_LOG("Failed to get value by pattern for:%s", options.get_patterns[j]);
                        vo.Clear();
                    }
                    vo.ToNumber();
                    values.push_back(vo);
                }
            }
        }

        uint32 step = options.get_patterns.empty() ? 1 : options.get_patterns.size();
        switch (options.aggregate)
        {
            case AGGREGATE_SUM:
            case AGGREGATE_AVG:
            {
                ValueDataArray result;
                result.resize(step);

                for (uint32 i = 0; i < result.size(); i++)
                {
                    for (uint32 j = i; j < values.size(); j += step)
                    {
                        result[i] += values[j];
                    }
                }
                if (options.aggregate == AGGREGATE_AVG)
                {
                    size_t count = values.size() / step;
                    for (uint32 i = 0; i < result.size(); i++)
                    {
                        result[i] /= count;
                    }
                }
                values.assign(result.begin(), result.end());
                break;
            }
            case AGGREGATE_MAX:
            case AGGREGATE_MIN:
            {
                ValueDataArray result;
                result.resize(step);
                for (uint32 i = 0; i < result.size(); i++)
                {
                    for (uint32 j = i; j < values.size(); j += step)
                    {
                        if (result[i].type == EMPTY_VALUE)
                        {
                            result[i] = values[j];
                        }
                        else
                        {
                            if (options.aggregate == AGGREGATE_MIN)
                            {
                                if (values[j] < result[i])
                                {
                                    result[i] = values[j];
                                }
                            }
                            else
                            {
                                if (values[j] > result[i])
                                {
                                    result[i] = values[j];
                                }
                            }
                        }
                    }
                }
                values.assign(result.begin(), result.end());
                break;
            }
            case AGGREGATE_COUNT:
            {
                size_t size = values.size() / step;
                values.clear();
                values.push_back(ValueData((int64) size));
                break;
            }
            default:
            {
                break;
            }
        }

        if (options.store_dst != NULL && !values.empty())
        {
            BatchWriteGuard guard(GetEngine());
            LClear(db, options.store_dst);
            ValueDataArray::iterator it = values.begin();
            int64 score = 0;

            while (it != values.end())
            {
                if (it->type != EMPTY_VALUE)
                {
                    ListKeyObject lk(options.store_dst, score, db);
                    SetKeyValueObject(lk, *it);
                    score++;
                }
                it++;
            }
            ListMetaValue meta;
            meta.min_score.SetIntValue(0);
            meta.max_score.SetIntValue(score - 1);
            meta.size = score;
            SetMeta(db, options.store_dst, meta);
        }
        return 0;
    }
}


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
#include <algorithm>
#include <vector>
#include <fnmatch.h>

namespace ardb
{
    int Ardb::Sort(Context& ctx, RedisCommandFrame& cmd)
    {
        DataArray vs;
        std::string key = cmd.GetArguments()[0];
        SortOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            fill_error_reply(ctx.reply, "Invalid SORT command or invalid state for SORT.");
            return 0;
        }
        int ret = SortCommand(ctx, key, options, vs);
        ctx.reply.Clear();
        if (ret < 0)
        {
            fill_error_reply(ctx.reply, "Invalid SORT command or invalid state for SORT.");
        }
        else
        {
            fill_array_reply(ctx.reply, vs);
        }
        return 0;
    }

    struct SortValue
    {
            Data* value;
            Data cmp;
            double score;
            SortValue(Data* v) :
                    value(v), score(0)
            {
            }
            int Compare(const SortValue& other) const
            {
                if (cmp.encoding == STRING_ECODING_NIL && other.cmp.encoding == STRING_ECODING_NIL)
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

    int Ardb::MatchValueByPattern(Context& ctx, const Slice& key_pattern, const Slice& value_pattern, Data& subst,
            Data& value)
    {
        if (0 != GetValueByPattern(ctx, key_pattern, subst, value))
        {
            return -1;
        }
        std::string str;
        value.GetDecodeString(str);
        std::string vpattern(value_pattern.data(), value_pattern.size());
        return fnmatch(vpattern.c_str(), str.c_str(), 0) == 0 ? 0 : -1;
    }

    int Ardb::GetValueByPattern(Context& ctx, const Slice& pattern, Data& subst, Data& value)
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
        subst.GetDecodeString(vstr);

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

                KeyType keytype = KEY_END;
                GetType(ctx, keystr, keytype);
                switch (keytype)
                {
                    case SET_META:
                    {
                        SetLen(ctx, keystr);
                        break;
                    }
                    case LIST_META:
                    {
                        ListLen(ctx, keystr);
                        break;
                    }
                    case ZSET_META:
                    {
                       ZSetLen(ctx, keystr);
                        break;
                    }
                    default:
                    {
                        return -1;
                    }
                }
                value.SetInt64(ctx.reply.integer);
                value.ToString();
                return 0;
            }
            ValueObject vv;
            int ret = StringGet(ctx, keystr, vv);
            if(0 == ret)
            {
                value = vv.meta.str_value;
                //value.ToString();
            }
            return ret;
        }
        else
        {
            size_t pos = keystr.find("->");
            std::string field = keystr.substr(pos + 2);
            keystr = keystr.substr(0, pos);
            int ret= HashGet(ctx, keystr, field, value);
            //value.ToString();
            return ret;
        }
    }

    int Ardb::SortCommand(Context& ctx, const Slice& key, SortOptions& options, DataArray& values)
    {
        values.clear();

        KeyType keytype = KEY_END;
        GetType(ctx, key, keytype);

        switch (keytype)
        {
            case LIST_META:
            {
                ListRange(ctx, key, 0, -1);
                break;
            }
            case SET_META:
            {
                SetMembers(ctx, key);
                break;
            }
            case ZSET_META:
            {
                ZSetRange(ctx, key, 0, -1, false,false, OP_GET);
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
        DataArray sortvals;
        if(ctx.reply.MemberSize() > 0)
        {
            for(uint32 i = 0 ; i < ctx.reply.MemberSize(); i++)
            {
                Data v;
                v.SetString(ctx.reply.MemberAt(i).str, true);
                sortvals.push_back(v);
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
                    if (GetValueByPattern(ctx, options.by, sortvals[i], sortvec[i].cmp) < 0)
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
                        sortvec[i].cmp.ToString();
                    }
                    else
                    {
                        sortvals[i].ToString();
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
                    std::sort(sortvals.begin(), sortvals.end(), less_value<Data>);
                }
                else
                {
                    std::sort(sortvals.begin(), sortvals.end(), greater_value<Data>);
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
            Data* patternObj = NULL;
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
                    Data vo;
                    if (GetValueByPattern(ctx, options.get_patterns[j], *patternObj, vo) < 0)
                    {
                        DEBUG_LOG("Failed to get value by pattern for:%s", options.get_patterns[j]);
                        vo.Clear();
                    }
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
                DataArray result;
                result.resize(step);

                for (uint32 i = 0; i < result.size(); i++)
                {
                    for (uint32 j = i; j < values.size(); j += step)
                    {
                        result[i].IncrBy(values[j]);
                    }
                }
                if (options.aggregate == AGGREGATE_AVG)
                {
                    size_t count = values.size() / step;
                    for (uint32 i = 0; i < result.size(); i++)
                    {
                        result[i].SetDouble(result[i].NumberValue()/count);
                    }
                }
                values.assign(result.begin(), result.end());
                break;
            }
            case AGGREGATE_MAX:
            case AGGREGATE_MIN:
            {
                DataArray result;
                result.resize(step);
                for (uint32 i = 0; i < result.size(); i++)
                {
                    for (uint32 j = i; j < values.size(); j += step)
                    {
                        if (result[i].IsNil()){
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
                Data v;
                v.SetInt64(size);
                values.push_back(v);
                break;
            }
            default:
            {
                break;
            }
        }

        if (options.store_dst != NULL && !values.empty())
        {
            DeleteKey(ctx, options.store_dst);

            ValueObject list_meta;
            list_meta.key.key = options.store_dst;
            list_meta.key.type = KEY_META;
            list_meta.key.db = ctx.currentDB;
            list_meta.type = LIST_META;
            list_meta.meta.encoding = COLLECTION_ECODING_ZIPLIST;

            BatchWriteGuard guard(GetKeyValueEngine());
            DataArray::iterator it = values.begin();
            while (it != values.end())
            {
                if (!it->IsNil())
                {
                    ListInsert(ctx, list_meta, NULL, it->ToString(), false, false);
                }
                it++;
            }
            SetKeyValue(ctx, list_meta);
        }
        return 0;
    }
}


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
#include <algorithm>
#include <vector>

namespace ardb
{
    struct SortOptions
    {
            const char* by;
            bool with_limit;
            int32 limit_offset;
            int32 limit_count;
            std::vector<const char*> get_patterns;
            bool is_desc;
            bool with_alpha;
            bool nosort;
            const char* store_dst;
            SortOptions() :
                    by(NULL), with_limit(false), limit_offset(0), limit_count(0), is_desc(false), with_alpha(false), nosort(false), store_dst(
                    NULL)
            {
            }
            bool Parse(const std::deque<std::string>& args, uint32 idx)
            {
                for (uint32 i = idx; i < args.size(); i++)
                {
                    if (!strcasecmp(args[i].c_str(), "asc"))
                    {
                        is_desc = false;
                    }
                    else if (!strcasecmp(args[i].c_str(), "desc"))
                    {
                        is_desc = true;
                    }
                    else if (!strcasecmp(args[i].c_str(), "alpha"))
                    {
                        with_alpha = true;
                    }
                    else if (!strcasecmp(args[i].c_str(), "limit") && (i + 2) < args.size())
                    {
                        with_limit = true;
                        if (!string_toint32(args[i + 1], limit_offset) || !string_toint32(args[i + 2], limit_count))
                        {
                            return false;
                        }
                        i += 2;
                    }
                    else if (!strcasecmp(args[i].c_str(), "store") && i < args.size() - 1)
                    {
                        store_dst = args[i + 1].c_str();
                        i++;
                    }
                    else if (!strcasecmp(args[i].c_str(), "by") && i < args.size() - 1)
                    {
                        by = args[i + 1].c_str();
                        if (!strcasecmp(by, "nosort"))
                        {
                            by = NULL;
                            nosort = true;
                        }
                        i++;
                    }
                    else if (!strcasecmp(args[i].c_str(), "get") && i < args.size() - 1)
                    {
                        get_patterns.push_back(args[i + 1].c_str());
                        i++;
                    }
                    else
                    {
                        DEBUG_LOG("Invalid sort option:%s", args[i].c_str());
                        return false;
                    }
                }
                return true;
            }
    };
    struct SortValue
    {
            Data value;
            Data weight;
            bool alpha_cmp;
            bool weight_cmp;
            int Compare(const SortValue& other) const
            {
                if (!weight_cmp)
                {
                    return value.Compare(other.value, alpha_cmp);
                }
                return weight.Compare(other.weight, alpha_cmp);
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
            ValueObject vv;
            KeyObject skey(ctx.ns, KEY_META, keystr);
            if (!CheckMeta(ctx, skey, KEY_STRING, vv))
            {
                return -1;
            }
            if (vv.GetType() > 0)
            {
                value = vv.GetStringValue();
            }
            return 0;
        }
        else
        {
            size_t pos = keystr.find("->");
            std::string field = keystr.substr(pos + 2);
            keystr = keystr.substr(0, pos);
            KeyObject hfield(ctx.ns, KEY_HASH_FIELD, keystr);
            hfield.SetHashField(field);
            ValueObject hvalue;
            int err = m_engine->Get(ctx, hfield, hvalue);
            if (0 == err)
            {
                value = hvalue.GetHashValue();
            }
            return 0;
        }
    }

    int Ardb::Sort(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        SortOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            reply.SetErrorReason("Invalid SORT command or invalid state for SORT.");
            return 0;
        }
        KeyObjectArray lock_keys;
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        lock_keys.push_back(key);
        if (options.store_dst != NULL)
        {
            KeyObject store_key(ctx.ns, KEY_META, options.store_dst);
            lock_keys.push_back(store_key);
        }
        KeysLockGuard guard(ctx, lock_keys);
        ValueObject meta;
        if (!CheckMeta(ctx, key, (KeyType) 0, meta))
        {
            return 0;
        }
        std::vector<SortValue> sortvals;
        if (meta.GetType() > 0)
        {
            if (meta.GetType() != KEY_SET && meta.GetType() != KEY_ZSET && meta.GetType() != KEY_LIST)
            {
                reply.SetErrorReason("Invalid SORT command or invalid state for SORT.");
                return 0;
            }
            KeyObject startkey(ctx.ns, (KeyType) element_type((KeyType) meta.GetType()), key.GetKey());
            Iterator* iter = m_engine->Find(ctx, startkey);
            while (iter->Valid())
            {
                KeyObject& k = iter->Key(true);
                if (k.GetType() != startkey.GetType() || k.GetNameSpace() != startkey.GetNameSpace() || k.GetKey() != startkey.GetKey())
                {
                    break;
                }
                SortValue item;
                item.weight_cmp = NULL != options.by;
                item.alpha_cmp = options.with_alpha;
                switch (k.GetType())
                {
                    case KEY_ZSET_SCORE:
                    {
                        item.value = k.GetZSetMember();
                        break;
                    }
                    case KEY_SET_MEMBER:
                    {
                        item.value = k.GetSetMember();
                        break;
                    }
                    case KEY_LIST_ELEMENT:
                    {
                        item.value = iter->Value().GetListElement();
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                sortvals.push_back(item);
                iter->Next();
            }
            DELETE(iter);
        }
        if (!options.nosort)
        {
        	if (NULL != options.by)
        	{
                for (size_t i = 0; i < sortvals.size(); i++)
                {
                	if (GetValueByPattern(ctx, options.by, sortvals[i].value, sortvals[i].weight) < 0)
                	{
                		DEBUG_LOG("Failed to get value by pattern:%s", options.by);
                	    sortvals[i].weight.Clear();
                	    continue;
                	}
                }
        	}
            if (!options.is_desc)
            {
                std::sort(sortvals.begin(), sortvals.end(), less_value<SortValue>);
            }
            else
            {
                std::sort(sortvals.begin(), sortvals.end(), greater_value<SortValue>);
            }
        }
        if (!options.with_limit)
        {
            options.limit_offset = 0;
            options.limit_count = sortvals.size();
        }

        DataArray value_list;
        value_list.reserve(sortvals.size());
        uint32 count = 0;
        for (uint32 i = options.limit_offset; i < sortvals.size() && count < (uint32) options.limit_count; i++, count++)
        {
            if (options.get_patterns.empty())
            {
                value_list.push_back(sortvals[i].value);
            }
            else
            {
                for (uint32 j = 0; j < options.get_patterns.size(); j++)
                {
                    Data vo;
                    if (GetValueByPattern(ctx, options.get_patterns[j], sortvals[i].value, vo) < 0)
                    {
                        DEBUG_LOG("Failed to get value by pattern for:%s", options.get_patterns[j]);
                        vo.Clear();
                    }
                    value_list.push_back(vo);
                }
            }
        }

        if (options.store_dst != NULL)
        {
            WriteBatchGuard batch(ctx, m_engine);
            DelKey(ctx, options.store_dst);
            KeyObject store_key(ctx.ns, KEY_META, options.store_dst);
            ValueObject store_meta;
            store_meta.SetType(KEY_LIST);
            store_meta.SetObjectLen(value_list.size());
            store_meta.GetMetaObject().list_sequential = true;
            store_meta.SetListMinIdx(0);
            DataArray::iterator it = value_list.begin();
            int64_t idx = 0;
            while (it != value_list.end())
            {
                KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, options.store_dst);
                ele_key.SetListIndex(idx);
                ValueObject ele_value;
                ele_value.SetType(KEY_LIST_ELEMENT);
                ele_value.SetListElement(*it);
                SetKeyValue(ctx, ele_key, ele_value);
                idx++;
                it++;
            }
            store_meta.SetListMaxIdx(idx);
            SetKeyValue(ctx, store_key, store_meta);
        }
        else
        {
            reply.ReserveMember(0);
            DataArray::iterator it = value_list.begin();
            while (it != value_list.end())
            {
                RedisReply& r = reply.AddMember();
                r.SetString(*it);
                it++;
            }
        }
        if (options.store_dst != NULL)
        {
            if (ctx.transc_err != 0)
            {
                reply.SetErrCode(ctx.transc_err);
            }
            else
            {
                reply.SetInteger(value_list.size());
            }
        }
        return 0;
    }
}


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
            Data* value;
            Data cmp;
            double score;
            SortValue(Data* v) :
                    value(v), score(0)
            {
            }
            int Compare(const SortValue& other) const
            {
                if (cmp.IsNil() && other.cmp.IsNil())
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
    int Ardb::Sort(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        SortOptions options;
        if (!options.Parse(cmd.GetArguments(), 1))
        {
            reply.SetErrorReason("Invalid SORT command or invalid state for SORT.");
            return 0;
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        if (!CheckMeta(ctx, key, (KeyType) 0, meta))
        {
            return 0;
        }
        if(meta.GetType() > 0)
        {
            if (meta.GetType() != KEY_SET && meta.GetType() != KEY_ZSET && meta.GetType() != KEY_LIST)
            {
                reply.SetErrorReason("Invalid SORT command or invalid state for SORT.");
                return 0;
            }
        }

        return 0;
    }
}


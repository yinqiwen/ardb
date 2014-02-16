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
#include "level1_cache.hpp"
#include "ardb.hpp"
#include <algorithm>

namespace ardb
{
    ZSetCache::ZSetCache() :
            CacheItem((uint8) ZSET_META)
    {
        m_estimate_mem_size = sizeof(ZSetCache);
    }

    void ZSetCache::DirectAdd(ValueData& score, ValueData& v, ValueData& attr)
    {
        Buffer buf1, buf2;
        v.Encode(buf1);
        attr.Encode(buf2);
        ZSetCaheElement e;
        e.value.assign(buf1.GetRawReadBuffer(), buf1.ReadableBytes());
        e.score = score.NumberValue();
        e.attr.assign(buf2.GetRawReadBuffer(), buf2.ReadableBytes());
        m_cache.push_back(e);
        m_estimate_mem_size += sizeof(ZSetCaheElement);
        m_estimate_mem_size += e.value.size();
        m_estimate_mem_size += e.attr.size();
    }

    void ZSetCache::GetRange(const ZRangeSpec& range, bool with_scores, bool with_attrs, ValueDataArray& res)
    {
        ZSetCaheElement min_ele(range.min.NumberValue(), "");
        ZSetCaheElement max_ele(range.max.NumberValue(), "");
        ZSetCaheElementDeque::iterator min_it = std::lower_bound(m_cache.begin(), m_cache.end(), min_ele);
        ZSetCaheElementDeque::iterator max_it = std::lower_bound(m_cache.begin(), m_cache.end(), max_ele);
        if (min_it != m_cache.end())
        {
            while (!range.contain_min && min_it != m_cache.end() && min_it->score == range.min.NumberValue())
            {
                min_it++;
            }
            while (!range.contain_max && max_it != m_cache.end() && max_it->score == range.max.NumberValue())
            {
                max_it--;
            }
            while (min_it <= max_it && min_it != m_cache.end())
            {
                ValueData v;
                Buffer buf(const_cast<char*>(min_it->value.data()), 0, min_it->value.size());
                v.Decode(buf);
                res.push_back(v);
                if (with_scores)
                {
                    ValueData score;
                    score.SetDoubleValue(min_it->score);
                    res.push_back(score);
                }
                if (with_attrs)
                {
                    ValueData attr_value;
                    Buffer attr_buf(const_cast<char*>(min_it->attr.data()), 0, min_it->attr.size());
                    attr_value.Decode(attr_buf);
                    res.push_back(attr_value);
                }
                min_it++;
            }
        }
    }
}


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

#ifndef REDIS_HELPER_HPP_
#define REDIS_HELPER_HPP_

#include "channel/codec/redis_reply.hpp"
#include "data_format.hpp"
#include "string_helper.hpp"
#include <stdio.h>

using ardb::codec::RedisReply;


namespace ardb
{
    void fill_error_reply(RedisReply& reply, const char* fmt, ...);
    void fill_fix_error_reply(RedisReply& reply, const std::string& err);

    void fill_status_reply(RedisReply& reply, const char* s);

    void fill_int_reply(RedisReply& reply, int64 v);
    void fill_double_reply(RedisReply& reply, double v);

    void fill_str_reply(RedisReply& reply, const std::string& v);

    void fill_value_reply(RedisReply& reply, const ValueData& v);

    bool check_uint32_arg(RedisReply& reply, const std::string& arg, uint32& v);

    bool check_uint64_arg(RedisReply& reply, const std::string& arg, uint64& v);

    bool check_double_arg(RedisReply& reply, const std::string& arg, double& v);

    template<typename T>
    void fill_array_reply(RedisReply& reply, T& v)
    {
        reply.type = REDIS_REPLY_ARRAY;
        typename T::iterator it = v.begin();
        while (it != v.end())
        {
            ValueData& vo = *it;
            RedisReply r;
            if (vo.type == EMPTY_VALUE)
            {
                r.type = REDIS_REPLY_NIL;
            }
            else
            {
                std::string str;
                vo.ToBytes();
                fill_str_reply(r, vo.bytes_value);
            }
            reply.elements.push_back(r);
            it++;
        }
    }

    template<typename T>
    void fill_str_array_reply(RedisReply& reply, T& v)
    {
        reply.type = REDIS_REPLY_ARRAY;
        typename T::iterator it = v.begin();
        while (it != v.end())
        {
            RedisReply r;
            fill_str_reply(r, *it);
            reply.elements.push_back(r);
            it++;
        }
    }

    template<typename T>
    void fill_int_array_reply(RedisReply& reply, T& v)
    {
        reply.type = REDIS_REPLY_ARRAY;
        typename T::iterator it = v.begin();
        while (it != v.end())
        {
            RedisReply r;
            fill_int_reply(r, *it);
            reply.elements.push_back(r);
            it++;
        }
    }
}

#endif /* REDIS_HELPER_HPP_ */

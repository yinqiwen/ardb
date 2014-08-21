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

#include "redis_helper.hpp"
#include <stdarg.h>

namespace ardb
{
    void fill_error_reply(RedisReply& reply, const char* fmt, ...)
    {
        va_list ap;
        va_start(ap, fmt);
        char buf[1024];
        sprintf(buf, "%s", "ERR ");
        vsnprintf(buf + 4, sizeof(buf) - 5, fmt, ap);
        va_end(ap);
        reply.type = REDIS_REPLY_ERROR;
        reply.str = buf;
    }
    void fill_fix_error_reply(RedisReply& reply, const std::string& err)
    {
        reply.type = REDIS_REPLY_ERROR;
        reply.str = err;
    }

    void fill_status_reply(RedisReply& reply, const char* s)
    {
        reply.type = REDIS_REPLY_STATUS;
        reply.str = s;
    }

    void fill_int_reply(RedisReply& reply, int64 v)
    {
        reply.type = REDIS_REPLY_INTEGER;
        reply.integer = v;
    }
    void fill_double_reply(RedisReply& reply, double v)
    {
        reply.type = REDIS_REPLY_DOUBLE;
        reply.double_value = v;
    }

    void fill_str_reply(RedisReply& reply, const std::string& v)
    {
        reply.type = REDIS_REPLY_STRING;
        reply.str = v;
    }

    void fill_value_reply(RedisReply& reply, const Data& v)
    {
        reply.type = REDIS_REPLY_STRING;
        std::string str;
        v.GetDecodeString(str);
        reply.str = str;
    }

    bool check_uint32_arg(RedisReply& reply, const std::string& arg, uint32& v)
    {
        if (!string_touint32(arg, v))
        {
            fill_error_reply(reply, "value is not an integer or out of range.");
            return false;
        }
        return true;
    }

    bool check_uint64_arg(RedisReply& reply, const std::string& arg, uint64& v)
    {
        if (!string_touint64(arg, v))
        {
            fill_error_reply(reply, "value is not an integer or out of range.");
            return false;
        }
        return true;
    }

    bool check_double_arg(RedisReply& reply, const std::string& arg, double& v)
    {
        if (!string_todouble(arg, v))
        {
            fill_error_reply(reply, "value is not an double or out of range.");
            return false;
        }
        return true;
    }
}


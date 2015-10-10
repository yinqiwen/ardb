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
#include <float.h>
#include <cmath>

OP_NAMESPACE_BEGIN

    int Ardb::LIndex(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::LLen(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::LPop(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::LInsert(Context& ctx, RedisCommandFrame& cmd)
    {
    }
    int Ardb::LPush(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::LPushx(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::ListRange(Context& ctx, const Slice& key, int64 start, int64 end)
    {
        return 0;
    }

    int Ardb::LRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::LRem(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::LSet(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::LTrim(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::RPop(Context& ctx, RedisCommandFrame& cmd)
    {
    }
    int Ardb::RPush(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::RPushx(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::RPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::BLPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::BRPop(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }
    int Ardb::BRPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

OP_NAMESPACE_END


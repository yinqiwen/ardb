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

OP_NAMESPACE_BEGIN
    int Ardb::KeysCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Randomkey(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Scan(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Keys(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Rename(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::RenameNX(Context& ctx, RedisCommandFrame& cmd)
    {
        Rename(ctx, cmd);
        return 0;
    }

    int Ardb::Move(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::Type(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Persist(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::GenericExpire(Context& ctx, const KeyObject& key, uint64 ms)
    {
        int64 ttl = ms + get_current_epoch_millis();
        KeyObject ttl_key(ctx.ns, key.GetStringKey(), KEY_TTL_VALUE);

        MergeOperation op;
        op.op = REDIS_CMD_PEXPIREAT;
        op.Add().SetInt64(key.GetKeyType());
        op.Add().SetInt64(ms);
        return m_engine.Merge(ctx, ttl_key, op);
    }

    int Ardb::PExpire(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::PTTL(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::TTL(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Expire(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Expireat(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Exists(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

OP_NAMESPACE_END


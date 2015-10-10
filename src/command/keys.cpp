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
        KeyObject ttl_data_key(key.db, KEY_TTL_DATA);
        ttl_data_key.elements[0].SetInt64(key.type);
        ttl_data_key.elements[1] = key.elements[0];
        KeyObject ttl_sort_key(key.db, KEY_TTL_SORT);
        ttl_sort_key.elements[0].SetInt64(ttl);
        ttl_sort_key.elements[1].SetInt64(key.type);
        ttl_sort_key.elements[2] = key.elements[0];

        ValueObject ttl_data_value;
        ttl_data_value.value.SetInt64(ttl);
        m_engine.Put(ctx, ttl_data_key, ttl_data_value);
        m_engine.Put(ctx, ttl_sort_key, ValueObject());
        return 0;
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
        fill_error_reply(ctx.reply, "Use StrTTL/HTTL/STTL/ZTTL/LTTL instead.");
        return 0;
    }

    int Ardb::Expire(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_error_reply(ctx.reply, "Use StrExpire/HExpire/SExpire/ZExpire/LExpire instead.");
        return 0;
    }

    int Ardb::Expireat(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_error_reply(ctx.reply, "Use StrExpireat/HExpireat/SExpireat/ZExpireat/LExpireat instead.");
        return 0;
    }

    int Ardb::Exists(Context& ctx, RedisCommandFrame& cmd)
    {

        //fill_error_reply(ctx.reply, "Use StrExists/HExists/SExists/ZExists/LExists instead.");
        return 0;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_error_reply(ctx.reply, "Use StrDel/HClear/SClear/ZClear/LClear instead.");
        return 0;
    }

OP_NAMESPACE_END


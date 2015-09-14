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
        KeysOptions opt;
        opt.op = OP_COUNT;
        opt.pattern = cmd.GetArguments()[0];
        KeysOperation(ctx, opt);
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

    int Ardb::KeysOperation(Context& ctx, const KeysOptions& options)
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
        DBID dst = 0;
        if (!string_touint32(cmd.GetArguments()[1], dst))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        RedisCommandFrame exists("Exists");
        exists.AddArg(cmd.GetArguments()[0]);
        Context tmpctx;
        tmpctx.currentDB = dst;
        Exists(tmpctx, exists);
        if (ctx.reply.integer == 1)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        KeyType type = KEY_END;
        GetType(ctx, cmd.GetArguments()[0], type);
        switch (type)
        {
            case SET_META:
            {
                RenameSet(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            case LIST_META:
            {
                RenameList(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            case ZSET_META:
            {
                RenameZSet(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            case HASH_META:
            {
                RenameHash(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            case STRING_META:
            {
                RenameString(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            case BITSET_META:
            {
                RenameBitset(ctx, ctx.currentDB, cmd.GetArguments()[0], dst, cmd.GetArguments()[0]);
                break;
            }
            default:
            {
                fill_error_reply(ctx.reply, "Invalid type to move");
                break;
            }
        }
        fill_int_reply(ctx.reply, 1);
        return 0;
    }

    int Ardb::Type(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyType type = KEY_END;
        GetType(ctx, cmd.GetArguments()[0], type);
        switch (type)
        {
            case SET_META:
            {
                fill_status_reply(ctx.reply, "set");
                break;
            }
            case LIST_META:
            {
                fill_status_reply(ctx.reply, "list");
                break;
            }
            case ZSET_META:
            {
                fill_status_reply(ctx.reply, "zset");
                break;
            }
            case HASH_META:
            {
                fill_status_reply(ctx.reply, "hash");
                break;
            }
            case STRING_META:
            {
                fill_status_reply(ctx.reply, "string");
                break;
            }
            case BITSET_META:
            {
                fill_status_reply(ctx.reply, "bitset");
                break;
            }
            default:
            {
                fill_status_reply(ctx.reply, "none");
                break;
            }
        }
        return 0;
    }

    int Ardb::Persist(Context& ctx, RedisCommandFrame& cmd)
    {
        GenericExpire(ctx, cmd.GetArguments()[0], 0);
        return 0;
    }

    int Ardb::GenericExpire(Context& ctx, const Slice& key, uint64 ms)
    {
        if (ms > 0 && get_current_epoch_millis() >= ms)
        {
            return 0;
        }
        if (m_cfg.slave_ignore_expire && ctx.IsSlave())
        {
            /*
             * ignore expire setting, but issue data change event for replication
             */
            ctx.data_change = true;
            return 0;
        }

        ValueObject meta;
        int err = GetMetaValue(ctx, key, KEY_END, meta);
        if (0 != err)
        {
            return err;
        }
        BatchWriteGuard guard(ctx);
        if (meta.meta.expireat != ms && meta.meta.expireat > 0)
        {
            KeyObject expire;
            expire.key = key;
            expire.type = KEY_EXPIRATION_ELEMENT;
            expire.db = ctx.currentDB;
            expire.score.SetInt64(meta.meta.expireat);
            DelKeyValue(ctx, expire);
        }
        meta.meta.expireat = ms;
        SetKeyValue(ctx, meta);

        if (meta.meta.expireat > 0)
        {
            ValueObject vv(KEY_EXPIRATION_ELEMENT);
            vv.key.key = key;
            vv.key.type = KEY_EXPIRATION_ELEMENT;
            vv.key.db = ctx.currentDB;
            vv.key.score.SetInt64(meta.meta.expireat);
            SetKeyValue(ctx, vv);
        }
        return 0;
    }

    int Ardb::PExpire(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 v = 0;
        if (!check_uint32_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        ValueObject meta;
        int err = GenericExpire(ctx, cmd.GetArguments()[0], v + get_current_epoch_millis());
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        fill_int_reply(ctx.reply, err == 0 ? 1 : 0);
        return 0;
    }
    int Ardb::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = GenericExpire(ctx, cmd.GetArguments()[0], v);
        fill_int_reply(ctx.reply, err == 0 ? 1 : 0);
        return 0;
    }

    int Ardb::GenericTTL(Context& ctx, const Slice& key, uint64& ms)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, KEY_END, meta);
        if (0 != err)
        {
            if (err == ERR_NOT_EXIST)
            {
                fill_int_reply(ctx.reply, -2);
            }
            return err;
        }
        ms = meta.meta.expireat;
        if (0 == ms)
        {
            fill_int_reply(ctx.reply, -1);
            return -1;
        }
        return 0;
    }
    int Ardb::PTTL(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 ms;
        int err = GenericTTL(ctx, cmd.GetArguments()[0], ms);
        if (0 == err)
        {
            fill_int_reply(ctx.reply, ms - get_current_epoch_millis());
        }
        return 0;
    }
    int Ardb::TTL(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 ms;
        int err = GenericTTL(ctx, cmd.GetArguments()[0], ms);
        if (0 == err)
        {
            fill_int_reply(ctx.reply, (ms - get_current_epoch_millis()) / 1000);
        }
        return 0;
    }

    int Ardb::Expire(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = GenericExpire(ctx, cmd.GetArguments()[0], v * 1000 + get_current_epoch_millis());
        fill_int_reply(ctx.reply, err == 0 ? 1 : 0);
        return 0;
    }

    int Ardb::Expireat(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = GenericExpire(ctx, cmd.GetArguments()[0], v * 1000);
        fill_int_reply(ctx.reply, err == 0 ? 1 : 0);
        return 0;
    }

    int Ardb::Exists(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], KEY_END, meta);
        fill_int_reply(ctx.reply, err == 0 ? 1 : 0);
        return 0;
    }

    int Ardb::DeleteKey(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        meta.key.type = KEY_META;
        meta.key.db = ctx.currentDB;
        meta.key.key = key;
        if (0 != GetKeyValue(ctx, meta.key, &meta))
        {
            return 0;
        }
        meta.key.meta_type = meta.type;
        switch (meta.type)
        {
            case SET_META:
            {
                SClear(ctx, meta);
                break;
            }
            case LIST_META:
            {
                LClear(ctx, meta);
                break;
            }
            case ZSET_META:
            {
                ZClear(ctx, meta);
                break;
            }
            case HASH_META:
            {
                HClear(ctx, meta);
                break;
            }
            case STRING_META:
            {
                DelKeyValue(ctx, meta.key);
                break;
            }
            case BITSET_META:
            {
                BitClear(ctx, meta);
                break;
            }
            default:
            {
                return 0;
            }
        }
        if(ctx.reply.type == REDIS_REPLY_ERROR)
        {
            return 0;
        }
        if (meta.meta.expireat > 0)
        {
            KeyObject expire;
            expire.type = KEY_EXPIRATION_ELEMENT;
            expire.db = ctx.currentDB;
            expire.key = key;
            expire.score.SetInt64(meta.meta.expireat);
            DelKeyValue(ctx, expire);
        }
        return 1;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        if (ctx.IsSlave() && m_cfg.slave_ignore_del)
        {
            return 0;
        }
        BatchWriteGuard guard(ctx);
        int count = 0;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            count += DeleteKey(ctx, cmd.GetArguments()[i]);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }


OP_NAMESPACE_END


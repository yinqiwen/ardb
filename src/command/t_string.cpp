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

    int Ardb::GenericGet(Context& ctx, const Slice& key, ValueObject& v, GenericGetOptions& options)
    {
        int err = GetMetaValue(ctx, key, STRING_META, v);
        if (0 == err)
        {
            ctx.reply.type = REDIS_REPLY_STRING;
            if (options.fill_reply)
            {
                const Data& data = v.meta.str_value;
                data.GetDecodeString(ctx.reply.str);
            }
        }
        else
        {
            if (err == ERR_NOT_EXIST)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            else
            {
                CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            }
        }
        return 0;
    }

    int Ardb::StringGet(Context& ctx, const std::string& key, ValueObject& value)
    {
        GenericGetOptions options;
        return GenericGet(ctx, key, value, options);
    }

    int Ardb::Get(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        ValueObject v;
        return StringGet(ctx, key, v);
    }
    int Ardb::MGet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            ValueObject v;
            RedisReply& reply = ctx.reply.AddMember();
            int err = GetMetaValue(ctx, cmd.GetArguments()[i], STRING_META, v);
            if (0 == err)
            {
                reply.type = REDIS_REPLY_STRING;
                v.meta.str_value.GetDecodeString(reply.str);
            }
            else
            {
                reply.type = REDIS_REPLY_NIL;
            }
        }
        return 0;
    }

    int Ardb::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& append = cmd.GetArguments()[1];
        ValueObject v;
        int err = GetMetaValue(ctx, key, STRING_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 == err)
        {
            Data& data = v.meta.str_value;
            data.ToString();
            data.value.sv = sdscatlen(data.value.sv, append.data(), append.size());
        }
        else if (err == ERR_NOT_EXIST)
        {
            v.meta.str_value.SetString(append, false);
        }
        err = SetKeyValue(ctx, v);
        CHECK_WRITE_RETURN_VALUE(ctx, err);
        fill_int_reply(ctx.reply, sdslen(v.meta.str_value.RawString()));
        return 0;
    }

    int Ardb::GenericSet(Context& ctx, const Slice& key, const Slice& value, GenericSetOptions& options)
    {
        if (options.with_expire && options.expire <= 0)
        {
            if (options.fill_reply)
            {
                fill_error_reply(ctx.reply, "invalid expire time in SETEX");
            }
            return 0;
        }
        int64 expire_mills = options.expire;
        if (options.with_expire)
        {
            if (options.unit == SECONDS)
            {
                expire_mills *= 1000;
            }
            expire_mills += get_current_epoch_millis();
        }
        Context& dbctx = (ctx);
        if (options.with_nx || options.with_xx)
        {
            ValueObject v;
            int err = GetMetaValue(dbctx, key, STRING_META, v);
            if (options.fill_reply)
            {
                CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            }
            if ((options.with_nx && 0 == err) || (options.with_xx && err == ERR_NOT_EXIST))
            {
                if (options.fill_reply)
                {
                    if (options.abort_reply.type != 0)
                    {
                        ctx.reply.Clone(options.abort_reply);
                    }
                    else
                    {
                        ctx.reply.Clear();
                        ctx.reply.type = REDIS_REPLY_NIL;
                    }
                }
                return 0;
            }
        }

        ValueObject v(STRING_META);
        v.key.type = KEY_META;
        v.key.key = key;
        v.key.db = ctx.currentDB;
        v.meta.str_value.SetString(value, true);
        v.meta.expireat = expire_mills;
        BatchWriteGuard guard(ctx, options.with_expire);
        int err = SetKeyValue(dbctx, v);
        CHECK_WRITE_RETURN_VALUE(ctx, err);
        if (options.with_expire)
        {
            ValueObject expire_key(KEY_EXPIRATION_ELEMENT);
            expire_key.key.type = KEY_EXPIRATION_ELEMENT;
            expire_key.key.key = key;
            expire_key.key.db = ctx.currentDB;
            expire_key.key.score.SetInt64(expire_mills);
            SetKeyValue(dbctx, expire_key);
        }
        if (options.fill_reply)
        {
            if (options.ok_reply.type != 0)
            {
                ctx.reply.Clone(options.ok_reply);
            }
            else
            {
                fill_status_reply(ctx.reply, "OK");
            }
        }
        return 0;
    }

    int Ardb::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 mills;
        if (!string_touint32(cmd.GetArguments()[1], mills))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        GenericSetOptions options;
        options.with_expire = true;
        options.expire = mills;
        options.unit = MILLIS;
        GenericSet(ctx, cmd.GetArguments()[0], cmd.GetArguments()[2], options);
        return 0;
    }

    int Ardb::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSET");
            return 0;
        }
        GenericSetOptions options;
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            options.with_nx = true;
        }
        BatchWriteGuard guard(ctx);
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            int err = GenericSet(ctx, cmd.GetArguments()[i], cmd.GetArguments()[i + 1], options);
            if (0 != err || ctx.reply.type == REDIS_REPLY_NIL)
            {
                guard.MarkFailed();
                break;
            }
        }
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            fill_int_reply(ctx.reply, guard.Success() ? 1 : 0);
        }
        else
        {
            fill_status_reply(ctx.reply, "OK");
        }
        return 0;
    }

    int Ardb::MSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MSet(ctx, cmd);
    }

    int Ardb::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        double increment, val;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        KeyLockerGuard guard(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        GenericGetOptions options;
        ValueObject v;
        GenericGet(ctx, cmd.GetArguments()[0], v, options);
        if (ctx.reply.type == REDIS_REPLY_ERROR)
        {
            return 0;
        }
        if (!GetDoubleValue(ctx, ctx.reply.str, val))
        {
            return 0;
        }
        val += increment;
        std::string tmp;
        tmp.assign(v.meta.str_value.RawString(), sdslen(v.meta.str_value.RawString()));
        fast_dtoa(val, 10, tmp);
        v.meta.str_value.SetString(tmp, false);
        int err = SetKeyValue(ctx, v);
        CHECK_WRITE_RETURN_VALUE(ctx, err);
        fill_double_reply(ctx.reply, val);

        RedisCommandFrame rewrite("set");
        rewrite.AddArg(tmp);
        RewriteClientCommand(ctx, rewrite);
        return 0;
    }

    int Ardb::IncrDecrCommand(Context& ctx, const Slice& key, int64 incr)
    {
        KeyLockerGuard guard(m_key_lock, ctx.currentDB, key);
        ValueObject val;
        GenericGetOptions options;
        options.fill_reply = false;
        GenericGet(ctx, key, val, options);
        if (ctx.reply.type == REDIS_REPLY_ERROR)
        {
            return 0;
        }
        else if (ctx.reply.type == REDIS_REPLY_NIL)
        {
            val.meta.str_value.SetInt64(0);
        }
        int64 oldvalue = 0;
        if (!val.meta.str_value.GetInt64(oldvalue))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue))
                || (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue)))
        {
            fill_error_reply(ctx.reply, "increment or decrement would overflow");
            return 0;
        }
        val.meta.str_value.SetInt64(oldvalue + incr);
        int err = SetKeyValue(ctx, val);
        CHECK_WRITE_RETURN_VALUE(ctx, err);
        fill_int_reply(ctx.reply, oldvalue + incr);
        return 0;
    }

    int Ardb::Incrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], increment);
    }

    int Ardb::Incr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], 1);
    }

    int Ardb::Decrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], 0 - increment);
    }

    int Ardb::Decr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -1);
    }

    int Ardb::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        GenericGetOptions get_options;
        ValueObject val;
        GenericGet(ctx, cmd.GetArguments()[0], val, get_options);
        if (ctx.reply.type != REDIS_REPLY_ERROR)
        {
            GenericSetOptions set_options;
            set_options.fill_reply = false;
            val.meta.str_value.SetString(cmd.GetArguments()[1], true);
            int err = SetKeyValue(ctx, val);
            CHECK_WRITE_RETURN_VALUE(ctx, err);
        }
        return 0;
    }

    int Ardb::GetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        GenericGetOptions get_options;
        ValueObject val;
        GenericGet(ctx, cmd.GetArguments()[0], val, get_options);
        if (ctx.reply.type == REDIS_REPLY_STRING)
        {
            if (start < 0)
                start = ctx.reply.str.size() + start;
            if (end < 0)
                end = ctx.reply.str.size() + end;
            if (start < 0)
                start = 0;
            if (end < 0)
                end = 0;
            if ((unsigned) end >= ctx.reply.str.size())
                end = ctx.reply.str.size() - 1;

            /* Precondition: end >= 0 && end < strlen, so the only condition where
             * nothing can be returned is: start > end. */
            if (start > end)
            {
                ctx.reply.str.clear();
            }
            else
            {
                ctx.reply.str = ctx.reply.str.substr(start, end - start + 1);
            }
        }
        else if (ctx.reply.type == REDIS_REPLY_NIL)
        {
            ctx.reply.type = REDIS_REPLY_STRING;
        }
        return 0;
    }

    int Ardb::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        GenericSetOptions options;
        if (cmd.GetArguments().size() > 2)
        {
            uint32 i = 0;
            uint64 px = 0, ex = 0;
            bool syntaxerror = false;
            for (i = 2; i < cmd.GetArguments().size(); i++)
            {
                const std::string& arg = cmd.GetArguments()[i];
                if (!strcasecmp(arg.c_str(), "px") || !strcasecmp(arg.c_str(), "ex") )
                {
                    int64 iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    if (!strcasecmp(arg.c_str(), "px"))
                    {
                        px = iv;
                        options.with_expire = true;
                        options.expire = px;
                        options.unit = MILLIS;
                    }
                    else
                    {
                        ex = iv;
                        options.with_expire = true;
                        options.expire = ex;
                        options.unit = SECONDS;
                    }
                    i++;
                }else if(!strcasecmp(arg.c_str(), "xx"))
                {
                    options.with_xx = true;
                }else if(!strcasecmp(arg.c_str(), "nx"))
                {
                    options.with_nx = true;
                }
                else
                {
                    syntaxerror = true;
                    break;
                }
            }
            if (syntaxerror)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
        }
        return GenericSet(ctx, key, value, options);
    }

    int Ardb::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 secs;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], secs))
        {
            return 0;
        }
        GenericSetOptions options;
        options.expire = secs;
        options.unit = SECONDS;
        options.with_expire = true;
        return GenericSet(ctx, cmd.GetArguments()[0], cmd.GetArguments()[2], options);
    }
    int Ardb::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        GenericSetOptions options;
        options.with_nx = true;
        fill_int_reply(options.abort_reply, 0);
        fill_int_reply(options.ok_reply, 1);
        return GenericSet(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1], options);
    }
    int Ardb::SetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 offset;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], offset))
        {
            return 0;
        }
        const std::string& value = cmd.GetArguments()[2];
        if (offset < 0)
        {
            fill_error_reply(ctx.reply, "offset is out of range");
            return 0;
        }
        KeyLockerGuard guard(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        GenericGetOptions get_options;
        ValueObject val;
        GenericGet(ctx, cmd.GetArguments()[0], val, get_options);
        if (ctx.reply.type == REDIS_REPLY_ERROR)
        {
            return 0;
        }
        if (ctx.reply.str.empty())
        {
            if (value.empty())
            {
                fill_int_reply(ctx.reply, 0);
                return 0;
            }
        }

        if (!value.empty())
        {
            val.meta.str_value.ToString();
            sds ss = val.meta.str_value.value.sv;
            if (ss != NULL && offset + value.size() > ctx.reply.str.size())
            {
                ss = sdsgrowzero(ss, offset + value.size());
            }
            else
            {
                ss = sdsnewlen(NULL, offset + value.size());
                memset(ss, 0, offset + value.size());
            }
            char* start = ss + offset;
            memcpy(start, value.data(), value.size());
            val.meta.str_value.value.sv = ss;
            int err = SetKeyValue(ctx, val);
            CHECK_WRITE_RETURN_VALUE(ctx, err);
        }
        fill_int_reply(ctx.reply, val.meta.str_value.StringLength());
        return 0;
    }
    int Ardb::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
        int ret = Get(ctx, cmd);
        if (0 == ret)
        {
            fill_int_reply(ctx.reply, ctx.reply.str.size());
        }
        return 0;
    }

    int Ardb::RenameString(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey)
    {
        Context tmpctx;
        tmpctx.currentDB = srcdb;
        ValueObject v;
        int err = GetMetaValue(tmpctx, srckey, STRING_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if(0 != err)
        {
            return 0;
        }
        DeleteKey(tmpctx, srckey);
        v.key.encode_buf.Clear();
        v.key.db = dstdb;
        v.meta.expireat = 0;
        err = SetKeyValue(ctx, v);
        CHECK_WRITE_RETURN_VALUE(ctx, err);
        return 0;
    }

OP_NAMESPACE_END


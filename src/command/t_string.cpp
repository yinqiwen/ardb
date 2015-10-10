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

    int Ardb::Get(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        KeyObject k(ctx.currentDB, KEY_STRING, key);
        ValueObject v;
        ctx.reply.type = REDIS_REPLY_STRING;
        int err = m_engine.Get(ctx, k, v);
        if (err == ERR_ENTRY_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            v.value.ToString(ctx.reply.str);
        }
        return 0;
    }
    int Ardb::MGet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        KeyObjectArray ks;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject k(ctx.currentDB, KEY_STRING, cmd.GetArguments()[i]);
            ks.push_back(k);
        }
        ValueObjectArray vs;
        std::vector<int> errs;
        m_engine.MultiGet(ctx, ks, vs, &errs);
        for (size_t i = 0; i < ks.size(); i++)
        {
            RedisReply& r = ctx.reply.AddMember();
            if (errs[i] != 0)
            {
                r.type = REDIS_REPLY_NIL;
            }
            else
            {
                fill_value_reply(ctx.reply, vs[i].value);
            }
        }
        return 0;
    }

    int Ardb::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& append = cmd.GetArguments()[1];
        KeyObject key(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject val;
        val.value = cmd.GetArguments()[1];
        int err = m_engine.Merge(ctx, key, cmd.GetType(), val);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            /*
             * just return appended value size
             */
            fill_int_reply(ctx.reply, cmd.GetArguments()[1].size());
            FireKeyChangedEvent(ctx, key);
        }
        return 0;
    }

    int Ardb::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 mills;
        KeyObject key(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        if (!string_touint32(cmd.GetArguments()[1], mills))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        ValueObject val;
        val.value.SetString(cmd.GetArguments()[1], true);
        {
            BatchWriteGuard guard(ctx);
            GenericExpire(ctx, key, mills);
            m_engine.Put(ctx, key, val);
        }
        if (ctx.WriteSuccess())
        {
            FillErrorReply(ctx, ctx.write_err);
        }
        else
        {
            fill_ok_reply(ctx.reply);
            FireKeyChangedEvent(ctx, key);
        }
        return 0;
    }

    int Ardb::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSET");
            return 0;
        }
        {
            BatchWriteGuard guard(ctx);
            for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
            {
                KeyObject key(ctx.currentDB, KEY_STRING, cmd.GetArguments()[i]);
                if (cmd.GetType() == REDIS_CMD_MSETNX)
                {
                    if (m_engine.Exists(ctx, key))
                    {
                        guard.MarkFailed(ERR_KEY_EXIST);
                        break;
                    }
                }
                ValueObject value;
                value.value.SetString(cmd.GetArguments()[i + 1], true);
                m_engine.Put(ctx, key, value);
            }
        }
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            fill_int_reply(ctx.reply, ctx.WriteSuccess() ? 1 : 0);
        }
        else
        {
            fill_ok_reply(ctx.reply);
        }
        return 0;
    }

    int Ardb::MSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MSet(ctx, cmd);
    }

    int Ardb::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        long double increment;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        KeyObject key(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        value.value.SetString(cmd.GetArguments()[1], true);
        int err = m_engine.Merge(ctx, key, REDIS_CMD_INCRBYFLOAT, value);
        if (err == 0)
        {
            fill_double_reply(ctx.reply, increment);
            FireKeyChangedEvent(ctx, key);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Ardb::IncrDecrCommand(Context& ctx, const std::string& key, int64 incr)
    {
        KeyObject keyobj(ctx.currentDB, KEY_STRING, key);
        ValueObject value;
        value.value.SetInt64(incr);
        int err = m_engine.Merge(ctx, keyobj, REDIS_CMD_INCRBY, value);
        if (0 == err)
        {
            fill_int_reply(ctx.reply, incr);
            FireKeyChangedEvent(ctx, keyobj);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
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
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -increment);
    }

    int Ardb::Decr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -1);
    }

    int Ardb::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject keyobj(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (err < 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            if (err == ERR_ENTRY_NOT_EXIST)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            else
            {
                fill_value_reply(ctx.reply, value.value);
            }
            value.value.SetString(cmd.GetArguments()[1], true);
            err = m_engine.Put(ctx, keyobj, value);
            if (0 != err)
            {
                FillErrorReply(ctx, err);
            }
            else
            {
                FireKeyChangedEvent(ctx, keyobj);
            }
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
        KeyObject keyobj(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_value_reply(ctx.reply, value.value);
        }
        return 0;
    }

    int Ardb::StringSet(Context& ctx, const std::string& key, const std::string& value, int32_t ex, int64_t px, int8_t nx_xx)
    {
        KeyObject keyobj(ctx.currentDB, KEY_STRING, key);
        ValueObject valueobj;
        valueobj.value.SetString(value, true);
        uint64_t ttl = 0;
        if (ex > 0)
        {
            ttl = ex;
            ttl *= 1000;
        }
        if (px > 0)
        {
            ttl = px;
        }
        if (nx_xx != -1)  // only set key when key not exist
        {
            m_engine.Merge(ctx, keyobj, nx_xx == 0 ? REDIS_CMD_SETNX : REDIS_CMD_SETXX, valueobj);
        }
        else
        {
            m_engine.Put(ctx, keyobj, valueobj);
        }
        if (ttl > 0)
        {
            GenericExpire(ctx, keyobj, ttl);
        }
        FireKeyChangedEvent(ctx, keyobj);
        return 0;
    }

    int Ardb::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int32_t ex = -1;
        int64_t px = -1;
        int8_t nx_xx = -1;
        if (cmd.GetArguments().size() > 2)
        {
            uint32 i = 0;
            bool syntaxerror = false;
            for (i = 2; i < cmd.GetArguments().size(); i++)
            {
                const std::string& arg = cmd.GetArguments()[i];
                if (!strcasecmp(arg.c_str(), "px") || !strcasecmp(arg.c_str(), "ex"))
                {
                    int64_t iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    if (!strcasecmp(arg.c_str(), "px"))
                    {
                        px = iv;
                    }
                    else
                    {
                        ex = iv;
                    }
                    i++;
                }
                else if (!strcasecmp(arg.c_str(), "xx"))
                {
                    nx_xx = 1;
                }
                else if (!strcasecmp(arg.c_str(), "nx"))
                {
                    nx_xx = 0;
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
        int err = StringSet(ctx, key, value, ex, px, nx_xx);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_ok_reply(ctx.reply);
        }
        return 0;
    }

    int Ardb::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 secs;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], secs))
        {
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        int err = StringSet(ctx, key, cmd.GetArguments()[2], secs, -1, -1);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_ok_reply(ctx.reply);
        }
        return 0;
    }
    int Ardb::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        KeyObject keyobj(ctx.currentDB, KEY_STRING, key);
        ValueObject valueobj;
        valueobj.value.SetString(cmd.GetArguments()[1], true);
        int err = m_engine.Merge(ctx, keyobj, REDIS_CMD_SETNX, valueobj);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, 1);
        }
        return 0;
    }
    int Ardb::SetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        KeyObject keyobj(ctx.currentDB, KEY_STRING, key);
        ValueObject valueobj;
        valueobj.value.SetString(cmd.GetArguments()[1], true);
        int err = m_engine.Merge(ctx, keyobj, REDIS_CMD_SETEANGE, valueobj);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, 1);
        }
        return 0;
    }
    int Ardb::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject keyobj(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, value.value.StringLength());
        }
        return 0;
    }
    int Ardb::StrDel(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject keyobj(ctx.currentDB, KEY_STRING, cmd.GetArguments()[0]);
        int err = m_engine.Del(ctx, keyobj);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_ok_reply(ctx.reply);
        }
        return 0;
    }

OP_NAMESPACE_END


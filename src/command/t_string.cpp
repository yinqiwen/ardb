/*
 *Copyright (c) 2013-2015, yinqiwen <yinqiwen@gmail.com>
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

    int Ardb::Get(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject k(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject v;
        int err = m_engine.Get(ctx, k, v);
        if (err == ERR_ENTRY_NOT_EXIST)
        {
            reply.Clear();
        }
        else if (err < 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetString(v.GetStringValue());
        }
        return 0;
    }
    int Ardb::MGet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.type = REDIS_REPLY_ARRAY;
        KeyObjectArray ks;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject k(ctx.ns, KEY_STRING, cmd.GetArguments()[i]);
            ks.push_back(k);
        }
        ValueObjectArray vs;
        ErrCodeArray errs;
        int err = m_engine.MultiGet(ctx, ks, vs, errs);
        if(0 != err)
        {
            reply.SetErrCode(err);
            return 0;
        }
        for (size_t i = 0; i < ks.size(); i++)
        {
            RedisReply& r = reply.AddMember();
            if (errs[i] != 0)
            {
                r.Clear();
            }
            else
            {
                r.SetString(vs[i].GetStringValue());
            }
        }
        return 0;
    }

    int Ardb::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& append = cmd.GetArguments()[1];
        KeyObject key(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        MergeOperation merge;
        merge.op = REDIS_CMD_APPEND;
        Data& merge_data = merge.Add();
        merge_data.SetString(cmd.GetArguments()[1], true);
        RedisReply& reply = ctx.GetReply();
        int err = m_engine.Merge(ctx, key, merge);
        if (err < 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            /*
             * just return appended value size
             */
            reply.SetInteger(cmd.GetArguments()[1].size());
        }
        return 0;
    }

    int Ardb::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        uint32 mills;
        if (!string_touint32(cmd.GetArguments()[1], mills))
        {
            reply.SetErrCode(ERR_OUTOFRANGE);
            return 0;
        }
        KeyObject key(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject val(KEY_STRING);
        val.SetStringValue(cmd.GetArguments()[1]);
        {
            TransactionGuard guard(ctx, m_engine);
            GenericExpire(ctx, key, mills);
            m_engine.Put(ctx, key, val);
        }
        if (ctx.transc_err != 0)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    int Ardb::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetArguments().size() % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_ARGS);
            return 0;
        }
        {
            TransactionGuard guard(ctx, m_engine);
            for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
            {
                KeyObject key(ctx.ns, KEY_STRING, cmd.GetArguments()[i]);
                if (cmd.GetType() == REDIS_CMD_MSETNX)
                {
                    if (m_engine.Exists(ctx, key))
                    {
                        guard.MarkFailed(ERR_KEY_EXIST);
                        break;
                    }
                }
                ValueObject value(KEY_STRING);
                value.SetStringValue(cmd.GetArguments()[i + 1]);
                m_engine.Put(ctx, key, value);
            }
        }
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            reply.SetInteger(ctx.transc_err == 0 ? 1 : 0);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    int Ardb::MSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MSet(ctx, cmd);
    }

    int Ardb::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        double increment;
        if (!string_todouble(cmd.GetArguments()[1], increment))
        {
            reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
            return 0;
        }
        KeyObject key(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        MergeOperation merge;
        merge.op = REDIS_CMD_INCRBYFLOAT;
        merge.Add().SetString(cmd.GetArguments()[1], true);
        int err = m_engine.Merge(ctx, key, merge);
        if (err == 0)
        {
            reply.SetDouble(increment);
        }
        else
        {
            reply.SetErrCode(err);
        }
        return 0;
    }

    int Ardb::IncrDecrCommand(Context& ctx, const std::string& key, int64 incr)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject keyobj(ctx.ns, KEY_STRING, key);
        MergeOperation merge;
        merge.op = REDIS_CMD_INCRBYFLOAT;
        Data& merge_data = merge.Add();
        merge_data.SetInt64(incr);
        int err = m_engine.Merge(ctx, keyobj, merge);
        if (0 == err)
        {
            reply.SetInteger(merge_data.GetInt64());
        }
        else
        {
            reply.SetErrCode(err);
        }
        return 0;
    }

    int Ardb::Incrby(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 increment;
        if (!string_toint64(cmd.GetArguments()[1], increment))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], cmd.GetType() == REDIS_CMD_INCRBY ? increment : -increment);
    }

    int Ardb::Incr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], 1);
    }

    int Ardb::Decrby(Context& ctx, RedisCommandFrame& cmd)
    {
        return Incrby(ctx, cmd);
    }

    int Ardb::Decr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -1);
    }

    int Ardb::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject keyobj(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (err < 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(err);
        }
        else if (0 == err)
        {
            reply.SetString(value.GetStringValue());
            value.SetStringValue(cmd.GetArguments()[1]);
            err = m_engine.Put(ctx, keyobj, value);
            if (0 != err)
            {
                reply.SetErrCode(err);
            }
        }
        return 0;
    }

    int Ardb::GetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 start, end;
        if (!string_toint64(cmd.GetArguments()[1], start) || !string_toint64(cmd.GetArguments()[1], end))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        KeyObject keyobj(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            std::string str;
            value.GetStringValue().ToString(str);
            size_t strlen = str.size();
            /* Convert negative indexes */
            if (start < 0)
                start = strlen + start;
            if (end < 0)
                end = strlen + end;
            if (start < 0)
                start = 0;
            if (end < 0)
                end = 0;
            if ((unsigned long long) end >= strlen)
                end = strlen - 1;

            /* Precondition: end >= 0 && end < strlen, so the only condition where
             * nothing can be returned is: start > end. */
            if (start > end || strlen == 0)
            {
                str.clear();
            }
            else
            {
                str = str.substr(start, end - start + 1);
            }
            reply.SetString(str);
        }
        return 0;
    }

    int Ardb::StringSet(Context& ctx, const std::string& key, const std::string& value, int32_t ex, int64_t px, int8_t nx_xx)
    {
        int err = 0;
        KeyObject keyobj(ctx.ns, KEY_STRING, key);
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
            MergeOperation merge;
            merge.op = nx_xx == 0 ? REDIS_CMD_SETNX : REDIS_CMD_SETXX;
            merge.Add().SetString(value, true);
            err = m_engine.Merge(ctx, keyobj, merge);
        }
        else
        {
            ValueObject valueobj(KEY_STRING);
            valueobj.SetStringValue(value);
            err = m_engine.Put(ctx, keyobj, valueobj);
        }
        if (0 != err)
        {
            return err;
        }
        if (ttl > 0)
        {
            err = GenericExpire(ctx, keyobj, ttl);
        }
        return err;
    }

    int Ardb::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int32_t ex = -1;
        int64_t px = -1;
        int8_t nx_xx = -1;
        if (cmd.GetArguments().size() > 2)
        {
            uint32 i = 0;
            for (i = 2; i < cmd.GetArguments().size(); i++)
            {
                const std::string& arg = cmd.GetArguments()[i];
                if (!strcasecmp(arg.c_str(), "px") || !strcasecmp(arg.c_str(), "ex"))
                {
                    int64_t iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
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
                    reply.SetErrCode(ERR_INVALID_SYNTAX);
                    return 0;
                }
            }
        }
        int err = StringSet(ctx, key, value, ex, px, nx_xx);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    int Ardb::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 secs;
        if (!string_toint64(cmd.GetArguments()[1], secs))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        int err = StringSet(ctx, key, cmd.GetArguments()[2], secs, -1, -1);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }
    int Ardb::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();

        const std::string& key = cmd.GetArguments()[0];
        KeyObject keyobj(ctx.ns, KEY_STRING, key);
        ValueObject valueobj;
        valueobj.value.SetString(cmd.GetArguments()[1], true);
        int err = StringSet(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1], -1, -1, 0);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(1);
        }
        return 0;
    }
    int Ardb::SetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 offset;
        if (!string_toint64(cmd.GetArguments()[1], offset))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        KeyObject keyobj(ctx.ns, KEY_STRING, key);
        MergeOperation merge;
        merge.op = REDIS_CMD_SETEANGE;
        merge.Add().SetInt64(offset);
        merge.Add().SetString(cmd.GetArguments()[2], false);
        int err = m_engine.Merge(ctx, keyobj, merge);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(1);
        }
        return 0;
    }
    int Ardb::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject keyobj(ctx.ns, KEY_STRING, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine.Get(ctx, keyobj, value);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(value.GetStringValue().StringLength());
        }
        return 0;
    }

OP_NAMESPACE_END


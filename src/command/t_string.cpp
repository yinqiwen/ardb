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
        KeyObject keyobj(ctx.ns, KEY_META, Data::WrapCStr(cmd.GetArguments()[0]));
        ValueObject v;
        if (!CheckMeta(ctx, keyobj, KEY_STRING, v))
        {
            return 0;
        }
        if (v.GetType() == 0)
        {
            //return nil if not exist
            reply.Clear();
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
        reply.ReserveMember(0);
        KeyObjectArray ks;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            ks.push_back(k);
        }
        ValueObjectArray vs;
        ErrCodeArray errs;
        int err = m_engine->MultiGet(ctx, ks, vs, errs);
        if (0 != err)
        {
            reply.SetErrCode(err);
            return 0;
        }
        for (size_t i = 0; i < ks.size(); i++)
        {
            RedisReply& r = reply.AddMember();
            if (errs[i] != 0 || vs[i].GetType() != KEY_STRING)
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

    int Ardb::MergeAppend(Context& ctx, const KeyObject& key, ValueObject& val, const std::string& append)
    {
        if (val.GetType() != 0 && val.GetType() != KEY_STRING)
        {
            return ERR_WRONG_TYPE;
        }
        std::string str;
        val.GetStringValue().ToString(str);
        str.append(append);
        val.SetType(KEY_STRING);
        val.GetStringValue().SetString(str, false);
        return 0;
    }

    int Ardb::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        const std::string& append = cmd.GetArguments()[1];
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject v;
        int err = 0;
        RedisReply& reply = ctx.GetReply();
        /*
         * merge append
         */
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            Data merge_data;
            merge_data.SetString(append, false);
            err = m_engine->Merge(ctx, key, REDIS_CMD_APPEND, merge_data);
            if (err < 0)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STRING, v))
        {
            return 0;
        }
        MergeAppend(ctx, key, v, append);
        err = SetKeyValue(ctx, key, v);
        if (err < 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(v.GetStringValue().StringLength());
        }
        return 0;
    }

    int Ardb::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        int64 mills;
        if (!string_toint64(cmd.GetArguments()[1], mills))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        if (cmd.GetType() == REDIS_CMD_SETEX)
        {
            mills *= 1000;
        }
        mills += get_current_epoch_millis();
        const std::string& key = cmd.GetArguments()[0];
        int err = SetString(ctx, key, cmd.GetArguments()[2], false, mills, -1);
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

    int Ardb::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetArguments().size() % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_ARGS);
            return 0;
        }
        {
            WriteBatchGuard guard(ctx, m_engine);
            for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
            {
                KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
                if (!ctx.flags.redis_compatible)
                {
                    if (cmd.GetType() == REDIS_CMD_MSETNX || cmd.GetType() == REDIS_CMD_MSETNX2)
                    {
                        uint16_t merge_op = REDIS_CMD_SETNX;
                        merge_op = REDIS_CMD_SETNX;
                        Data v;
                        v.SetString(cmd.GetArguments()[i + 1], true, false);
                        m_engine->Merge(ctx, key, merge_op, v);
                    }
                    else
                    {
                        ValueObject valueobj;
                        valueobj.SetType(KEY_STRING);
                        valueobj.SetTTL(0);
                        valueobj.GetStringValue().SetString(cmd.GetArguments()[i + 1], true, false);
                        SetKeyValue(ctx, key, valueobj);
                    }
                }
                else
                {
                    ValueObject valueobj;
                    if (!CheckMeta(ctx, cmd.GetArguments()[i], KEY_STRING, valueobj))
                    {
                        guard.MarkFailed(reply.ErrCode());
                        break;
                    }
                    if (cmd.GetType() == REDIS_CMD_MSETNX)
                    {
                        if (valueobj.GetType() != 0)
                        {
                            guard.MarkFailed(ERR_KEY_EXIST);
                            break;
                        }
                    }
                    ValueObject value;
                    value.SetType(KEY_STRING);
                    value.GetStringValue().SetString(cmd.GetArguments()[i + 1], true, false);
                    SetKeyValue(ctx, key, value);
                }
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

    int Ardb::MergeIncrByFloat(Context& ctx, const KeyObject& key, ValueObject& val, double inc)
    {
        if (val.GetType() > 0)
        {
            if (val.GetType() != KEY_STRING ||
				(!val.GetStringValue().IsInteger() && !val.GetStringValue().IsFloat()))
            {
                return ERR_WRONG_TYPE;
            }
        }
        val.SetType(KEY_STRING);
        Data& data = val.GetStringValue();
        data.SetFloat64(data.GetFloat64() + inc);
        return 0;
    }

    int Ardb::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        double increment;
        if (!string_todouble(cmd.GetArguments()[1], increment))
        {
            reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
            return 0;
        }
        int err = 0;
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        /*
         * merge incr
         */
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            Data merge_data;
            merge_data.SetFloat64(increment);
            err = m_engine->Merge(ctx, key, cmd.GetType(), merge_data);
            if (err < 0)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        ValueObject v;
        err = m_engine->Get(ctx, key, v);
        if (err == ERR_ENTRY_NOT_EXIST || 0 == err)
        {
            err = MergeIncrByFloat(ctx, key, v, increment);
            if (0 == err)
            {
                err = SetKeyValue(ctx, key, v);
            }
        }
        if (err != 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetDouble(v.GetStringValue().GetFloat64());
        }
        return 0;
    }

    int Ardb::MergeIncrBy(Context& ctx, const KeyObject& key, ValueObject& val, int64_t incr)
    {
        if (val.GetType() > 0)
        {
            if (val.GetType() != KEY_STRING || !val.GetStringValue().IsInteger())
            {
                return ERR_WRONG_TYPE;
            }
        }
        val.SetType(KEY_STRING);
        Data& data = val.GetStringValue();
        data.SetInt64(data.GetInt64() + incr);
        return 0;
    }

    int Ardb::IncrDecrCommand(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        int64 incr = 1;
        if (cmd.GetArguments().size() > 1)
        {
            if (!GetLongFromProtocol(ctx, cmd.GetArguments()[1], incr))
            {
                return 0;
            }
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        int err = 0;
        /*
         * merge incr
         */
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            Data merge_data;
            merge_data.SetInt64(incr);
            err = m_engine->Merge(ctx, key, cmd.GetType(), merge_data);
            if (err < 0)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        ValueObject v;
        err = m_engine->Get(ctx, key, v);
        if (err == ERR_ENTRY_NOT_EXIST || 0 == err)
        {
            switch (cmd.GetType())
            {
                case REDIS_CMD_DECR:
                {
                    incr = -1;
                    break;
                }
                case REDIS_CMD_DECRBY:
                {
                    incr = 0 - incr;
                    break;
                }
                default:
                {
                    break;
                }
            }
            err = MergeIncrBy(ctx, key, v, incr);
            if (0 == err)
            {
                err = SetKeyValue(ctx, key, v);
            }
        }
        if (err < 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(v.GetStringValue().GetInt64());
        }
        return 0;
    }

    int Ardb::Incrby(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd);
    }

    int Ardb::Incr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd);
    }

    int Ardb::Decrby(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd);
    }

    int Ardb::Decr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd);
    }

    int Ardb::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.Clear(); //deault nil response
        KeyObject keyobj(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine->Get(ctx, keyobj, value);
        if (err < 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(err);
        }
        else if (0 == err)
        {
            if (value.GetType() != KEY_STRING)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
            reply.SetString(value.GetStringValue());
            value.GetStringValue().SetString(cmd.GetArguments()[1], true);
            err = SetKeyValue(ctx, keyobj, value);
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
        if (!GetLongFromProtocol(ctx, cmd.GetArguments()[1], start) || !GetLongFromProtocol(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        KeyObject keyobj(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject value;
        int err = m_engine->Get(ctx, keyobj, value);
        if (0 != err)
        {
            if (err == ERR_ENTRY_NOT_EXIST)
            {
                reply.SetString("");
            }
            else
            {
                reply.SetErrCode(err);
            }
        }
        else
        {
            if (value.GetType() != KEY_STRING)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
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

    int Ardb::MergeSet(Context& ctx, const KeyObject& key, ValueObject& val, uint16_t op, const Data& data, int64 ttl)
    {
        uint8 val_type = val.GetType();
        if (val_type > 0 && val_type != KEY_STRING)
        {
            return ERR_WRONG_TYPE;
        }

        if (op == REDIS_CMD_SETNX || op == REDIS_CMD_SETNX2)
        {
            if (val_type != 0)
            {
                return ERR_NOTPERFORMED;
            }
        }
        else if (op == REDIS_CMD_SETXX)
        {
            if (0 == val_type)
            {
                return ERR_NOTPERFORMED;
            }
        }
        val.SetType(KEY_STRING);
        val.GetStringValue().Clone(data);
        if (ttl > 0)
        {
            MergeExpire(ctx, key, val, ttl);
        }
        return 0;
    }

    int Ardb::SetString(Context& ctx, const std::string& key, const std::string& value, bool redis_compatible, int64_t px, int8_t nx_xx)
    {
        ctx.flags.create_if_notexist = 1;
        int err = 0;
        KeyObject keyobj(ctx.ns, KEY_META, Data::WrapCStr(key));
        uint64_t ttl = 0;
        if (px > 0)
        {
            ttl = px;
        }

        uint16 op = REDIS_CMD_SET;
        if (nx_xx != -1)
        {
            op = nx_xx == 0 ? REDIS_CMD_SETNX : REDIS_CMD_SETXX;
        }
        ValueObject valueobj;
        /*
         * setex should work on compatible mode if storage engine do not support merge
         */
        if (!m_engine->GetFeatureSet().support_merge && ttl > 0)
        {
            redis_compatible = true;
        }
        if (redis_compatible)
        {
            if (!CheckMeta(ctx, key, KEY_STRING, valueobj))
            {
                return ctx.GetReply().ErrCode();
            }
            Data merge;
            merge.SetString(value, true);
            int64 oldttl = valueobj.GetTTL();
            err = MergeSet(ctx, keyobj, valueobj, op, merge, ttl);
            if (0 == err)
            {
                err = SetKeyValue(ctx, keyobj, valueobj);
                if (0 == err)
                {
                    SaveTTL(ctx, keyobj.GetNameSpace(), key, oldttl, ttl);
                }
            }
        }
        else
        {
            if (REDIS_CMD_SET == op)
            {
                valueobj.SetType(KEY_STRING);
                valueobj.SetTTL(ttl);
                valueobj.GetStringValue().SetString(value, true, false);
                err = SetKeyValue(ctx, keyobj, valueobj);
            }
            else
            {
                DataArray merge_data;
                Data merge;
                merge.SetString(value, true);
                merge_data.push_back(merge);
                if (ttl > 0)
                {
                    if (GetConf().master_host.empty() || !GetConf().slave_ignore_expire)
                    {
                        op = REDIS_CMD_PSETEX;
                        Data ttl_data;
                        ttl_data.SetInt64(ttl);
                        merge_data.push_back(ttl_data);
                    }
                }
                err = MergeKeyValue(ctx, keyobj, op, merge_data);
            }
        }
        return err;
    }

    int Ardb::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int64_t ttl = -1;
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
                        ttl = iv;
                    }
                    else
                    {
                        ttl = iv;
                        ttl *= 1000;
                    }
                    ttl += get_current_epoch_millis();
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
        bool redis_compatible = ctx.flags.redis_compatible;
        int err = SetString(ctx, key, value, redis_compatible, ttl, nx_xx);
        if (0 != err)
        {
            if (ERR_NOTPERFORMED == err)
            {
                reply.Clear(); //return nil response
            }
            else
            {
                reply.SetErrCode(err);
            }
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    int Ardb::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        return PSetEX(ctx, cmd);
    }
    int Ardb::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& key = cmd.GetArguments()[0];
        bool redis_compatible = ctx.flags.redis_compatible || !m_engine->GetFeatureSet().support_merge;
        int err = SetString(ctx, key, cmd.GetArguments()[1], redis_compatible, -1, 0);
        if (0 != err)
        {
            if (err == ERR_NOTPERFORMED)
            {
                reply.SetInteger(0);
            }
            else
            {
                reply.SetErrCode(err);
            }
        }
        else
        {
            if (!redis_compatible)
            {
                reply.SetStatusCode(STATUS_OK);
            }
            else
            {
                reply.SetInteger(1);
            }
        }
        return 0;
    }

    int Ardb::MergeSetRange(Context& ctx, const KeyObject& key, ValueObject& val, int64_t offset, const std::string& range)
    {
        uint8 val_type = val.GetType();
        if (val_type > 0)
        {
            if (val_type != KEY_STRING)
            {
                return ERR_WRONG_TYPE;
            }
        }
        val.SetType(KEY_STRING);
        if (offset + range.size() > 512 * 1024 * 1024)
        {
            return ERR_STRING_EXCEED_LIMIT;
        }
        std::string str;
        val.GetStringValue().ToString(str);
        if (str.size() < offset + range.size())
        {
            str.resize(offset + range.size());
        }
        str.replace(offset, range.size(), range);
        val.GetStringValue().SetString(str, false);
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
        if (offset < 0)
        {
            reply.SetErrCode(ERR_OUTOFRANGE);
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        KeyObject keyobj(ctx.ns, KEY_META, key);
        int err = 0;

        /*
         * merge setrange
         */
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            DataArray args(2);
            args[0].SetInt64(offset);
            args[1].SetString(cmd.GetArguments()[2], false);
            err = MergeKeyValue(ctx, keyobj, cmd.GetType(), args);
            if (0 != err)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
        }
        else
        {
            ValueObject valueobj;
            err = m_engine->Get(ctx, keyobj, valueobj);
//            if (err == ERR_ENTRY_NOT_EXIST)
//            {
//                reply.SetInteger(0);
//                return 0;
//            }
            if (0 != err && err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
                return 0;
            }
            err = MergeSetRange(ctx, keyobj, valueobj, offset, cmd.GetArguments()[2]);
            if (0 == err)
            {
                err = SetKeyValue(ctx, keyobj, valueobj);
            }
            if (0 != err)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetInteger(valueobj.GetStringValue().StringLength());
            }
        }
        return 0;
    }
    int Ardb::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject keyobj(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject value;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STRING, value))
        {
            return 0;
        }
        reply.SetInteger(value.GetStringValue().StringLength());
        return 0;
    }

OP_NAMESPACE_END


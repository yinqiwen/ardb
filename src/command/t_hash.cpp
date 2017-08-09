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
    int Ardb::MergeHSet(Context& ctx, const KeyObject& key, ValueObject& value, uint16_t op, const Data& opv)
    {
        bool nx = (op == REDIS_CMD_HSETNX || op == REDIS_CMD_HSETNX2);
        bool set_meta = key.GetType() == KEY_META;
        if (value.GetType() > 0)
        {
            if (nx && !set_meta)
            {
                return ERR_NOTPERFORMED;
            }
            if (set_meta)
            {
                if (value.GetType() != KEY_HASH)
                {
                    return ERR_NOTPERFORMED;
                }
                if (value.GetObjectLen() == -1)
                {
                    //value is not changed
                    return ERR_NOTPERFORMED;
                }
                value.SetObjectLen(-1);
            }
            else
            {
                if (value.GetType() != KEY_HASH_FIELD)
                {
                    return ERR_NOTPERFORMED;
                }
                if (value.GetHashValue() == opv)
                {
                    return ERR_NOTPERFORMED;
                }
                value.SetHashValue(opv);
            }
            return 0;
        }
        if (set_meta)
        {
            value.SetType(KEY_HASH);
            value.SetObjectLen(opv.IsInteger() ? opv.GetInt64() : -1);
        }
        else
        {
            value.SetType(KEY_HASH_FIELD);
            value.SetHashValue(opv);
        }
        return 0;
    }

    /*
     *  hmset2 would overwrite META key and write fields, which may overwrite exist key with other type(string/list/set/zset)
     */
    int Ardb::HMSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        ctx.flags.create_if_notexist = 1;
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        {
            WriteBatchGuard batch(ctx, m_engine);
            if (ctx.flags.redis_compatible)
            {
                if (!CheckMeta(ctx, key, KEY_HASH, meta))
                {
                    return 0;
                }
            }
            meta.SetType(KEY_HASH);
            meta.SetObjectLen(-1);
            meta.SetTTL(0); //clear ttl setting
            SetKeyValue(ctx, key, meta);

            for (size_t i = 1; i < cmd.GetArguments().size(); i += 2)
            {
                KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
                field.SetHashField(cmd.GetArguments()[i]);
                ValueObject field_value;
                field_value.SetType(KEY_HASH_FIELD);
                field_value.SetHashValue(cmd.GetArguments()[i + 1]);
                SetKeyValue(ctx, field, field_value);
            }
        }
        if (0 != ctx.transc_err)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

    /*
     *  hset2 would overwrite META key and write fields, which may overwrite exist key with other type(string/list/set/zset)
     */
    int Ardb::HSet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        int err = 0;
        if (!ctx.flags.redis_compatible)
        {
            {
                WriteBatchGuard batch(ctx, m_engine);
                for (size_t i = 1; i < cmd.GetArguments().size(); i += 2)
                {
                    KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
                    field.SetHashField(cmd.GetArguments()[i]);
                    ValueObject field_value;
                    field_value.SetType(KEY_HASH_FIELD);
                    field_value.SetHashValue(cmd.GetArguments()[i + 1]);
                    if (cmd.GetType() == REDIS_CMD_HSETNX || cmd.GetType() == REDIS_CMD_HSETNX2)
                    {
                        Data meta_size;
                        meta_size.SetInt64(1);
                        m_engine->Merge(ctx, key, REDIS_CMD_HSETNX, meta_size);
                        m_engine->Merge(ctx, field, REDIS_CMD_HSETNX, field_value.GetHashValue());
                    }
                    else
                    {
                        SetKeyValue(ctx, field, field_value);
                    }
                }
                meta.SetType(KEY_HASH);
                meta.SetObjectLen(-1);
                meta.SetTTL(0); //clear ttl setting
                SetKeyValue(ctx, key, meta);
            }
            if (0 != ctx.transc_err)
            {
                reply.SetErrCode(ctx.transc_err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        KeyLockGuard guard(ctx, key);
        KeyObjectArray keys;
        keys.push_back(key);
        for (size_t i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
            field.SetHashField(cmd.GetArguments()[i]);
            keys.push_back(field);
        }
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (errs[0] != 0 && errs[0] != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(errs[0]);
            return 0;
        }

        Data meta_size;
        meta_size.SetInt64(1);
        err = MergeHSet(ctx, keys[0], vals[0], cmd.GetType(), meta_size);
        bool inserted = vals[1].GetType() == 0;
        if (0 == err || ERR_NOTPERFORMED == err)
        {
            for (size_t i = 1; i < keys.size(); i++)
            {
                Data field_value;
                field_value.SetString(cmd.GetArguments()[i * 2], true);
                err = MergeHSet(ctx, keys[i], vals[i], cmd.GetType(), field_value);
            }

        }
        if (0 == err)
        {
            vals[0].SetTTL(0);
            {
                WriteBatchGuard batch(ctx, m_engine);
                for (size_t i = 0; i < keys.size(); i++)
                {
                    SetKeyValue(ctx, keys[i], vals[i]);
                }
            }
            err = ctx.transc_err;
        }
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
            reply.SetInteger(inserted ? 1 : 0);
        }
        return 0;
    }
    int Ardb::HSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return HSet(ctx, cmd);
    }

    int Ardb::HIterate(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();

        reply.ReserveMember(0);
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        Iterator* iter = m_engine->Find(ctx, key);

        bool checked_meta = false;
        while (iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (!checked_meta)
            {
                if (field.GetType() == KEY_META && field.GetKey() == key.GetKey())
                {

                    ValueObject& meta = iter->Value();

                    if (meta.GetType() != KEY_HASH)
                    {
                        reply.SetErrCode(ERR_WRONG_TYPE);
                        break;
                    }
                    checked_meta = true;
                    iter->Next();
                    continue;
                }
                else
                {
                    break;
                }
            }
            if (field.GetType() != KEY_HASH_FIELD || field.GetNameSpace() != key.GetNameSpace()
                    || field.GetKey() != key.GetKey())
            {
                break;
            }

            if (cmd.GetType() == REDIS_CMD_HKEYS || cmd.GetType() == REDIS_CMD_HGETALL)
            {
                RedisReply& r = reply.AddMember();
                r.SetString(field.GetHashField());
            }
            if (cmd.GetType() == REDIS_CMD_HVALS || cmd.GetType() == REDIS_CMD_HGETALL)
            {
                ValueObject& fv = iter->Value();
                RedisReply& r = reply.AddMember();
                r.SetString(fv.GetHashValue());
            }
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }
    int Ardb::HKeys(Context& ctx, RedisCommandFrame& cmd)
    {
        return HIterate(ctx, cmd);
    }
    int Ardb::HVals(Context& ctx, RedisCommandFrame& cmd)
    {
        return HIterate(ctx, cmd);
    }

    int Ardb::HGetAll(Context& ctx, RedisCommandFrame& cmd)
    {
        return HIterate(ctx, cmd);
    }
    int Ardb::HScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }

    int Ardb::HMGet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.ReserveMember(cmd.GetArguments().size() - 1);
        KeyObjectArray keys;
        KeyObject meta_key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        keys.push_back(meta_key);
        for (size_t i = 1; i < cmd.GetArguments().size(); i++)
        {
            KeyObject key(ctx.ns, KEY_HASH_FIELD, cmd.GetArguments()[0]);
            key.SetHashField(cmd.GetArguments()[i]);
            keys.push_back(key);
        }
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (errs[0] != 0)
        {
            if (errs[0] != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(errs[0]);
            }
            return 0;
        }
        else
        {
            if (vals[0].GetType() != KEY_HASH)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
        }

        for (size_t i = 1; i < errs.size(); i++)
        {
            if (errs[i] != 0)
            {
                reply.MemberAt(i - 1).Clear();
            }
            else
            {
                reply.MemberAt(i - 1).SetString(vals[i].GetHashValue());
            }
        }
        return 0;
    }

    int Ardb::HLen(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_HASH, cmd.GetArguments()[0]);
    }

    int Ardb::MergeHIncrby(Context& ctx, const KeyObject& key, ValueObject& value, uint16_t op, const Data& v)
    {
        if (op == REDIS_CMD_HINCR)
        {
            if (!v.IsInteger())
            {
                return ERR_INVALID_INTEGER_ARGS;
            }
        }
        else if (op == REDIS_CMD_HINCRBYFLOAT)
        {
            if (!v.IsNumber())
            {
                return ERR_INVALID_INTEGER_ARGS;
            }
        }

        if (value.GetType() == 0)
        {
            value.SetType(KEY_HASH_FIELD);
            value.SetHashValue(v);
        }
        else
        {
            if (op == REDIS_CMD_HINCR)
            {
                if (!value.GetHashValue().IsInteger())
                {
                    return ERR_INVALID_INTEGER_ARGS;
                }
                value.GetHashValue().SetInt64(v.GetInt64() + value.GetHashValue().GetInt64());
            }
            else  //hincrbyfloat
            {
                if (!value.GetHashValue().IsNumber())
                {
                    return ERR_INVALID_FLOAT_ARGS;
                }
                value.GetHashValue().SetFloat64(v.GetFloat64() + value.GetHashValue().GetFloat64());
            }
        }
        return 0;
    }

    int Ardb::HIncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        return HIncrby(ctx, cmd);
    }

    int Ardb::HIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        bool inc_float = (cmd.GetType() == REDIS_CMD_HINCRBYFLOAT || cmd.GetType() == REDIS_CMD_HINCRBYFLOAT2);
        int64 increment_integer = 0;
        double increment_float = 0;
        int64 int_val = 0;
        double float_val = 0;
        if (inc_float)
        {
            if (!string_todouble(cmd.GetArguments()[2], increment_float))
            {
                reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
                return 0;
            }
        }
        else
        {
            if (!string_toint64(cmd.GetArguments()[2], increment_integer))
            {
                reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
        }

        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyObject field_key(ctx.ns, KEY_HASH_FIELD, keystr);
        field_key.SetHashField(cmd.GetArguments()[1]);
        int err = 0;
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            Data arg;
            if (inc_float)
            {
                arg.SetFloat64(increment_float);
            }
            else
            {
                arg.SetInt64(increment_integer);
            }
            {
                WriteBatchGuard batch(ctx, m_engine);
                ValueObject meta;
                meta.SetType(KEY_HASH);
                meta.SetObjectLen(-1);
                SetKeyValue(ctx, meta_key, meta);
                m_engine->Merge(ctx, field_key, cmd.GetType(), arg);
            }
            err = ctx.transc_err;
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
        KeyLockGuard guard(ctx, meta_key);
        KeyObjectArray keys;
        keys.push_back(meta_key);
        keys.push_back(field_key);
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        bool meta_change = false;
        if (errs[0] != 0 && ERR_ENTRY_NOT_EXIST != errs[0])
        {
            reply.SetErrCode(errs[0]);
            return 0;
        }
        if (vals[0].GetType() > 0 && vals[0].GetType() != KEY_HASH)
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        if (vals[0].GetType() == 0)
        {
            vals[0].SetType(KEY_HASH);
            vals[0].SetObjectLen(0);
            meta_change = true;
        }
        if (vals[1].GetType() == 0)
        {
            vals[1].SetType(KEY_HASH_FIELD);
            if (vals[0].GetObjectLen() >= 0)
            {
                vals[0].SetObjectLen(vals[0].GetObjectLen() + 1);
                meta_change = true;
            }
            if (inc_float)
            {
                vals[1].GetHashValue().SetFloat64(increment_float);
                float_val = increment_float;
            }
            else
            {
                vals[1].GetHashValue().SetInt64(increment_integer);
                int_val = increment_integer;
            }
        }
        else
        {
            if (inc_float)
            {
                if (!vals[1].GetHashValue().IsNumber())
                {
                    err = ERR_WRONG_TYPE;
                }
                else
                {
                    float_val = vals[1].GetHashValue().GetFloat64();
                    float_val += increment_float;
                    vals[1].GetHashValue().SetFloat64(float_val);
                }
            }
            else
            {
                if (!vals[1].GetHashValue().IsInteger())
                {
                    err = ERR_WRONG_TYPE;
                }
                else
                {
                    int_val = vals[1].GetHashValue().GetInt64();
                    int_val += increment_integer;
                    vals[1].GetHashValue().SetInt64(int_val);
                }
            }
        }
        if (0 == err)
        {
            WriteBatchGuard batch(ctx, m_engine);
            if (meta_change)
            {
                SetKeyValue(ctx, keys[0], vals[0]);
            }
            SetKeyValue(ctx, keys[1], vals[1]);
        }
        if (0 == err)
        {
            err = ctx.transc_err;
        }

        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            if (inc_float)
            {
                reply.SetDouble(float_val);
            }
            else
            {
                reply.SetInteger(int_val);
            }
        }
        return 0;
    }

    int Ardb::HGet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        key.SetHashField(cmd.GetArguments()[1]);
        KeyObjectArray keys;
        keys.push_back(meta_key);
        keys.push_back(key);
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (errs[0] != 0 || errs[1] != 0)
        {
            int err = errs[0] != 0 ? errs[0] : errs[1];
            if (err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.Clear();
            }
            return 0;
        }
        if (vals[0].GetType() > 0 && vals[0].GetType() != KEY_HASH)
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
        }
        else
        {
            reply.SetString(vals[1].GetHashValue());
        }
        return 0;
    }

    int Ardb::HExists(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_HASH))
        {
            return 0;
        }
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        key.SetHashField(cmd.GetArguments()[1]);
        bool existed = m_engine->Exists(ctx, key);
        reply.SetInteger(existed ? 1 : 0);
        return 0;
    }
    int Ardb::HDel(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        int err = 0;
        if (!ctx.flags.redis_compatible)
        {
            {
                WriteBatchGuard batch(ctx, m_engine);
                meta.SetType(KEY_HASH);
                meta.SetObjectLen(-1);
                SetKeyValue(ctx, key, meta);
                for (size_t i = 1; i < cmd.GetArguments().size(); i++)
                {
                    KeyObject field(ctx.ns, KEY_HASH_FIELD, cmd.GetArguments()[0]);
                    field.SetHashField(cmd.GetArguments()[i]);
                    RemoveKey(ctx, field);
                }
            }
            if (0 != ctx.transc_err)
            {
                reply.SetErrCode(ctx.transc_err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        err = m_engine->Get(ctx, key, meta);
        if (err != 0)
        {
            if (err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetInteger(0);
            }
            return 0;
        }
        if (meta.GetType() != KEY_HASH)
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }
        int64_t del_num = 0;
        {
            WriteBatchGuard batch(ctx, m_engine);
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
                field.SetHashField(cmd.GetArguments()[i]);
                if (m_engine->Exists(ctx, field))
                {
                    RemoveKey(ctx, field);
                    del_num++;
                }
            }
            if (meta.GetObjectLen() > 0)
            {
                meta.SetObjectLen(meta.GetObjectLen() - del_num);
                if (meta.GetObjectLen() == 0)
                {
                    RemoveKey(ctx, key);
                }
                else
                {
                    SetKeyValue(ctx, key, meta);
                }
            }
        }
        if (0 != ctx.transc_err)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetInteger(del_num);
        }
        return 0;
    }

OP_NAMESPACE_END


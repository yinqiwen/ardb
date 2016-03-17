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
    int Ardb::MergeHSet(Context& ctx,KeyObject& key, ValueObject& meta_value, const Data& field, const Data& value, bool inc_size, bool nx)
    {
        if (nx && meta_value.GetType() != 0)
        {
            return ERR_NOTPERFORMED;
        }
        if (meta_value.GetType() > 0 && meta_value.GetType() != KEY_HASH)
        {
            return ERR_INVALID_TYPE;
        }
        bool meta_changed = false;
        if (meta_value.GetType() == 0)
        {
            meta_value.GetHashMeta().size = 1;
            meta_changed = true;
        }
        else
        {
            if (inc_size && meta_value.GetHashMeta().size >= 0)
            {
                meta_value.GetHashMeta().size++;
                meta_changed = true;
            }
            else
            {
                if (meta_value.GetHashMeta().size >= 0)
                {
                    meta_value.GetHashMeta().size = -1;
                    meta_changed = true;
                }
            }
        }
        meta_value.SetType(KEY_HASH);
        KeyObject field_key(key.GetNameSpace(), KEY_HASH_FIELD, key.GetKey());
        field_key.SetHashField(field);
        ValueObject value_obj;
        value_obj.SetType(KEY_HASH_FIELD);
        value_obj.SetHashValue(value);
        m_engine->Put(ctx, field_key, value_obj);
        return meta_changed ? 0 : ERR_NOTPERFORMED;
    }

    int Ardb::HMSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        int err = 0;
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            DataArray args(cmd.GetArguments().size() - 1);
            for (size_t i = 0; i < args.size(); i++)
            {
                args[i].SetString(cmd.GetArguments()[i + 1], true);
            }
            err = m_engine->Merge(ctx, key, cmd.GetType(), args);
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
        KeyLockGuard guard(key);
        ValueObject meta;
        if (!CheckMeta(ctx, keystr, KEY_HASH, meta))
        {
            return 0;
        }
        {
            meta.SetObjectLen(-1);
            TransactionGuard batch(ctx, m_engine);
            for (size_t i = 1; i < cmd.GetArguments().size(); i += 2)
            {
                KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
                field.SetHashField(cmd.GetArguments()[i]);
                ValueObject field_value;
                field_value.SetType(KEY_HASH_FIELD);
                field_value.SetHashValue(cmd.GetArguments()[i + 1]);
                m_engine->Put(ctx, field, field_value);
            }
            m_engine->Put(ctx, key, meta);
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

    int Ardb::HSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        int err = 0;
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            DataArray args(2);
            args[0].SetString(cmd.GetArguments()[1], true);
            args[1].SetString(cmd.GetArguments()[2], true);
            err = m_engine->Merge(ctx, key, cmd.GetType(), args);
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
        KeyLockGuard guard(key);
        KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
        field.SetHashField(cmd.GetArguments()[1]);
        KeyObjectArray keys;
        keys.push_back(key);
        keys.push_back(field);
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (errs[0] != 0 && errs[0] != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(errs[0]);
            return 0;
        }
        bool nx = false;
        if (cmd.GetType() == REDIS_CMD_HSETNX || cmd.GetType() == REDIS_CMD_HSETNX2)
        {
            nx = true;
            if (errs[0] != ERR_ENTRY_NOT_EXIST && errs[1] != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetInteger(0);
                return 0;
            }
        }

        bool inserted = (errs[0] == ERR_ENTRY_NOT_EXIST || errs[1] == ERR_ENTRY_NOT_EXIST);
        err = MergeHSet(ctx,key, vals[0], field.GetHashField(), vals[1].GetHashValue(), inserted, nx);
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            if (inserted)
            {
                m_engine->Put(ctx, key, vals[0]);
            }
            reply.SetInteger(inserted ? 1 : 0);
        }
        return 0;
    }
    int Ardb::HSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return HSet(ctx, cmd);
    }
    int Ardb::HKeys(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_HASH, meta))
        {
            return 0;
        }
        reply.ReserveMember(0);
        if (meta.GetType() == 0)
        {
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        Iterator* iter = m_engine->Find(ctx, key);
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_HASH_FIELD || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != field.GetKey())
            {
                break;
            }
            RedisReply& r = reply.AddMember();
            r.SetString(field.GetHashField());
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }
    int Ardb::HVals(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_HASH, meta))
        {
            return 0;
        }
        reply.ReserveMember(0);
        if (meta.GetType() == 0)
        {
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        Iterator* iter = m_engine->Find(ctx, key);
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_HASH_FIELD || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != field.GetKey())
            {
                break;
            }
            ValueObject& val = iter->Value();
            RedisReply& r = reply.AddMember();
            r.SetString(val.GetHashValue());
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::HGetAll(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_HASH, meta))
        {
            return 0;
        }
        reply.ReserveMember(0);
        if (meta.GetType() == 0)
        {
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        Iterator* iter = m_engine->Find(ctx, key);
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_HASH_FIELD || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != field.GetKey())
            {
                break;
            }
            ValueObject& val = iter->Value();
            RedisReply& r1 = reply.AddMember();
            RedisReply& r2 = reply.AddMember();
            r1.SetString(field.GetHashField());
            r2.SetString(val.GetHashValue());
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }
    int Ardb::HScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
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
                reply.SetErrCode(ERR_INVALID_TYPE);
                return 0;
            }
        }

        for (size_t i = 1; i < errs.size(); i++)
        {
            if (errs[i] != 0)
            {
                reply.MemberAt(i).Clear();
            }
            else
            {
                reply.MemberAt(i).SetString(vals[i].GetHashValue());
            }
        }
        return 0;
    }

    int Ardb::HLen(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_HASH, cmd.GetArguments()[0]);
    }

    int Ardb::MergeHCreateMeta(Context& ctx,KeyObject& key, ValueObject& value, const Data& v)
    {
        if (value.GetType() != 0)
        {
            if (value.GetType() != KEY_HASH)
            {
                KeyObject key(key.GetNameSpace(), KEY_HASH_FIELD, key.GetKey());
                key.SetHashField(v);
                Context tmpctx;
                m_engine->Del(tmpctx, key);
            }
            //value is not changed
            return ERR_NOTPERFORMED;
        }
        value.SetType(KEY_HASH);
        value.GetHashMeta().size = 1;
        return 0;
    }

    int Ardb::MergeHIncrby(Context& ctx,KeyObject& key, ValueObject& value, uint16_t op, const Data& v)
    {
        if (value.GetType() == 0)
        {
            value.SetHashValue(v);
        }
        else
        {
            if (op == REDIS_CMD_HINCR || op == REDIS_CMD_HINCR2)
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
        RedisReply& reply = ctx.GetReply();
        double increment = 0;
        double val = 0;
        if (!string_todouble(cmd.GetArguments()[2], increment))
        {
            reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        key.SetHashField(cmd.GetArguments()[1]);
        int err = 0;
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            Data arg;
            arg.SetFloat64(increment);
            {
                TransactionGuard batch(ctx, m_engine);
                m_engine->Merge(ctx, key, cmd.GetType(), arg);
                m_engine->Merge(ctx, meta_key, REDIS_CMD_HMETA_CREATE, arg);
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
        KeyLockGuard guard(meta_key);
        KeyObjectArray keys;
        keys.push_back(meta_key);
        keys.push_back(key);
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        Data inc_data;
        inc_data.SetFloat64(increment);
        if (errs[0] != 0)
        {
            if (ERR_ENTRY_NOT_EXIST != errs[0])
            {
                err = errs[0];
            }
            else
            {

                err = MergeHSet(ctx,meta_key, vals[0], key.GetHashField(), inc_data, false, false);
                val = increment;
            }
        }
        else
        {
            if (errs[1] != 0)
            {
                if (ERR_ENTRY_NOT_EXIST != errs[1])
                {
                    err = errs[1];
                }
                else
                {
                    err = MergeHSet(ctx,meta_key, vals[0], key.GetHashField(), inc_data, false, false);
                    val = increment;
                }
            }
            else
            {
                if (vals[1].GetHashValue().IsNumber())
                {
                    err = ERR_INVALID_TYPE;
                }
                else
                {
                    val = vals[1].GetHashValue().GetFloat64();
                    val += increment;
                    vals[1].GetHashValue().SetFloat64(val);
                    err = m_engine->Put(ctx, keys[1], vals[1]);
                }
            }
        }

        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetDouble(val);
        }
        return 0;
    }

    int Ardb::HIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 increment = 0;
        int64 val = 0;
        if (!string_toint64(cmd.GetArguments()[2], increment))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyObject key(ctx.ns, KEY_HASH_FIELD, keystr);
        key.SetHashField(cmd.GetArguments()[1]);
        int err = 0;
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            Data arg;
            arg.SetInt64(increment);
            {
                TransactionGuard batch(ctx, m_engine);
                m_engine->Merge(ctx, meta_key, REDIS_CMD_HMETA_CREATE, arg);
                m_engine->Merge(ctx, key, cmd.GetType(), arg);
            }

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
        KeyLockGuard guard(meta_key);
        KeyObjectArray keys;
        keys.push_back(meta_key);
        keys.push_back(key);
        ValueObjectArray vals;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (errs[0] != 0)
        {
            if (ERR_ENTRY_NOT_EXIST != errs[0])
            {
                err = errs[0];
            }
            else
            {
                err = MergeHSet(ctx,meta_key, vals[0], key.GetHashField(), increment, false, false);
                val = increment;
            }
        }
        else
        {
            if (errs[1] != 0)
            {
                if (ERR_ENTRY_NOT_EXIST != errs[1])
                {
                    err = errs[1];
                }
                else
                {
                    err = MergeHSet(ctx,meta_key, vals[0], key.GetHashField(), increment, false, false);
                    val = increment;
                }
            }
            else
            {
                if (vals[1].GetHashValue().IsInteger())
                {
                    err = ERR_INVALID_TYPE;
                }
                else
                {
                    val = vals[1].GetHashValue().GetInt64();
                    val += increment;
                    vals[1].GetHashValue().SetInt64(val);
                    err = m_engine->Put(ctx, keys[1], vals[1]);
                }
            }
        }

        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(val);
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
            reply.SetErrCode(ERR_INVALID_TYPE);
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
    int Ardb::MergeHDel(Context& ctx,KeyObject& key, ValueObject& meta_value, const DataArray& fields)
    {
        if (meta_value.GetType() == 0 || meta_value.GetType() != KEY_HASH)
        {
            return ERR_NOTPERFORMED;
        }
        TransactionGuard batch(ctx, m_engine);
        for (size_t i = 1; i < fields.size(); i++)
        {
            KeyObject field(key.GetNameSpace(), KEY_HASH_FIELD, key.GetKey());
            field.SetHashField(fields[i]);
            m_engine->Del(ctx, field);
        }
        if (meta_value.GetObjectLen() >= 0)
        {
            meta_value.SetObjectLen(-1);
            return 0;
        }
        else
        {
            /*
             * meta value is not changed
             */
            return ERR_NOTPERFORMED;
        }
    }
    int Ardb::HDel(Context& ctx, RedisCommandFrame& cmd)
    {
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
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            DataArray args(cmd.GetArguments().size() - 1);
            for (size_t i = 0; i < args.size(); i++)
            {
                args[i].SetString(cmd.GetArguments()[i + 1], true);
            }
            err = m_engine->Merge(ctx, key, cmd.GetType(), args);
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
        KeyLockGuard guard(key);
        err = m_engine->Get(ctx, key, meta);
        if (err != 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(err);
            return 0;
        }
        if (meta.GetType() != KEY_HASH)
        {
            reply.SetErrCode(ERR_INVALID_TYPE);
            return 0;
        }
        int64_t del_num = 0;
        {
            TransactionGuard batch(ctx, m_engine);
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                KeyObject field(ctx.ns, KEY_HASH_FIELD, keystr);
                field.SetHashField(cmd.GetArguments()[i]);
                if (m_engine->Exists(ctx, field))
                {
                    m_engine->Del(ctx, field);
                    del_num++;
                }
            }
            if (meta.GetObjectLen() > 0)
            {
                meta.SetObjectLen(meta.GetObjectLen() - del_num);
                m_engine->Put(ctx, key, meta);
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


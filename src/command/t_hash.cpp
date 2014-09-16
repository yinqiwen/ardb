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

    int Ardb::HashMultiSet(Context& ctx, ValueObject& meta, DataMap& fs)
    {
        if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPMAP)
        {
            bool multi_write = fs.size() > 1;
            bool set_meta = meta.meta.len != -1;

            if (meta.meta.Length() > 0)
            {
                multi_write = true;
            }
            BatchWriteGuard guard(GetKeyValueEngine(), multi_write);
            DataMap::iterator it = fs.begin();
            while (it != fs.end())
            {
                ValueObject v(HASH_FIELD);
                v.element = it->second;
                v.key.type = HASH_FIELD;
                v.key.db = meta.key.db;
                v.key.key = meta.key.key;
                v.key.element = it->first;
                SetKeyValue(ctx, v);
                it++;
            }
            if (set_meta)
            {
                meta.meta.len = -1;
                SetKeyValue(ctx, meta);
            }
            fill_int_reply(ctx.reply, fs.size());
        }
        else
        {
            int64 oldlen = meta.meta.Length();
            DataMap::iterator it = fs.begin();
            bool zipsave = true;
            while (it != fs.end())
            {
                const Data& field = it->first;
                Data& value = it->second;
                meta.meta.zipmap[field] = value;
                it++;
                if (!meta.attach.force_zipsave
                        && (field.StringLength() > m_cfg.hash_max_ziplist_value
                                || value.StringLength() > m_cfg.hash_max_ziplist_value))
                {
                    zipsave = false;
                }
            }
            if (meta.meta.zipmap.size() > m_cfg.hash_max_ziplist_entries)
            {
                zipsave = false;
            }
            BatchWriteGuard guard(GetKeyValueEngine(), !zipsave);
            if (!zipsave)
            {
                /*
                 * convert to non zipmap encoding
                 */
                DataMap::iterator fit = meta.meta.zipmap.begin();
                while (fit != meta.meta.zipmap.end())
                {
                    ValueObject v(HASH_FIELD);
                    v.element = fit->second;
                    v.key.type = HASH_FIELD;
                    v.key.db = meta.key.db;
                    v.key.key = meta.key.key;
                    v.key.element = fit->first;
                    SetKeyValue(ctx, v);
                    fit++;
                }
                meta.meta.len = meta.meta.zipmap.size();
                meta.meta.zipmap.clear();
                meta.meta.SetEncoding(COLLECTION_ECODING_RAW);
            }
            SetKeyValue(ctx, meta);
            fill_int_reply(ctx.reply, meta.meta.Length() - oldlen);
        }

        return 0;
    }

    int Ardb::HashSet(Context& ctx, ValueObject& meta, const Data& field, Data& value)
    {
        DataMap fs;
        fs.insert(DataMap::value_type(field, value));
        return HashMultiSet(ctx, meta, fs);
    }

    int Ardb::HashGet(Context& ctx, ValueObject& meta, Data& field, Data& value)
    {
        if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPMAP)
        {
            ValueObject vv;
            vv.key.type = HASH_FIELD;
            vv.key.db = meta.key.db;
            vv.key.key = meta.key.key;
            vv.key.element = field;
            int err = GetKeyValue(ctx, vv.key, &vv);
            if (0 == err)
            {
                value = vv.element;
            }
            return err;
        }
        else
        {
            DataMap::iterator fit = meta.meta.zipmap.find(field);
            if (fit == meta.meta.zipmap.end())
            {
                return ERR_NOT_EXIST;
            }
            value = fit->second;
            return 0;
        }
    }

    int Ardb::HashGet(Context& ctx, const std::string& key, const std::string& field, Data& v)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data f;
        f.SetString(field, true);
        err = HashGet(ctx, meta, f, v);
        if (err == ERR_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_STRING;
            v.GetDecodeString(ctx.reply.str);
        }
        return 0;
    }

    int Ardb::HReplace(Context& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for HReplace");
            return 0;
        }
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        ValueObject meta;
        meta.key.db = ctx.currentDB;
        meta.key.key = cmd.GetArguments()[0];
        meta.key.type = KEY_META;
        meta.type = HASH_META;
        meta.meta.SetEncoding(COLLECTION_ECODING_ZIPMAP);
        DataMap fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            Data f(cmd.GetArguments()[i]), v(cmd.GetArguments()[i + 1]);
            fs[f] = v;
        }
        meta.attach.force_zipsave = true;
        HashMultiSet(ctx, meta, fs);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int Ardb::HMSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if (m_cfg.replace_for_hmset)
        {
            HReplace(ctx, cmd);
            return 0;
        }
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for HMSet");
            return 0;
        }
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        DataMap fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            Data f(cmd.GetArguments()[i]), v(cmd.GetArguments()[i + 1]);
            fs[f] = v;
        }
        HashMultiSet(ctx, meta, fs);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    int Ardb::HSet(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data field(cmd.GetArguments()[1]), value(cmd.GetArguments()[2]);
        HashSet(ctx, meta, field, value);
        //fill_int_reply(ctx.reply, err);
        return 0;
    }
    int Ardb::HSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data value;
        Data field(cmd.GetArguments()[1]);
        err = HashGet(ctx, meta, field, value);
        if (err == ERR_NOT_EXIST)
        {
            value.SetString(cmd.GetArguments()[2], true);
            HashSet(ctx, meta, field, value);
            err = 1;
        }
        else
        {
            err = 0;
        }
        fill_int_reply(ctx.reply, err);
        return 0;
    }
    int Ardb::HKeys(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            return 0;
        }
        HashIterator iter;
        err = HashIter(ctx, meta, "", iter, true);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        while (iter.Valid())
        {
            const Data* value = iter.Field();
            RedisReply& r = ctx.reply.AddMember();
            std::string tmp;
            fill_str_reply(r, value->GetDecodeString(tmp));
            iter.Next();
        }
        return 0;
    }
    int Ardb::HVals(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            return 0;
        }
        HashIterator iter;
        err = HashIter(ctx, meta, "", iter, true);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        while (iter.Valid())
        {
            Data* value = iter.Value();
            RedisReply& r = ctx.reply.AddMember();
            fill_value_reply(r, *value);
            iter.Next();
        }
        return 0;
    }

    int Ardb::HashGetAll(Context& ctx, const Slice& key, RedisReply& reply)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        reply.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            return 0;
        }
        HashIterator iter;
        err = HashIter(ctx, meta, "", iter, true);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        reply.type = REDIS_REPLY_ARRAY;
        while (iter.Valid())
        {
            const Data* field = iter.Field();
            Data* value = iter.Value();
            RedisReply& r = reply.AddMember();
            fill_value_reply(r, *field);
            RedisReply& r1 = reply.AddMember();
            fill_value_reply(r1, *value);
            iter.Next();
        }
        return 0;
    }

    int Ardb::HGetAll(Context& ctx, RedisCommandFrame& cmd)
    {
        HashGetAll(ctx, cmd.GetArguments()[0], ctx.reply);
        return 0;
    }
    int Ardb::HScan(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string pattern;
        uint32 limit = 10000; //return max 10000 keys one time
        if (cmd.GetArguments().size() > 2)
        {
            for (uint32 i = 2; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "count"))
                {
                    if (i + 1 >= cmd.GetArguments().size() || !string_touint32(cmd.GetArguments()[i + 1], limit))
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    i++;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "match"))
                {
                    if (i + 1 >= cmd.GetArguments().size())
                    {
                        fill_error_reply(ctx.reply, "'MATCH' need one args followed");
                        return 0;
                    }
                    pattern = cmd.GetArguments()[i + 1];
                    i++;
                }
                else
                {
                    fill_error_reply(ctx.reply, "Syntax error, try scan 0 ");
                    return 0;
                }
            }
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);

        RedisReply& r1 = ctx.reply.AddMember();
        RedisReply& r2 = ctx.reply.AddMember();
        r2.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            fill_str_reply(r1, "0");
            return 0;
        }
        const std::string& cursor = cmd.GetArguments()[1];
        HashIterator iter;
        std::string scan_start_element;
        if (cursor != "0")
        {
            if (m_cfg.scan_redis_compatible)
            {
                FindElementByRedisCursor(cursor, scan_start_element);
            }
        }
        HashIter(ctx, meta, scan_start_element, iter, true);
        std::string tmpelement;
        while (iter.Valid())
        {
            const Data* field = iter.Field();
            tmpelement.clear();
            field->GetDecodeString(tmpelement);
            if (r2.MemberSize() >= (limit * 2))
            {
                break;
            }
            if ((pattern.empty() || stringmatchlen(pattern.c_str(), pattern.size(), tmpelement.c_str(), tmpelement.size(), 0) == 1))
            {
                RedisReply& rr1 = r2.AddMember();
                RedisReply& rr2 = r2.AddMember();
                fill_str_reply(rr1, tmpelement);
                fill_value_reply(rr2, *(iter.Value()));
            }
            iter.Next();
        }
        if (iter.Valid())
        {
            if(m_cfg.scan_redis_compatible)
            {
                fill_str_reply(r1, stringfromll(GetNewRedisCursor(tmpelement)));
            }else
            {
                fill_str_reply(r1, tmpelement);
            }
        }
        else
        {
            fill_str_reply(r1, "0");
        }
        return 0;
    }

    int Ardb::HMGet(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (err == 0)
        {
            for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
            {
                Data value;
                Data field(cmd.GetArguments()[i]);
                RedisReply& r = ctx.reply.AddMember();
                if (0 == HashGet(ctx, meta, field, value))
                {
                    r.type = REDIS_REPLY_STRING;
                    value.GetDecodeString(r.str);
                }
                else
                {
                    r.type = REDIS_REPLY_NIL;
                }
            }
        }
        else
        {
            ctx.reply.ReserveMember(cmd.GetArguments().size());
        }
        return 0;
    }

    int Ardb::HashLen(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (err == ERR_NOT_EXIST)
        {
            fill_int_reply(ctx.reply, 0);
        }
        else
        {
            if (meta.meta.Length() < 0)
            {
                HashIterator iter;
                meta.meta.len = 0;
                HashIter(ctx, meta, "", iter, true);
                while (iter.Valid())
                {
                    meta.meta.len++;
                    iter.Next();
                }
                SetKeyValue(ctx, meta);
            }
            fill_int_reply(ctx.reply, meta.meta.Length());
        }
        return 0;
    }

    int Ardb::HLen(Context& ctx, RedisCommandFrame& cmd)
    {
        HashLen(ctx, cmd.GetArguments()[0]);
        return 0;
    }

    int Ardb::HIncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        double increment, val = 0;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[2], increment))
        {
            return 0;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data value;
        Data field(cmd.GetArguments()[1]);
        if (0 == err)
        {
            HashGet(ctx, meta, field, value);
            if (!value.GetDouble(val))
            {
                fill_error_reply(ctx.reply, "value is not a float or out of range");
                return 0;
            }
        }
        val += increment;
        std::string tmp;
        fast_dtoa(val, 10, tmp);
        value.SetString(tmp, false);
        HashSet(ctx, meta, field, value);
        fill_str_reply(ctx.reply, tmp);
        return 0;
    }

    int Ardb::HMIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for HMIncrby");
            return 0;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        DataMap fs;
        Int64Array vs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            int64 inc = 0;
            if (!GetInt64Value(ctx, cmd.GetArguments()[i + 1], inc))
            {
                return 0;
            }
            Data field(cmd.GetArguments()[i]);
            if (err == ERR_NOT_EXIST)
            {
                fs[field].SetInt64(inc);
                vs.push_back(inc);
            }
            else
            {
                Data value;
                HashGet(ctx, meta, field, value);
                int64 val = 0;
                if (!value.GetInt64(val))
                {
                    fill_error_reply(ctx.reply, "value is not a float or out of range");
                    return 0;
                }
                fs[field].SetInt64(inc + val);
                vs.push_back(inc + val);
            }
        }
        HashMultiSet(ctx, meta, fs);
        fill_int_array_reply(ctx.reply, vs);
        return 0;
    }

    int Ardb::HIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment, val = 0;
        if (!GetInt64Value(ctx, cmd.GetArguments()[2], increment))
        {
            return 0;
        }
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        Data field(cmd.GetArguments()[1]);
        Data value;
        if (err == 0)
        {
            err = HashGet(ctx, meta, field, value);
        }
        if (err == ERR_NOT_EXIST)
        {
            value.SetInt64(increment);
        }
        else
        {
            if (!value.GetInt64(val))
            {
                fill_error_reply(ctx.reply, "value is not a integer or out of range");
                return 0;
            }
            value.SetInt64(val + increment);
        }
        HashSet(ctx, meta, field, value);
        fill_int_reply(ctx.reply, val + increment);
        return 0;
    }

    int Ardb::HGet(Context& ctx, RedisCommandFrame& cmd)
    {
        Data v;
        HashGet(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1], v);
        return 0;
    }

    int Ardb::HExists(Context& ctx, RedisCommandFrame& cmd)
    {
        HGet(ctx, cmd);
        fill_int_reply(ctx.reply, ctx.reply.type == REDIS_REPLY_NIL ? 0 : 1);
        return 0;
    }

    int Ardb::HDel(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], HASH_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        int count = 0;
        if (err == 0)
        {
            BatchWriteGuard guard(GetKeyValueEngine());
            for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
            {
                Data field(cmd.GetArguments()[i]);
                Data value;
                err = HashGet(ctx, meta, field, value);
                if (err == 0)
                {
                    if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPMAP)
                    {
                        KeyObject k;
                        k.db = ctx.currentDB;
                        k.key = cmd.GetArguments()[0];
                        k.type = HASH_FIELD;
                        k.element = field;
                        DelKeyValue(ctx, k);
                        if (meta.meta.len > 0)
                        {
                            meta.meta.len--;
                        }
                    }
                    else
                    {
                        meta.meta.zipmap.erase(field);
                    }
                    count++;
                }
            }
            if (count > 0)
            {
                if (meta.meta.Length() != 0)
                {
                    SetKeyValue(ctx, meta);
                }
                else
                {
                    DelKeyValue(ctx, meta.key);
                }
            }
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::HClear(Context& ctx, ValueObject& meta)
    {
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPMAP);
        if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPMAP)
        {
            HashIterator iter;
            HashIter(ctx, meta, "", iter, false);
            while (iter.Valid())
            {
                DelRaw(ctx, iter.CurrentRawKey());
                iter.Next();
            }
        }
        DelKeyValue(ctx, meta.key);
        return 0;
    }

    int Ardb::RenameHash(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey)
    {
        Context tmpctx;
        tmpctx.currentDB = srcdb;
        ValueObject v;
        int err = GetMetaValue(tmpctx, srckey, HASH_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_error_reply(ctx.reply, "no such key or some error");
            return 0;
        }
        if (v.meta.Encoding() == COLLECTION_ECODING_ZIPMAP)
        {
            DelKeyValue(tmpctx, v.key);
            v.key.encode_buf.Clear();
            v.key.db = dstdb;
            v.key.key = dstkey;
            v.meta.expireat = 0;
            SetKeyValue(ctx, v);
        }
        else
        {
            HashIterator iter;
            HashIter(ctx, v, "", iter, false);
            tmpctx.currentDB = dstdb;
            ValueObject dstmeta;
            dstmeta.key.type = KEY_META;
            dstmeta.key.key = dstkey;
            dstmeta.type = HASH_META;
            dstmeta.meta.SetEncoding(COLLECTION_ECODING_ZIPMAP);
            BatchWriteGuard guard(GetKeyValueEngine());
            while (iter.Valid())
            {
                HashSet(tmpctx, dstmeta, *(iter.Field()), *(iter.Value()));
                iter.Next();
            }
            SetKeyValue(tmpctx, dstmeta);
            tmpctx.currentDB = srcdb;
            DeleteKey(tmpctx, srckey);
        }
        ctx.data_change = true;
        return 0;
    }
OP_NAMESPACE_END


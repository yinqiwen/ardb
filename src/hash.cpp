/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "db.hpp"
#include "ardb_server.hpp"
#include <fnmatch.h>

namespace ardb
{
    int ArdbServer::HMSet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for HMSet");
            return 0;
        }
        SliceArray fs;
        SliceArray vals;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            fs.push_back(cmd.GetArguments()[i]);
            vals.push_back(cmd.GetArguments()[i + 1]);
        }
        int ret = m_db->HMSet(ctx.currentDB, cmd.GetArguments()[0], fs, vals);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    int ArdbServer::HSet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->HSet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, 1);
        return 0;
    }
    int ArdbServer::HSetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->HSetNX(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }
    int ArdbServer::HVals(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        StringArray keys;
        int ret = m_db->HVals(ctx.currentDB, cmd.GetArguments()[0], keys);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_str_array_reply(ctx.reply, keys);
        return 0;
    }
    int ArdbServer::HScan(ArdbConnContext& ctx, RedisCommandFrame& cmd)
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
        std::string newcursor = "0";
        ValueDataArray vs;
        int ret = m_db->HScan(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], pattern, limit, vs, newcursor);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        ctx.reply.elements.push_back(RedisReply(newcursor));
        RedisReply rs;
        fill_array_reply(rs, vs);
        ctx.reply.elements.push_back(rs);
        return 0;
    }

    int ArdbServer::HMGet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        StringArray vals;
        SliceArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->HMGet(ctx.currentDB, cmd.GetArguments()[0], fs, vals);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_str_array_reply(ctx.reply, vals);
        return 0;
    }

    int ArdbServer::HLen(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int len = m_db->HLen(ctx.currentDB, cmd.GetArguments()[0]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, len);
        fill_int_reply(ctx.reply, len);
        return 0;
    }

    int ArdbServer::HKeys(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        StringArray keys;
        int ret = m_db->HKeys(ctx.currentDB, cmd.GetArguments()[0], keys);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_str_array_reply(ctx.reply, keys);
        return 0;
    }

    int ArdbServer::HIncrbyFloat(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        double increment, val = 0;
        if (!string_todouble(cmd.GetArguments()[2], increment))
        {
            fill_error_reply(ctx.reply, "value is not a float or out of range");
            return 0;
        }
        int ret = m_db->HIncrbyFloat(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], increment, val);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_double_reply(ctx.reply, val);
        return 0;
    }

    int ArdbServer::HMIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if ((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for HMIncrby");
            return 0;
        }
        SliceArray fs;
        Int64Array incs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            fs.push_back(cmd.GetArguments()[i]);
            int64 v = 0;
            if (!string_toint64(cmd.GetArguments()[i + 1], v))
            {
                fill_error_reply(ctx.reply, "value is not a integer or out of range");
                return 0;
            }
            incs.push_back(v);
        }
        Int64Array vs;
        int ret = m_db->HMIncrby(ctx.currentDB, cmd.GetArguments()[0], fs, incs, vs);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::HIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int64 increment, val = 0;
        if (!string_toint64(cmd.GetArguments()[2], increment))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->HIncrby(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], increment, val);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, val);
        return 0;
    }

    int ArdbServer::HGetAll(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        StringArray fields;
        StringArray results;
        int ret = m_db->HGetAll(ctx.currentDB, cmd.GetArguments()[0], fields, results);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        for (uint32 i = 0; i < fields.size(); i++)
        {
            RedisReply reply1, reply2;
            fill_str_reply(reply1, fields[i]);
            std::string str;
            fill_str_reply(reply2, results[i]);
            ctx.reply.elements.push_back(reply1);
            ctx.reply.elements.push_back(reply2);
        }
        return 0;
    }

    int ArdbServer::HGet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int ret = m_db->HGet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], &v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        if (ret < 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            fill_str_reply(ctx.reply, v);
        }
        return 0;
    }

    int ArdbServer::HExists(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->HExists(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::HDel(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray fields;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fields.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->HDel(ctx.currentDB, cmd.GetArguments()[0], fields);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::HClear(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->HClear(ctx.currentDB, cmd.GetArguments()[0]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    HashMetaValue* Ardb::GetHashMeta(const DBID& db, const Slice& key, int& err, bool& create)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != HASH_META)
        {
            DELETE(meta);
            err = ERR_INVALID_TYPE;
            return NULL;
        }
        if (NULL == meta)
        {
            err = ERR_NOT_EXIST;
            meta = new HashMetaValue;
            create = true;
        }
        else
        {
            create = false;
        }
        return (HashMetaValue*) meta;
    }

    int Ardb::GetHashZipEntry(HashMetaValue* meta, const ValueData& field, ValueData*& value)
    {
        HashFieldMap::iterator it = meta->values.find(field);
        if (it != meta->values.end())
        {
            value = &(it->second);
            return 0;
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::HSetValue(HashKeyObject& key, HashMetaValue* meta, CommonValueObject& value)
    {
        KeyLockerGuard lockGuard(m_key_locker, key.db, key.key);
        BatchWriteGuard guard(GetEngine());
        if (!meta->ziped)
        {
            if (!meta->dirty)
            {
                meta->dirty = true;
                SetMeta(key.db, key.key, *meta);
            }
            return SetKeyValueObject(key, value);
        }
        else
        {
            bool zipSave = true;
            if (key.field.type == BYTES_VALUE
                    && value.data.bytes_value.size() >= (uint32) m_config.hash_max_ziplist_value)
            {
                zipSave = false;
            }
            else if (meta->values.size() >= (uint32) m_config.hash_max_ziplist_entries)
            {
                zipSave = false;
            }
            else
            {
                zipSave = true;
            }
            meta->values[key.field] = value.data;
            if (!zipSave)
            {
                meta->ziped = false;
                meta->dirty = false;
                meta->size = meta->values.size();
                HashFieldMap::iterator it = meta->values.begin();
                while (it != meta->values.end())
                {
                    HashKeyObject k(key.key, it->first, key.db);
                    CommonValueObject v(it->second);
                    SetKeyValueObject(k, v);
                    it++;
                }
                meta->values.clear();
            }
            SetMeta(key.db, key.key, *meta);
        }
        return 0;
    }
    int Ardb::HSet(const DBID& db, const Slice& key, const Slice& field, const Slice& value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            return err;
        }
        CommonValueObject valueobject;
        valueobject.data.SetValue(value, true);
        HashKeyObject hk(key, field, db);
        int ret = HSetValue(hk, meta, valueobject) == 0 ? 1 : 0;
        DELETE(meta);
        return ret;
    }

    int Ardb::HSetNX(const DBID& db, const Slice& key, const Slice& field, const Slice& value)
    {
        if (HExists(db, key, field))
        {
            return 0;
        }
        return HSet(db, key, field, value) > 0 ? 1 : 0;
    }

    int Ardb::HDel(const DBID& db, const Slice& key, const SliceArray& fields)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        SliceArray::const_iterator it = fields.begin();
        while (it != fields.end())
        {
            ValueData field_value;
            field_value.SetValue(*it, true);
            if (meta->ziped)
            {
                meta->values.erase(field_value);
            }
            else
            {
                HashKeyObject k(key, *it, db);
                DelValue(k);
            }
            it++;
        }
        if (meta->ziped)
        {
            SetMeta(db, key, *meta);
        }
        else
        {
            if (!meta->dirty)
            {
                meta->dirty = true;
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return fields.size();
    }

    int Ardb::HGetValue(HashKeyObject& key, HashMetaValue* meta, CommonValueObject& value)
    {
        int err = 0;
        bool createHash = false;
        bool createMeta = false;
        if (NULL == meta)
        {
            meta = GetHashMeta(key.db, key.key, err, createHash);
            createMeta = true;
            if (NULL == meta || createHash)
            {
                DELETE(meta);
                return err;
            }
        }

        if (meta->ziped)
        {
            ValueData* v = NULL;
            if (0 == GetHashZipEntry(meta, key.field, v))
            {
                value.data = *v;
                err = 0;
            }
            else
            {
                err = ERR_NOT_EXIST;
            }
        }
        else
        {
            err = GetKeyValueObject(key, value);
        }
        if (createMeta)
        {
            DELETE(meta);
        }
        return err;
    }

    int Ardb::HIterate(const DBID& db, const Slice& key, ValueVisitCallback* cb, void* cbdata)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        uint32 total = 0;
        uint32 cursor = 0;
        if (meta->ziped)
        {
            HashFieldMap::iterator it = meta->values.begin();
            while (it != meta->values.end())
            {
                cb(it->first, cursor++, cbdata);
                cb(it->second, cursor++, cbdata);
                it++;
            }
            total = meta->values.size();
        }
        else
        {
            Slice empty;
            HashKeyObject k(key, empty, db);
            struct HashWalk: public WalkHandler
            {
                    ValueVisitCallback* vcb;
                    void* vcb_data;
                    uint32 size;
                    uint32 cur;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        CommonValueObject* cv = (CommonValueObject*) v;
                        HashKeyObject* sek = (HashKeyObject*) k;
                        vcb(sek->field, cur++, vcb_data);
                        vcb(cv->data, cur++, vcb_data);
                        size++;
                        return 0;
                    }
                    HashWalk(ValueVisitCallback* cb, void* cbdata) :
                            vcb(cb), vcb_data(cbdata), size(0), cur(0)
                    {
                    }
            } walk(cb, cbdata);
            Walk(k, false, true, &walk);
            if (meta->dirty)
            {
                meta->dirty = false;
                meta->size = walk.size;
                SetMeta(db, key, *meta);
            }
            total = walk.size;
        }
        DELETE(meta);
        return total == 0 ? ERR_NOT_EXIST : 0;
    }

    int Ardb::HGet(const DBID& db, const Slice& key, const Slice& field, std::string* value)
    {
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, NULL, valueObj);
        if (ret == 0)
        {
            if (NULL != value)
            {
                valueObj.data.ToString(*value);
            }
            return 0;
        }
        return ret;
    }

    int Ardb::HMSet(const DBID& db, const Slice& key, const SliceArray& fields, const SliceArray& values)
    {
        if (fields.size() != values.size())
        {
            return ERR_INVALID_ARGS;
        }
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        SliceArray::const_iterator it = fields.begin();
        SliceArray::const_iterator sit = values.begin();
        while (it != fields.end())
        {
            CommonValueObject valueobject;
            valueobject.data.SetValue(*sit, true);
            HashKeyObject hk(key, *it, db);
            HSetValue(hk, meta, valueobject);
            it++;
            sit++;
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::HMGet(const DBID& db, const Slice& key, const SliceArray& fields, StringArray& values)
    {
        SliceArray::const_iterator it = fields.begin();
        while (it != fields.end())
        {
            std::string valueObj;
            HGet(db, key, *it, &valueObj);
            values.push_back(valueObj);
            it++;
        }
        return 0;
    }

    int Ardb::HClear(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        if (NULL != m_level1_cahce)
        {
            m_level1_cahce->Evict(db, key);
        }
        if (!meta->ziped)
        {
            Slice empty;
            HashKeyObject sk(key, empty, db);
            struct HClearWalk: public WalkHandler
            {
                    Ardb* z_db;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        HashKeyObject* sek = (HashKeyObject*) k;
                        z_db->DelValue(*sek);
                        return 0;
                    }
                    HClearWalk(Ardb* db) :
                            z_db(db)
                    {
                    }
            } walk(this);
            BatchWriteGuard guard(GetEngine());
            Walk(sk, false, false, &walk);
        }
        DelMeta(db, key, meta);
        DELETE(meta);
        return 0;
    }

    static int HashStoreCallback(const ValueData& value, int cursor, void* cb)
    {
        HashIterContext* ctx = (HashIterContext*) cb;
        int left = cursor % 2;
        switch (left)
        {
            case 0:
            {
                //ctx->field = value;
                if (NULL != ctx->fields)
                {
                    ctx->fields->push_back(value.AsString());
                }
                break;
            }
            case 1:
            {
                //ctx->value = value;
                if (NULL != ctx->values)
                {
                    ctx->values->push_back(value.AsString());
                }
                break;
            }
            default:
            {
                break;
            }
        }
        return 0;
    }

    int Ardb::HKeys(const DBID& db, const Slice& key, StringArray& fields)
    {
        HashIterContext ctx;
        ctx.fields = &fields;
        int ret = HIterate(db, key, HashStoreCallback, &ctx);
        if (ret != 0)
        {
            return ret;
        }
        return fields.empty() ? ERR_NOT_EXIST : 0;
    }

    int Ardb::HLen(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        int len = 0;
        if (meta->ziped)
        {
            len = meta->values.size();
        }
        else
        {
            if (!meta->dirty)
            {
                len = meta->size;
            }
            else
            {
                Slice empty;
                HashKeyObject k(key, empty, db);
                Iterator* it = FindValue(k);
                while (NULL != it && it->Valid())
                {
                    Slice tmpkey = it->Key();
                    KeyObject* kk = decode_key(tmpkey, &k);
                    if (NULL == kk)
                    {
                        break;
                    }
                    len++;
                    it->Next();
                    DELETE(kk);
                }
                DELETE(it);
                meta->size = len;
                meta->dirty = false;
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return len;
    }

    int Ardb::HVals(const DBID& db, const Slice& key, StringArray& values)
    {
        HashIterContext ctx;
        ctx.values = &values;
        int ret = HIterate(db, key, HashStoreCallback, &ctx);
        if (ret != 0)
        {
            return ret;
        }
        return values.empty() ? ERR_NOT_EXIST : 0;
    }

    int Ardb::HGetAll(const DBID& db, const Slice& key, StringArray& fields, StringArray& values)
    {
        HashIterContext ctx;
        ctx.values = &values;
        ctx.fields = &fields;
        int ret = HIterate(db, key, HashStoreCallback, &ctx);
        if (ret != 0)
        {
            return ret;
        }
        return fields.empty() ? ERR_NOT_EXIST : 0;
    }

    bool Ardb::HExists(const DBID& db, const Slice& key, const Slice& field)
    {
        return HGet(db, key, field, NULL) == 0;
    }

    int Ardb::HIncrby(const DBID& db, const Slice& key, const Slice& field, int64_t increment, int64_t& value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, meta, valueObj);
        if (0 == ret)
        {
            if (valueObj.data.type != INTEGER_VALUE)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            valueObj.data.Incrby(increment);
            value = valueObj.data.integer_value;
        }
        else
        {
            valueObj.data.SetIntValue(increment);
            value = valueObj.data.integer_value;
        }
        ret = HSetValue(hk, meta, valueObj);
        DELETE(meta);
        return ret;
    }

    int Ardb::HMIncrby(const DBID& db, const Slice& key, const SliceArray& fields, const Int64Array& increments,
            Int64Array& vs)
    {
        if (fields.size() != increments.size())
        {
            return ERR_INVALID_ARGS;
        }
        vs.clear();
        SliceArray::const_iterator it = fields.begin();
        Int64Array::const_iterator sit = increments.begin();
        while (it != fields.end())
        {
            int64 v = 0;
            HIncrby(db, key, *it, *sit, v);
            vs.push_back(v);
            it++;
            sit++;
        }
        return 0;
    }

    int Ardb::HIncrbyFloat(const DBID& db, const Slice& key, const Slice& field, double increment, double& value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, meta, valueObj);
        if (0 == ret)
        {
            if (valueObj.data.type != INTEGER_VALUE && valueObj.data.type != DOUBLE_VALUE)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            valueObj.data.IncrbyFloat(increment);
            value = valueObj.data.NumberValue();
        }
        else
        {
            valueObj.data.SetDoubleValue(increment);
            value = valueObj.data.NumberValue();
        }
        ret = HSetValue(hk, meta, valueObj);
        DELETE(meta);
        return ret;
    }

    int Ardb::RenameHash(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, HashMetaValue* meta)
    {
        BatchWriteGuard guard(GetEngine());
        if (meta->ziped)
        {
            DelMeta(db1, key1, meta);
            meta->header.expireat = 0;
            SetMeta(db2, key2, *meta);
        }
        else
        {
            Slice empty;
            HashKeyObject k(key1, empty, db1);
            HashMetaValue hmeta;
            hmeta.dirty = false;
            hmeta.ziped = false;
            struct HashWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    HashMetaValue& hm;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        CommonValueObject* cv = (CommonValueObject*) v;
                        HashKeyObject* sek = (HashKeyObject*) k;
                        sek->key = dst;
                        sek->db = dstdb;
                        z_db->SetKeyValueObject(*sek, *cv);
                        hm.size++;
                        return 0;
                    }
                    HashWalk(Ardb* db, const DBID& dbid, const Slice& dstkey, HashMetaValue& m) :
                            z_db(db), dstdb(dbid), dst(dstkey), hm(m)
                    {
                    }
            } walk(this, db2, key2, hmeta);
            Walk(k, false, true, &walk);
            SetMeta(db2, key2, hmeta);
            HClear(db1, key1);
        }
        return 0;
    }

    int Ardb::HScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
            uint32 limit, ValueDataArray& vs, std::string& newcursor)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        std::string from = cursor;
        if (cursor == "0")
        {
            from = "";
        }
        ValueData fromobj(from);
        if (meta->ziped)
        {
            HashFieldMap::iterator fit = meta->values.upper_bound(fromobj);
            while (fit != meta->values.end())
            {
                std::string fstr;
                fit->first.ToString(fstr);
                if ((pattern.empty() || fnmatch(pattern.c_str(), fstr.c_str(), 0) == 0))
                {
                    vs.push_back(fit->first);
                    vs.push_back(fit->second);
                }
                if (vs.size() >= (limit * 2))
                {
                    break;
                }
                fit++;
            }
        }
        else
        {
            HashKeyObject hk(key, from, db);
            struct HGetWalk: public WalkHandler
            {
                    ValueDataArray& h_vs;
                    const ValueData& first;
                    int l;
                    const std::string& h_pattern;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        HashKeyObject* sek = (HashKeyObject*) k;
                        if (0 == cursor)
                        {
                            if (first == sek->field)
                            {
                                return 0;
                            }
                        }
                        CommonValueObject* cv = (CommonValueObject*) v;
                        std::string fstr;
                        sek->field.ToString(fstr);
                        if ((h_pattern.empty() || fnmatch(h_pattern.c_str(), fstr.c_str(), 0) == 0))
                        {
                            h_vs.push_back(sek->field);
                            h_vs.push_back(cv->data);
                        }
                        if (l > 0 && h_vs.size() >= (uint32) l * 2)
                        {
                            return -1;
                        }
                        return 0;
                    }
                    HGetWalk(ValueDataArray& fs, int count, const ValueData& s, const std::string& p) :
                            h_vs(fs), first(s), l(count), h_pattern(p)
                    {
                    }
            } walk(vs, limit, hk.field, pattern);
            Walk(hk, false, true, &walk);
        }
        if (vs.empty())
        {
            newcursor = "0";
        }
        else
        {
            vs[vs.size() - 2].ToString(newcursor);
        }
        DELETE(meta);
        return 0;
    }
}


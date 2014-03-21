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

#include "db.hpp"
#include "ardb_server.hpp"
#include <algorithm>
#include <fnmatch.h>

#define MAX_SET_INTER_NUM 500000
#define MAX_SET_UNION_NUM 100000
#define MAX_SET_DIFF_NUM  500000
#define MAX_SET_OP_STORE_NUM 10000
#define MAX_SET_QUERY_NUM 1000000

namespace ardb
{
    int ArdbServer::SAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray values;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            values.push_back(cmd.GetArguments()[i]);
        }
        int count = m_db->SAdd(ctx.currentDB, cmd.GetArguments()[0], values);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::SCard(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->SCard(ctx.currentDB, cmd.GetArguments()[0]);
        fill_int_reply(ctx.reply, ret > 0 ? ret : 0);
        return 0;
    }

    int ArdbServer::SDiff(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        ValueDataArray vs;
        m_db->SDiff(ctx.currentDB, keys, vs);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::SDiffStore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size() || keystrs.count(cmd.GetArguments()[0]) > 0)
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        int ret = m_db->SDiffStore(ctx.currentDB, cmd.GetArguments()[0], keys);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SInter(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        ValueDataArray vs;
        m_db->SInter(ctx.currentDB, keys, vs);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::SInterStore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size() || keystrs.count(cmd.GetArguments()[0]) > 0)
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        int ret = m_db->SInterStore(ctx.currentDB, cmd.GetArguments()[0], keys);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SIsMember(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->SIsMember(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SMembers(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ValueDataArray vs;
        int ret = m_db->SMembers(ctx.currentDB, cmd.GetArguments()[0], vs);
        if (ret == ERR_TOO_LARGE_RESPONSE)
        {
            fill_error_reply(ctx.reply, "too many elements in set, use SRANGE/SREVRANGE to fetch.");
        }
        else
        {
            fill_array_reply(ctx.reply, vs);
        }
        return 0;
    }

    int ArdbServer::SMove(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->SMove(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string res;
        m_db->SPop(ctx.currentDB, cmd.GetArguments()[0], res);
        fill_str_reply(ctx.reply, res);
        return 0;
    }

    int ArdbServer::SRandMember(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        ValueDataArray vs;
        int32 count = 1;
        if (cmd.GetArguments().size() > 1)
        {
            if (!string_toint32(cmd.GetArguments()[1], count))
            {
                fill_error_reply(ctx.reply, "value is not an integer or out of range");
                return 0;
            }
        }
        m_db->SRandMember(ctx.currentDB, cmd.GetArguments()[0], vs, count);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::SRem(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication values in arguments");
            return 0;
        }
        ValueSet vs;
        int ret = m_db->SRem(ctx.currentDB, cmd.GetArguments()[0], keys);
        if (ret < 0)
        {
            ret = 0;
        }
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SUnion(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        ValueDataArray vs;
        m_db->SUnion(ctx.currentDB, keys, vs);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }

    int ArdbServer::SUnionStore(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size() || keystrs.count(cmd.GetArguments()[0]) > 0)
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        int ret = m_db->SUnionStore(ctx.currentDB, cmd.GetArguments()[0], keys);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::SUnionCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        uint32 count = 0;
        m_db->SUnionCount(ctx.currentDB, keys, count);
        fill_int_reply(ctx.reply, count);
        return 0;
    }
    int ArdbServer::SInterCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        uint32 count = 0;
        m_db->SInterCount(ctx.currentDB, keys, count);
        fill_int_reply(ctx.reply, count);
        return 0;
    }
    int ArdbServer::SDiffCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        std::set<std::string> keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
            keystrs.insert(cmd.GetArguments()[i]);
        }

        if (keystrs.size() != keys.size())
        {
            fill_error_reply(ctx.reply, "duplication keys in arguments");
            return 0;
        }
        uint32 count = 0;
        m_db->SDiffCount(ctx.currentDB, keys, count);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::SScan(ArdbConnContext& ctx, RedisCommandFrame& cmd)
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
        m_db->SScan(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], pattern, limit, vs, newcursor);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        ctx.reply.elements.push_back(RedisReply(newcursor));
        RedisReply rs;
        fill_array_reply(rs, vs);
        ctx.reply.elements.push_back(rs);
        return 0;
    }

    int ArdbServer::SClear(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        m_db->SClear(ctx.currentDB, cmd.GetArguments()[0]);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    SetMetaValue* Ardb::GetSetMeta(const DBID& db, const Slice& key, int& err, bool& create)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != SET_META)
        {
            DELETE(meta);
            err = ERR_INVALID_TYPE;
            return NULL;
        }
        if (NULL == meta)
        {
            meta = new SetMetaValue;
            err = ERR_NOT_EXIST;
            create = true;
        }
        else
        {
            SetMetaValue* smeta = (SetMetaValue*) meta;
            if (smeta->ziped && smeta->zipvs.size() > 0)
            {
                smeta->min = *(smeta->zipvs.begin());
                smeta->max = *(smeta->zipvs.rbegin());
            }
            if (smeta->ziped)
            {
                smeta->dirty = false;
                smeta->size = smeta->zipvs.size();
            }
            create = false;
        }
        return (SetMetaValue*) meta;
    }
    void Ardb::FindSetMinMaxValue(const DBID& db, const Slice& key, SetMetaValue* meta)
    {
        if (meta->ziped || !meta->dirty)
        {
            return;
        }
        meta->min.Clear();
        meta->max.Clear();
        SetKeyObject sk(key, Slice(), db);
        Iterator* iter = FindValue(sk, false);
        if (NULL != iter && iter->Valid())
        {
            KeyObject* kk = decode_key(iter->Key(), &sk);
            if (NULL != kk)
            {
                SetKeyObject* mink = (SetKeyObject*) kk;
                meta->min = mink->value;
            }
            DELETE(kk);
        }
        DELETE(iter);

        std::string next;
        next_key(key, next);
        SetKeyObject sk1(next, Slice(), db);
        iter = FindValue(sk1, false);
        if (NULL != iter)
        {
            if (iter->Valid())
            {
                iter->Prev();
            }
            else
            {
                iter->SeekToLast();
            }
        }
        if (NULL != iter && iter->Valid())
        {
            KeyObject* kk = decode_key(iter->Key(), &sk);
            if (NULL != kk && kk->type == SET_ELEMENT && kk->db == db && kk->key.compare(key) == 0)
            {
                SetKeyObject* maxk = (SetKeyObject*) kk;
                meta->max = maxk->value;
            }
            DELETE(kk);
        }
        DELETE(iter);
    }

    int Ardb::SAdd(const DBID& db, const Slice& key, const SliceArray& values)
    {
        int count = 0;
        for (uint32 i = 0; i < values.size(); i++)
        {
            count += SAdd(db, key, values[i]);
        }
        return count;
    }

    int Ardb::SAdd(const DBID& db, const Slice& key, const Slice& value)
    {
        KeyLockerGuard guard(m_key_locker, db, key);
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta)
        {
            return err;
        }
        bool zip_save = meta->ziped;
        ValueData element(value);
        if (zip_save)
        {
            if (!meta->zipvs.insert(element).second)
            {
                DELETE(meta);
                return 0;
            }
            if (element.type == BYTES_VALUE && element.bytes_value.size() >= (uint32) m_config.set_max_ziplist_value)
            {
                zip_save = false;
            }
            if (meta->zipvs.size() >= (uint32) m_config.set_max_ziplist_entries)
            {
                zip_save = false;
            }
        }
        if (zip_save)
        {
            meta->size = meta->zipvs.size();
            meta->dirty = false;
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 1;
        }
        /*
         * convert from ziplist
         */
        if (meta->ziped)
        {
            meta->size = meta->zipvs.size();
            ValueSet::iterator it = meta->zipvs.begin();
            while (it != meta->zipvs.end())
            {
                SetKeyObject sk(key, *it, db);
                EmptyValueObject empty;
                SetKeyValueObject(sk, empty);
                it++;
            }
            meta->min = *(meta->zipvs.begin());
            meta->max = *(meta->zipvs.rbegin());
            meta->ziped = false;
            meta->zipvs.clear();
            meta->dirty = false;
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 1;
        }

        SetKeyObject sk(key, element, db);
        EmptyValueObject empty;
        if (meta->dirty)
        {
            SetKeyValueObject(sk, empty);
        }
        else
        {
            meta->dirty = true;
            BatchWriteGuard guard(GetEngine());
            SetKeyValueObject(sk, empty);
            SetMeta(db, key, *meta);
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::SCard(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta)
        {
            return err;
        }
        if (meta->dirty)
        {
            KeyLockerGuard guard(m_key_locker, db, key);
            meta->size = 0;
            SetKeyObject sk(key, Slice(), db);
            struct SMembersWalk: public WalkHandler
            {
                    SetMetaValue* smeta;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        SetKeyObject* sk = (SetKeyObject*) k;
                        if (cursor == 0)
                        {
                            smeta->min = sk->value;
                        }
                        smeta->max = sk->value;
                        smeta->size++;
                        return 0;
                    }
                    SMembersWalk(SetMetaValue* m) :
                            smeta(m)
                    {
                    }
            } walk(meta);
            Walk(sk, false, false, &walk);
            meta->dirty = false;
            SetMeta(db, key, *meta);
        }
        int size = meta->size;
        DELETE(meta);
        return size;
    }

    bool Ardb::SIsMember(const DBID& db, const Slice& key, const Slice& value)
    {
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet)
        {
            DELETE(meta);
            return err;
        }
        ValueData element(value);
        bool exist = false;
        if (meta->ziped)
        {
            exist = meta->zipvs.find(element) != meta->zipvs.end();
        }
        else
        {
            SetKeyObject sk(key, value, db);
            std::string empty;
            exist = (0 == GetRawValue(sk, empty));
        }
        DELETE(meta);
        return exist;
    }

    int Ardb::SRem(const DBID& db, const Slice& key, const SliceArray& values)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet)
        {
            if (createSet)
            {
                err = 0;
            }
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            SliceArray::const_iterator it = values.begin();
            uint32 erased = meta->zipvs.size();
            while (it != values.end())
            {
                ValueData element(*it);
                meta->zipvs.erase(element);
                it++;
            }
            erased -= meta->zipvs.size();
            meta->size = meta->zipvs.size();
            SetMeta(db, key, *meta);
            DELETE(meta);
            return erased;
        }
        BatchWriteGuard guard(GetEngine());
        SliceArray::const_iterator it = values.begin();
        while (it != values.end())
        {
            SetKeyObject sk(key, *it, db);
            DelValue(sk);
            it++;
        }
        if (!meta->dirty)
        {
            meta->dirty = true;
            SetMeta(db, key, *meta);
        }
        return values.size();
    }

    int Ardb::SRem(const DBID& db, const Slice& key, const Slice& value)
    {
        SliceArray vs;
        vs.push_back(value);
        return SRem(db, key, vs);
    }

    int Ardb::SRevRange(const DBID& db, const Slice& key, const Slice& value_end, int count, bool with_end,
            ValueDataArray& values)
    {
        if (count == 0)
        {
            return 0;
        }
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet)
        {
            DELETE(meta);
            return err;
        }
        FindSetMinMaxValue(db, key, meta);
        ValueData firstObj;
        if (value_end.empty() || !strcasecmp(value_end.data(), "+inf"))
        {
            firstObj = meta->max;
            with_end = true;
        }
        else
        {
            firstObj.SetValue(value_end, true);
        }

        if (meta->ziped)
        {
            ValueSet::iterator fit = meta->zipvs.lower_bound(firstObj);
            ValueSet::iterator it = meta->zipvs.begin();
            while (fit != meta->zipvs.begin() && fit != meta->zipvs.end())
            {
                if (!with_end)
                {
                    if (firstObj.Compare(*fit) != 0)
                    {
                        values.push_back(*it);
                    }
                }
                else
                {
                    values.push_back(*it);
                }
                if (values.size() >= (uint32) count)
                {
                    break;
                }
                fit--;
            }
            if (fit == meta->zipvs.begin())
            {
                values.push_back(*fit);
            }
            DELETE(meta);
            return 0;
        }
        SetKeyObject sk(key, firstObj, db);
        struct SGetWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                ValueData& first;
                bool with_first;
                int l;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    SetKeyObject* sek = (SetKeyObject*) k;
                    if (0 == cursor)
                    {
                        if (!with_first)
                        {
                            if (first.Compare(sek->value) == 0)
                            {
                                return 0;
                            }
                        }
                    }
                    z_values.push_back(sek->value);
                    if (l > 0 && z_values.size() >= (uint32) l)
                    {
                        return -1;
                    }
                    return 0;
                }
                SGetWalk(ValueDataArray& vs, int count, ValueData& s, bool with) :
                        z_values(vs), first(s), with_first(with), l(count)
                {
                }
        } walk(values, count, firstObj, with_end);
        Walk(sk, true, false, &walk);
        return 0;
    }

    int Ardb::SetRange(const DBID& db, const Slice& key, SetMetaValue* meta, const ValueData& value_begin,
            const ValueData& value_end, int32 limit, bool with_begin, const std::string& pattern,
            ValueDataArray& values)
    {
        if (limit == 0)
        {
            return 0;
        }
        if (meta->ziped)
        {
            ValueSet::iterator fit = meta->zipvs.lower_bound(value_begin);
            while (fit != meta->zipvs.end())
            {
                if (value_end.Compare(*fit) < 0)
                {
                    break;
                }
                if (value_begin.Compare(*fit) == 0)
                {
                    if (with_begin)
                    {
                        if (pattern.empty())
                        {
                            values.push_back(*fit);
                        }
                        else
                        {
                            std::string str;
                            fit->ToString(str);
                            if (fnmatch(pattern.c_str(), str.c_str(), 0) == 0)
                            {
                                values.push_back(*fit);
                            }
                        }
                    }
                }
                else
                {
                    if (pattern.empty())
                    {
                        values.push_back(*fit);
                    }
                    else
                    {
                        std::string str;
                        fit->ToString(str);
                        if (fnmatch(pattern.c_str(), str.c_str(), 0) == 0)
                        {
                            values.push_back(*fit);
                        }
                    }
                    if (limit > 0 && values.size() >= (uint32) limit)
                    {
                        break;
                    }
                }
                fit++;
            }
            return 0;
        }

        SetKeyObject sk(key, value_begin, db);
        struct SGetWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                const ValueData& first;
                const ValueData& end;
                bool with_first;
                int32 l;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    SetKeyObject* sek = (SetKeyObject*) k;
                    if (0 == cursor)
                    {
                        if (!with_first)
                        {
                            if (first.Compare(sek->value) == 0)
                            {
                                return 0;
                            }
                        }
                    }
                    if (end.Compare(sek->value) < 0)
                    {
                        return -1;
                    }
                    z_values.push_back(sek->value);
                    if (l > 0 && z_values.size() >= (uint32) l)
                    {
                        return -1;
                    }
                    return 0;
                }
                SGetWalk(ValueDataArray& vs, int32 count, const ValueData& s, const ValueData& e, bool with) :
                        z_values(vs), first(s), end(e), with_first(with), l(count)
                {
                }
        } walk(values, limit, value_begin, value_end, with_begin);
        Walk(sk, false, false, &walk);
        return 0;
    }

    int Ardb::SScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
            uint32 limit, ValueDataArray& vs, std::string& newcursor)
    {
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        std::string from = cursor;
        if (cursor == "0")
        {
            from = "";
        }
        ValueData start(from);
        ValueData end;
        end.type = MAX_VALUE_TYPE;
        SetRange(db, key, meta, start, end, limit, false, pattern, vs);
        if (vs.empty())
        {
            newcursor = "0";
        }
        else
        {
            vs[vs.size() - 1].ToString(newcursor);
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::SRange(const DBID& db, const Slice& key, const Slice& value_begin, int count, bool with_begin,
            ValueDataArray& values)
    {
        if (count == 0)
        {
            return 0;
        }
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        if (meta->dirty)
        {
            FindSetMinMaxValue(db, key, meta);
        }
        SetKeyObject sk(key, value_begin, db);
        ValueData end_obj;
        if (value_begin.empty() || !strcasecmp(value_begin.data(), "-inf"))
        {
            if (!meta->ziped)
            {
                sk.value = meta->min;
                end_obj = meta->max;
            }
            else
            {
                end_obj = *(meta->zipvs.rbegin());
            }
            with_begin = true;
        }
        std::string empty_pattern;
        SetRange(db, key, meta, sk.value, end_obj, count, with_begin, empty_pattern, values);
        DELETE(meta);
        return 0;
    }

    int Ardb::SMembers(const DBID& db, const Slice& key, ValueDataArray& values)
    {
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            ValueSet::iterator it = meta->zipvs.begin();
            while (it != meta->zipvs.end())
            {
                values.push_back(*it);
                it++;
            }
            DELETE(meta);
            return 0;
        }
        if (meta->size >= MAX_SET_QUERY_NUM)
        {
            return ERR_TOO_LARGE_RESPONSE;
        }
        SetKeyObject sk(key, meta->dirty ? Slice() : meta->min, db);
        struct SMembersWalk: public WalkHandler
        {
                ValueDataArray& z_values;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    SetKeyObject* sek = (SetKeyObject*) k;
                    z_values.push_back(sek->value);
                    return 0;
                }
                SMembersWalk(ValueDataArray& vs) :
                        z_values(vs)
                {
                }
        } walk(values);
        Walk(sk, false, false, &walk);
        DELETE(meta);
        return 0;
    }

    int Ardb::SClear(const DBID& db, const Slice& key)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        Slice empty;
        SetKeyObject sk(key, empty, db);
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        if (!meta->ziped)
        {
            struct SClearWalk: public WalkHandler
            {
                    Ardb* z_db;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        SetKeyObject* sek = (SetKeyObject*) k;
                        z_db->DelValue(*sek);
                        return 0;
                    }
                    SClearWalk(Ardb* db) :
                            z_db(db)
                    {
                    }
            } walk(this);
            Walk(sk, false, false, &walk);
        }
        DelMeta(db, key, meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::SDiffCount(const DBID& db, SliceArray& keys, uint32& count)
    {
        struct CountCallback: public SetOperationCallback
        {
                uint32 count;
                void OnSubset(ValueDataArray& array)
                {
                    //INFO_LOG("size = %u",array.size());
                    count += array.size();
                }
        } callback;
        callback.count = 0;
        SDiff(db, keys, &callback, MAX_SET_DIFF_NUM);
        count = callback.count;
        return 0;
    }

    int Ardb::SDiff(const DBID& db, SliceArray& keys, ValueDataArray& values)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct DiffCallback: public SetOperationCallback
        {
                ValueDataArray& vs;
                DiffCallback(ValueDataArray& v) :
                        vs(v)
                {
                }
                void OnSubset(ValueDataArray& array)
                {
                    vs.insert(vs.end(), array.begin(), array.end());
                }
        } callback(values);
        SDiff(db, keys, &callback, MAX_SET_DIFF_NUM);
        return 0;
    }

    int Ardb::SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct DiffCallback: public SetOperationCallback
        {
                Ardb* db;
                const DBID& dbid;
                const Slice& key;
                SetMetaValue meta;
                DiffCallback(Ardb* a, const DBID& id, const Slice& k) :
                        db(a), dbid(id), key(k)
                {
                    meta.ziped = false;
                }
                void OnSubset(ValueDataArray& array)
                {
                    BatchWriteGuard guard(db->GetEngine());
                    meta.size += array.size();
                    meta.max = *(array.rbegin());
                    if (meta.min.type == EMPTY_VALUE)
                    {
                        meta.min = *(array.begin());
                    }
                    ValueDataArray::iterator it = array.begin();
                    while (it != array.end())
                    {
                        SetKeyObject sk(key, *it, dbid);
                        EmptyValueObject empty;
                        db->SetKeyValueObject(sk, empty);
                        it++;
                    }
                    db->SetMeta(dbid, key, meta);
                }
        } callback(this, db, dst);
        callback.db = this;
        SClear(db, dst);
        KeyLockerGuard keyguard(m_key_locker, db, dst);
        SDiff(db, keys, &callback, MAX_SET_OP_STORE_NUM);
        return callback.meta.size;
    }

    int Ardb::SInterCount(const DBID& db, SliceArray& keys, uint32& count)
    {
        struct CountCallback: public SetOperationCallback
        {
                uint32 count;
                void OnSubset(ValueDataArray& array)
                {
                    //INFO_LOG("size = %u",array.size());
                    count += array.size();
                }
        } callback;
        callback.count = 0;
        SInter(db, keys, &callback, MAX_SET_INTER_NUM);
        count = callback.count;
        return 0;
    }

    int Ardb::SDiff(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        SetMetaValueArray metas;
        SliceArray::iterator kit = keys.begin();
        while (kit != keys.end())
        {
            int err = 0;
            bool createSet = false;
            SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
            if (NULL == meta)
            {
                delete_pointer_container(metas);
                return err;
            }
            FindSetMinMaxValue(db, *kit, meta);
            metas.push_back(meta);
            kit++;
        }
        std::vector<ValueData> fromObjs;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            fromObjs.push_back(metas[i]->min);
        }
        bool isfirst = true;
        std::string empty_pattern;
        while (true)
        {
            ValueDataArray base;
            ValueDataArray next;
            ValueDataArray* cmp = &base;
            ValueDataArray* result = &next;
            SetRange(db, keys[0], metas[0], fromObjs[0], metas[0]->max, max_subset_num, isfirst, empty_pattern, *cmp);
            if (cmp->empty())
            {
                return 0;
            }
            fromObjs[0] = *(cmp->rbegin());
            for (uint32 i = 1; i < keys.size(); i++)
            {
                ValueDataArray tmp;
                SetRange(db, keys[i], metas[i], fromObjs[i], fromObjs[0], -1, isfirst, empty_pattern, tmp);
                if (!tmp.empty())
                {
                    fromObjs[i] = *(tmp.rbegin());
                }
                else
                {
                    fromObjs[i] = fromObjs[0];
                }
                result->clear();
                result->resize(cmp->size());
                ValueDataArray::iterator ret = std::set_difference(cmp->begin(), cmp->end(), tmp.begin(), tmp.end(),
                        result->begin());
                if (ret == result->begin())
                {
                    cmp->clear();
                    break;
                }
                else
                {
                    result->resize(ret - result->begin());
                    ValueDataArray* p = cmp;
                    cmp = result;
                    result = p;
                }
            }
            isfirst = false;
            if (!cmp->empty())
            {
                callback->OnSubset(*cmp);
            }
        }
        delete_pointer_container(metas);
        return 0;
    }

    int Ardb::SUnion(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        SetMetaValueArray metas;
        ValueData min;
        ValueData max;
        SliceArray::iterator kit = keys.begin();
        while (kit != keys.end())
        {
            int err = 0;
            bool createSet = false;
            SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
            if (NULL == meta)
            {
                delete_pointer_container(metas);
                return err;
            }
            FindSetMinMaxValue(db, *kit, meta);
            metas.push_back(meta);
            if (min.type == EMPTY_VALUE || min.Compare(meta->min) < 0)
            {
                min = meta->min;
            }
            if (max.type == EMPTY_VALUE || max.Compare(meta->max) > 0)
            {
                max = meta->max;
            }
            kit++;
        }
        std::vector<ValueData> fromObjs;
        std::set<uint32> readySetKeyIdxs;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            fromObjs.push_back(metas[i]->min);
            readySetKeyIdxs.insert(i);
        }

        bool isfirst = true;
        std::string empty_pattern;
        while (!readySetKeyIdxs.empty())
        {
            ValueDataArray base;
            ValueDataArray next;
            ValueDataArray* cmp = &base;
            ValueDataArray* result = &next;
            uint32 firtidx = *(readySetKeyIdxs.begin());
            std::string str1, str2;
            SetRange(db, keys[firtidx], metas[firtidx], fromObjs[firtidx], metas[firtidx]->max, max_subset_num, isfirst,
                    empty_pattern, *cmp);
            if (!cmp->empty())
            {
                fromObjs[firtidx] = *(cmp->rbegin());
            }
            else
            {
                fromObjs[firtidx] = metas[firtidx]->max;
            }
            std::set<uint32>::iterator iit = readySetKeyIdxs.begin();
            iit++;
            std::set<uint32> finishedidxs;
            while (iit != readySetKeyIdxs.end())
            {
                uint32 idx = *iit;
                ValueDataArray tmp;
                SetRange(db, keys[idx], metas[idx], fromObjs[idx], fromObjs[firtidx], -1, isfirst, empty_pattern, tmp);
                if (!tmp.empty())
                {
                    fromObjs[idx] = *(tmp.rbegin());
                }
                else
                {
                    fromObjs[idx] = fromObjs[firtidx];
                }
                result->clear();
                result->resize(cmp->size() + tmp.size());
                ValueDataArray::iterator ret = std::set_union(cmp->begin(), cmp->end(), tmp.begin(), tmp.end(),
                        result->begin());
                if (ret != result->begin())
                {
                    result->resize(ret - result->begin());
                    ValueDataArray* p = cmp;
                    cmp = result;
                    result = p;
                }
                else
                {

                }
                if (fromObjs[idx] >= metas[idx]->max)
                {
                    finishedidxs.insert(idx);
                }
                iit++;
            }

            isfirst = false;
            if (!cmp->empty())
            {
                callback->OnSubset(*cmp);
            }
            if (fromObjs[firtidx] >= metas[firtidx]->max)
            {
                finishedidxs.insert(firtidx);
            }
            std::set<uint32>::iterator eit = finishedidxs.begin();
            while (eit != finishedidxs.end())
            {
                readySetKeyIdxs.erase(*eit);
                eit++;
            }
        }
        delete_pointer_container(metas);
        return 0;
    }

    int Ardb::SInter(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        int32 min_size = -1;
        SetMetaValueArray metas;
        uint32 min_idx = 0;
        uint32 idx = 0;
        ValueData min;
        ValueData max;
        SliceArray::iterator kit = keys.begin();
        while (kit != keys.end())
        {
            int err = 0;
            bool createSet = false;
            SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
            if (NULL == meta || createSet)
            {
                DELETE(meta);
                delete_pointer_container(metas);
                return err;
            }
            FindSetMinMaxValue(db, *kit, meta);
            if (!createSet)
            {
                if (min_size == -1 || min_size > (int32) meta->size)
                {
                    min_size = meta->size;
                    min_idx = idx;
                }
            }
            if (meta->size == 0)
            {
                delete_pointer_container(metas);
                return 0;
            }
            if (min.type == EMPTY_VALUE || min.Compare(meta->min) < 0)
            {
                min = meta->min;
            }
            if (max.type == EMPTY_VALUE || max.Compare(meta->max) > 0)
            {
                max = meta->max;
            }
            metas.push_back(meta);
            kit++;
            idx++;
        }
        std::vector<ValueData> fromObjs;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            fromObjs.push_back(min);
        }
        bool isfirst = true;
        std::string empty_pattern;
        while (true)
        {
            ValueDataArray base;
            ValueDataArray next;
            ValueDataArray* cmp = &base;
            ValueDataArray* result = &next;
            /*
             * Use the smallest set as the base set
             */
            SetRange(db, keys[min_idx], metas[min_idx], fromObjs[min_idx], max, max_subset_num, isfirst, empty_pattern,
                    *cmp);
            if (cmp->empty())
            {
                delete_pointer_container(metas);
                return 0;
            }
            fromObjs[min_idx] = *(cmp->rbegin());
            for (uint32 i = 0; i < keys.size(); i++)
            {
                if (i != min_idx)
                {
                    result->clear();
                    result->resize(cmp->size());
                    ValueDataArray tmp;
                    SetRange(db, keys[i], metas[i], fromObjs[i], fromObjs[min_idx], -1, isfirst, empty_pattern, tmp);
                    if (tmp.empty())
                    {
                        return 0;
                    }
                    fromObjs[i] = *(tmp.rbegin());
                    result->resize(cmp->size());
                    ValueDataArray::iterator ret = std::set_intersection(cmp->begin(), cmp->end(), tmp.begin(),
                            tmp.end(), result->begin());
                    if (ret == result->begin())
                    {
                        break;
                    }
                    else
                    {
                        result->resize(ret - result->begin());

                        ValueDataArray* p = cmp;
                        cmp = result;
                        result = p;
                    }
                }
            }
            isfirst = false;
            if (!cmp->empty())
            {
                callback->OnSubset(*cmp);
            }
        }
        delete_pointer_container(metas);
        return 0;
    }

    int Ardb::SInter(const DBID& db, SliceArray& keys, ValueDataArray& values)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct InterCallback: public SetOperationCallback
        {
                ValueDataArray& vs;
                InterCallback(ValueDataArray& v) :
                        vs(v)
                {
                }
                void OnSubset(ValueDataArray& array)
                {
                    vs.insert(vs.end(), array.begin(), array.end());
                }
        } callback(values);
        SInter(db, keys, &callback, MAX_SET_INTER_NUM);
        return 0;
    }

    int Ardb::SInterStore(const DBID& db, const Slice& dst, SliceArray& keys)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct InterCallback: public SetOperationCallback
        {
                Ardb* db;
                const DBID& dbid;
                const Slice& key;
                SetMetaValue meta;
                InterCallback(Ardb* a, const DBID& id, const Slice& k) :
                        db(a), dbid(id), key(k)
                {
                    meta.ziped = false;
                }
                void OnSubset(ValueDataArray& array)
                {
                    BatchWriteGuard guard(db->GetEngine());
                    meta.size += array.size();
                    meta.max = *(array.rbegin());
                    if (meta.min.type == EMPTY_VALUE)
                    {
                        meta.min = *(array.begin());
                    }
                    ValueDataArray::iterator it = array.begin();
                    while (it != array.end())
                    {
                        SetKeyObject sk(key, *it, dbid);
                        EmptyValueObject empty;
                        db->SetKeyValueObject(sk, empty);
                        it++;
                    }
                    db->SetMeta(dbid, key, meta);
                }
        } callback(this, db, dst);
        callback.db = this;
        SClear(db, dst);
        KeyLockerGuard keyguard(m_key_locker, db, dst);
        SInter(db, keys, &callback, MAX_SET_OP_STORE_NUM);
        return callback.meta.size;
    }

    int Ardb::SMove(const DBID& db, const Slice& src, const Slice& dst, const Slice& value)
    {
        SetKeyObject sk(src, value, db);
        std::string sv;
        if (0 != GetRawValue(sk, sv))
        {
            return 0;
        }
        SRem(db, src, value);
        SAdd(db, dst, value);
        return 1;
    }

    int Ardb::SPop(const DBID& db, const Slice& key, std::string& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            ValueData v = *(meta->zipvs.begin());
            v.ToString(value);
            meta->zipvs.erase(meta->zipvs.begin());
            meta->size--;
            if (meta->size <= 0)
            {
                DelMeta(db, key, meta);
            }
            else
            {
                SetMeta(db, key, *meta);
            }
            DELETE(meta);
            return 0;
        }
        Slice empty;
        SetKeyObject sk(key, meta->dirty ? Slice() : meta->min, db);
        Iterator* iter = FindValue(sk);
        BatchWriteGuard guard(GetEngine());
        if (iter != NULL && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, &sk);
            if (NULL != kk)
            {
                SetKeyObject* sek = (SetKeyObject*) kk;
                sek->value.ToString(value);
                DelValue(*sek);
                DELETE(kk);
            }
        }
        if (!meta->dirty)
        {
            meta->dirty = true;
            SetMeta(db, key, *meta);
        }
        DELETE(iter);
        DELETE(meta);
        return 0;
    }

    int Ardb::SRandMember(const DBID& db, const Slice& key, ValueDataArray& values, int count)
    {
        int err = 0;
        bool createSet = false;
        SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
        if (NULL == meta || createSet || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            ValueSet::iterator vit = meta->zipvs.begin();
            while (vit != meta->zipvs.end())
            {
                values.push_back(*vit);
                if (values.size() >= (uint32) count)
                {
                    break;
                }
                vit++;
            }
            DELETE(meta);
            return 0;
        }
        Slice empty;
        SetKeyObject sk(key, empty, db);
        Iterator* iter = FindValue(sk, true);
        uint32 total = count;
        if (count < 0)
        {
            total = 0 - count;
        }
        uint32 cursor = 0;
        while (iter != NULL && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, &sk);
            if (NULL == kk)
            {
                DELETE(kk);
                break;
            }
            SetKeyObject* sek = (SetKeyObject*) kk;
            values.push_back(sek->value);
            DELETE(kk);
            iter->Next();
            cursor++;
            if (cursor == total)
            {
                break;
            }
        }
        DELETE(iter);
        while (!values.empty() && count < 0 && values.size() < total)
        {
            values.push_back(values.front());
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::SUnionCount(const DBID& db, SliceArray& keys, uint32& count)
    {
        struct CountCallback: public SetOperationCallback
        {
                uint32 count;
                void OnSubset(ValueDataArray& array)
                {
                    count += array.size();
                }
        } callback;
        callback.count = 0;
        SUnion(db, keys, &callback, MAX_SET_UNION_NUM);
        count = callback.count;
        return 0;
    }

    int Ardb::SUnion(const DBID& db, SliceArray& keys, ValueDataArray& values)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct UnionCallback: public SetOperationCallback
        {
                ValueDataArray& result;
                UnionCallback(ValueDataArray& r) :
                        result(r)
                {
                }
                void OnSubset(ValueDataArray& array)
                {
                    result.insert(result.end(), array.begin(), array.end());
                }
        } callback(values);
        SUnion(db, keys, &callback, MAX_SET_UNION_NUM);
        return 0;
    }

    int Ardb::SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_ARGS;
        }
        struct UnionCallback: public SetOperationCallback
        {
                Ardb* db;
                const DBID& dbid;
                const Slice& key;
                SetMetaValue meta;
                UnionCallback(Ardb* a, const DBID& id, const Slice& k) :
                        db(a), dbid(id), key(k)
                {
                    meta.ziped = false;
                }
                void OnSubset(ValueDataArray& array)
                {
                    BatchWriteGuard guard(db->GetEngine());
                    meta.size += array.size();
                    if (array.size() > 0)
                    {
                        meta.max = *(array.rbegin());
                        if (meta.min.type == EMPTY_VALUE)
                        {
                            meta.min = *(array.begin());
                        }
                        ValueDataArray::iterator it = array.begin();
                        while (it != array.end())
                        {
                            SetKeyObject sk(key, *it, dbid);
                            EmptyValueObject empty;
                            db->SetKeyValueObject(sk, empty);
                            it++;
                        }
                        db->SetMeta(dbid, key, meta);
                    }
                }
        } callback(this, db, dst);
        callback.db = this;
        SClear(db, dst);
        KeyLockerGuard keyguard(m_key_locker, db, dst);
        SUnion(db, keys, &callback, MAX_SET_OP_STORE_NUM);
        return callback.meta.size;
    }

    int Ardb::RenameSet(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, SetMetaValue* meta)
    {
        BatchWriteGuard guard(GetEngine());
        if (meta->ziped)
        {
            DelMeta(db1, key1, meta);
            SetMeta(db2, key2, *meta);
        }
        else
        {
            Slice empty;
            SetKeyObject k(key1, empty, db1);
            SetMetaValue smeta;
            smeta.ziped = false;
            smeta.dirty = false;
            struct SetWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    SetMetaValue& sme;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        SetKeyObject* sek = (SetKeyObject*) k;
                        sek->key = dst;
                        sek->db = dstdb;
                        EmptyValueObject empty;
                        z_db->SetKeyValueObject(*sek, empty);
                        sme.size++;
                        if (cursor == 0)
                        {
                            sme.min = sek->value;
                        }
                        sme.max = sek->value;
                        return 0;
                    }
                    SetWalk(Ardb* db, const DBID& dbid, const Slice& dstkey, SetMetaValue& s) :
                            z_db(db), dstdb(dbid), dst(dstkey), sme(s)
                    {
                    }
            } walk(this, db2, key2, smeta);
            Walk(k, false, true, &walk);
            SetMeta(db2, key2, smeta);
            SClear(db1, key1);
        }
        return 0;
    }

    int Ardb::SLastMember(const DBID& db, const Slice& key, ValueData& member)
    {
        std::string next;
        next_key(key, next);
        SetKeyObject hk(key, next, db);
        Iterator* iter = FindValue(hk, false);
        int err = ERR_NOT_EXIST;
        if (iter != NULL && iter->Valid())
        {
            iter->Prev();
            if (iter->Valid())
            {
                KeyObject* kk = decode_key(iter->Key(), NULL);
                if (NULL != kk && kk->type == SET_ELEMENT && kk->key.compare(key) == 0)
                {
                    SetKeyObject* tmp = (SetKeyObject*) kk;
                    member = tmp->value;
                    err = 0;
                }
                DELETE(kk);
            }
        }
        DELETE(iter);
        return err;
    }
    int Ardb::SFirstMember(const DBID& db, const Slice& key, ValueData& member)
    {
        SetKeyObject sk(key, "", db);
        Iterator* iter = FindValue(sk, false);
        int err = ERR_NOT_EXIST;
        if (iter != NULL && iter->Valid())
        {
            KeyObject* kk = decode_key(iter->Key(), NULL);
            if (NULL != kk && kk->type == SET_ELEMENT && kk->key.compare(key) == 0)
            {
                SetKeyObject* tmp = (SetKeyObject*) kk;
                member = tmp->value;
                err = 0;
            }
            DELETE(kk);
        }
        DELETE(iter);
        return err;
    }
}


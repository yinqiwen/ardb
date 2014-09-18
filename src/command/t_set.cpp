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
#include <float.h>

OP_NAMESPACE_BEGIN

    int Ardb::GetSetMinMax(Context& ctx, ValueObject& meta, Data& min, Data& max)
    {
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            if (meta.meta.zipset.size() > 0)
            {
                min = *(meta.meta.zipset.begin());
                max = *(meta.meta.zipset.rbegin());
            }
        }
        else
        {
            min = meta.meta.min_index;
            max = meta.meta.max_index;
        }
        return 0;
    }

    int Ardb::SetAdd(Context& ctx, ValueObject& meta, const std::string& value, bool& meta_change)
    {
        meta_change = false;
        uint32 count = 0;
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            Data element;
            element.SetString(value, true);
            meta_change = meta.meta.zipset.insert(element).second;
            if (!meta_change)
            {
                return 0;
            }
            count++;
            if (!meta.attach.force_zipsave
                    && (meta.meta.zipset.size() > m_cfg.set_max_ziplist_entries
                            || element.StringLength() >= m_cfg.set_max_ziplist_value) && meta.meta.zipset.size() > 1)
            {
                meta.meta.SetEncoding(COLLECTION_ECODING_RAW);
                DataSet::iterator it = meta.meta.zipset.begin();
                BatchWriteGuard guard(GetKeyValueEngine());
                while (it != meta.meta.zipset.end())
                {
                    ValueObject v;
                    v.type = SET_ELEMENT;
                    v.key.type = SET_ELEMENT;
                    v.key.key = meta.key.key;
                    v.key.db = ctx.currentDB;
                    v.key.element = *it;
                    SetKeyValue(ctx, v);
                    it++;
                }
                meta.meta.min_index = *(meta.meta.zipset.begin());
                meta.meta.max_index = *(meta.meta.zipset.rbegin());
                meta.meta.len = meta.meta.zipset.size();
                meta.meta.zipset.clear();
            }
            meta_change = true;
        }
        else
        {
            ValueObject v(SET_ELEMENT);
            v.key.type = SET_ELEMENT;
            v.key.key = meta.key.key;
            v.key.db = ctx.currentDB;
            v.key.element.SetString(value, true);
            if (meta.meta.min_index > v.key.element)
            {
                meta.meta.min_index = v.key.element;
                meta_change = true;
            }
            if (meta.meta.max_index < v.key.element)
            {
                meta.meta.max_index = v.key.element;
                meta_change = true;
            }
            if (meta.meta.len != -1)
            {
                meta.meta.len = -1;
                meta_change = true;
            }
            SetKeyValue(ctx, v);
            count++;
        }
        return count;
    }

    int Ardb::SReplace(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        meta.key.db = ctx.currentDB;
        meta.key.key = cmd.GetArguments()[0];
        meta.key.type = KEY_META;
        meta.type = SET_META;
        meta.meta.SetEncoding(COLLECTION_ECODING_ZIPSET);
        bool meta_change = false;
        meta.attach.force_zipsave = true;
        int64 count = 0;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            bool v;
            count += SetAdd(ctx, meta, cmd.GetArguments()[i], v);
            if (v)
            {
                meta_change = true;
            }
        }
        if (meta_change)
        {
            SetKeyValue(ctx, meta);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::SAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        if (m_cfg.replace_for_multi_sadd && cmd.GetArguments().size() > 2)
        {
            SReplace(ctx, cmd);
            return 0;
        }
        BatchWriteGuard guard(GetKeyValueEngine());
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        int64 count = 0;
        bool meta_change = false;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            bool v;
            count += SetAdd(ctx, meta, cmd.GetArguments()[i], v);
            if (v)
            {
                meta_change = true;
            }
        }
        if (meta_change)
        {
            SetKeyValue(ctx, meta);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::SetLen(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (meta.meta.Length() >= 0)
        {
            fill_int_reply(ctx.reply, meta.meta.Length());
        }
        else
        {
            SetIterator iter;
            SetIter(ctx, meta, meta.meta.min_index, iter, false);
            uint32 count = 0;
            while (iter.Valid())
            {
                count++;
                iter.Next();
            }
            meta.meta.len = count;
            SetKeyValue(ctx, meta);
            fill_int_reply(ctx.reply, meta.meta.Length());
        }
        return 0;
    }

    int Ardb::SCard(Context& ctx, RedisCommandFrame& cmd)
    {
        SetLen(ctx, cmd.GetArguments()[0]);
        return 0;
    }

    bool Ardb::SetIsMember(Context& ctx, ValueObject& meta, Data& element)
    {
        bool exist = false;
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            exist = meta.meta.zipset.count(element) > 0;
        }
        else
        {
            KeyObject k;
            k.type = SET_ELEMENT;
            k.key = meta.key.key;
            k.db = ctx.currentDB;
            k.element = element;
            exist = (0 == GetKeyValue(ctx, k, NULL));
        }
        return exist;
    }

    int Ardb::SIsMember(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }

        Data element;
        element.SetString(cmd.GetArguments()[1], true);
        bool exist = SetIsMember(ctx, meta, element);
        fill_int_reply(ctx.reply, exist ? 1 : 0);
        return 0;
    }

    int Ardb::SetMembers(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (0 != err)
        {
            return 0;
        }
        SetIterator iter;
        SetIter(ctx, meta, meta.meta.min_index, iter, false);
        uint32 count = 0;
        while (iter.Valid())
        {
            count++;
            Data* element = iter.Element();
            RedisReply& r = ctx.reply.AddMember();
            std::string tmp;
            fill_str_reply(r, element->GetDecodeString(tmp));
            iter.Next();
        }
        if (meta.meta.len == -1)
        {
            meta.meta.len = count;
            SetKeyValue(ctx, meta);
        }
        return 0;
    }

    int Ardb::SMembers(Context& ctx, RedisCommandFrame& cmd)
    {
        SetMembers(ctx, cmd.GetArguments()[0]);
        return 0;
    }

    int Ardb::SMove(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        RedisCommandFrame ismember("sismember");
        ismember.AddArg(cmd.GetArguments()[0]);
        ismember.AddArg(cmd.GetArguments()[2]);
        SIsMember(ctx, ismember);
        int moved = 0;
        if (ctx.reply.integer == 1)
        {
            RedisCommandFrame rem("srem");
            rem.AddArg(cmd.GetArguments()[0]);
            rem.AddArg(cmd.GetArguments()[2]);
            SRem(ctx, rem);
            RedisCommandFrame add("sadd");
            add.AddArg(cmd.GetArguments()[1]);
            add.AddArg(cmd.GetArguments()[2]);
            SAdd(ctx, add);
            moved = 1;
        }
        fill_int_reply(ctx.reply, moved);
        return 0;
    }

    int Ardb::SPop(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err || meta.meta.Length() == 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
            return 0;
        }
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            Data& front = *(meta.meta.zipset.begin());
            std::string tmp;
            fill_str_reply(ctx.reply, front.GetDecodeString(tmp));
            meta.meta.zipset.erase(meta.meta.zipset.begin());
            meta.meta.len = meta.meta.zipset.size();
            SetKeyValue(ctx, meta);
        }
        else
        {
            SetIterator iter;
            SetIter(ctx, meta, meta.meta.min_index, iter, false);
            uint32 count = 0;
            bool min_changed = false;
            BatchWriteGuard guard(GetKeyValueEngine());
            while (iter.Valid())
            {
                if (count == 0)
                {
                    Data* element = iter.Element();
                    std::string tmp;
                    fill_str_reply(ctx.reply, element->GetDecodeString(tmp));
                    KeyObject k;
                    k.type = SET_ELEMENT;
                    k.key = meta.key.key;
                    k.db = ctx.currentDB;
                    k.element = *element;
                    DelKeyValue((ctx), k);
                    if (meta.meta.Length() > 0)
                    {
                        meta.meta.len--;
                    }
                }
                else if (count == 1)
                {
                    meta.meta.min_index = *(iter.Element());
                    min_changed = true;
                    break;
                }
                count++;
                iter.Next();
            }
            if (!min_changed || meta.meta.Length() == 0)
            {
                DelKeyValue(ctx, meta.key);
            }
            else
            {
                SetKeyValue(ctx, meta);
            }
        }
        if (ctx.reply.type != REDIS_REPLY_STRING)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        return 0;
    }

    int Ardb::SRandMember(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err || meta.meta.len == 0)
        {
            return 0;
        }
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
        {
            DataSet::iterator it = meta.meta.zipset.begin();
            it.increment_by(random_between_int32(0, meta.meta.zipset.size()));
            std::string tmp;
            fill_str_reply(ctx.reply, it->GetDecodeString(tmp));
        }
        else
        {
            SetIterator iter;
            SetIter(ctx, meta, meta.meta.min_index, iter, false);
            if (iter.Valid())
            {
                std::string tmp;
                fill_str_reply(ctx.reply, iter.Element()->GetDecodeString(tmp));
            }
        }
        return 0;
    }

    int Ardb::SRem(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        int64 count = 0;
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPSET);
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        bool meta_change = false;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            Data element;
            element.SetString(cmd.GetArguments()[i], true);
            if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
            {
                count += meta.meta.zipset.erase(element);
                if (count > 0)
                {
                    meta_change = true;
                }
            }
            else
            {
                KeyObject k;
                k.type = SET_ELEMENT;
                k.key = meta.key.key;
                k.db = ctx.currentDB;
                k.element = element;
                DelKeyValue(ctx, k);
                if (meta.meta.Length() > 0)
                {
                    meta.meta.len = -1;
                    meta_change = true;
                }
                count++;
            }
        }
        if (meta_change)
        {
            SetKeyValue(ctx, meta);
        }
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int Ardb::SetDiff(Context& ctx, const std::string& first, StringSet& keys, const std::string* store, int64* count)
    {
        if (NULL != count)
        {
            *count = 0;
        }
        if (NULL == count && NULL == store)
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, first, SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (keys.count(first) > 0 || 0 != err)
        {
            if (NULL != store)
            {
                DeleteKey(ctx, *store);
            }
            if (NULL != count)
            {
                fill_int_reply(ctx.reply, *count);
            }
            return 0;
        }
        ValueObject dest_meta;
        if (NULL != store)
        {
            int err = GetMetaValue(ctx, *store, SET_META, dest_meta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (dest_meta.meta.Length() != 0)
            {
                DeleteKey(ctx, *store);
            }
            dest_meta.Clear();
            dest_meta.meta.SetEncoding(COLLECTION_ECODING_ZIPSET);
            dest_meta.meta.len = 0;
        }
        BatchWriteGuard guard(GetKeyValueEngine(), NULL != store);
        ValueObjectArray diff_metas;
        StringSet::iterator sit = keys.begin();
        while (sit != keys.end())
        {
            ValueObject kmeta;
            err = GetMetaValue(ctx, *sit, SET_META, kmeta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (0 == err)
            {
                diff_metas.push_back(kmeta);
            }
            sit++;
        }
        SetIterator iter;
        SetIter(ctx, meta, meta.meta.min_index, iter, false);
        while (iter.Valid())
        {
            bool found = false;
            for (uint32 i = 0; i < diff_metas.size(); i++)
            {
                if (SetIsMember(ctx, diff_metas[i], *(iter.Element())))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                if (NULL != count)
                {
                    (*count)++;
                }
                if (NULL != store)
                {
                    bool tmp;
                    std::string tmpstr;
                    iter.Element()->GetDecodeString(tmpstr);
                    SetAdd(ctx, dest_meta, tmpstr, tmp);
                }
                if (NULL == store && NULL == count)
                {
                    RedisReply& r = ctx.reply.AddMember();
                    fill_value_reply(r, *(iter.Element()));
                }
            }
            iter.Next();
        }
        if (NULL == store && NULL == count)
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
        }
        else
        {
            if (NULL != store)
            {
                if (dest_meta.meta.Length() != 0)
                {
                    SetKeyValue(ctx, dest_meta);
                }
            }
            fill_int_reply(ctx.reply, *count);
        }
        return 0;
    }

    int Ardb::SDiff(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        SetDiff(ctx, cmd.GetArguments()[0], keystrs, NULL, NULL);
        return 0;
    }

    int Ardb::SDiffStore(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 2; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetDiff(ctx, cmd.GetArguments()[1], keystrs, cmd.GetArgument(0), &count);
        return 0;
    }
    int Ardb::SDiffCount(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetDiff(ctx, cmd.GetArguments()[0], keystrs, NULL, &count);
        return 0;
    }

    int Ardb::SetInter(Context& ctx, StringSet& keys, const std::string* store, int64* count)
    {
        ValueObject dest_meta;
        if (NULL != store)
        {
            int err = GetMetaValue(ctx, *store, SET_META, dest_meta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (dest_meta.meta.Length() != 0)
            {
                DeleteKey(ctx, *store);
            }
            dest_meta.Clear();
            dest_meta.meta.SetEncoding(COLLECTION_ECODING_ZIPSET);
            dest_meta.meta.len = 0;
        }
        if (NULL == store && NULL == count)
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_INTEGER;
        }
        ValueObjectArray metas;
        StringSet::iterator sit = keys.begin();
        while (sit != keys.end())
        {
            ValueObject kmeta;
            int err = GetMetaValue(ctx, *sit, SET_META, kmeta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (0 == err)
            {
                metas.push_back(kmeta);
            }
            else
            {
                metas.clear();
                break;
            }
            sit++;
        }
        if (metas.empty())
        {
            return 0;
        }
        BatchWriteGuard guard(GetKeyValueEngine(), NULL != store);
        Data max_min, min_max;
        for (uint32 i = 0; i < metas.size(); i++)
        {
            Data min, max;
            GetSetMinMax(ctx, metas[i], min, max);
            if (max_min.IsNil() || min > max_min)
            {
                max_min = min;
            }
            if (min_max.IsNil() || max < min_max)
            {
                min_max = max;
            }
        }

        if (max_min > min_max)
        {
            if (NULL != count)
            {
                fill_int_reply(ctx.reply, 0);
            }
            return 0;
        }
        else if (max_min == min_max)
        {
            for (uint32 i = 0; i < metas.size(); i++)
            {
                if (!SetIsMember(ctx, metas[i], max_min))
                {
                    return 0;
                }
            }
            if (NULL != count)
            {
                *count = 1;
                fill_int_reply(ctx.reply, 1);
            }
            if (NULL != store)
            {
                bool tmp;
                std::string tmpstr;
                min_max.GetDecodeString(tmpstr);
                SetAdd(ctx, dest_meta, tmpstr, tmp);
                SetKeyValue(ctx, dest_meta);
            }
            if (NULL == store && NULL == count)
            {
                RedisReply& r = ctx.reply.AddMember();
                fill_value_reply(r, min_max);
            }
            return 0;
        }
        else
        {
            SetIterator iter;
            SetIter(ctx, metas[0], max_min, iter, false);
            while (iter.Valid())
            {
                if (iter.Element()->Compare(min_max) > 0)
                {
                    break;
                }
                bool match = true;
                for (uint32 i = 1; i < metas.size(); i++)
                {
                    if (!SetIsMember(ctx, metas[i], *(iter.Element())))
                    {
                        match = false;
                        break;
                    }
                }
                if (match)
                {
                    if (NULL != count)
                    {
                        (*count)++;
                    }
                    if (NULL != store)
                    {
                        bool tmp;
                        std::string tmpstr;
                        iter.Element()->GetDecodeString(tmpstr);
                        SetAdd(ctx, dest_meta, tmpstr, tmp);
                    }
                    if (NULL == store && NULL == count)
                    {
                        RedisReply& r = ctx.reply.AddMember();
                        fill_value_reply(r, *(iter.Element()));
                    }
                }
                iter.Next();
            }
            if (NULL != store)
            {
                if (dest_meta.meta.Length() != 0)
                {
                    SetKeyValue((ctx), dest_meta);
                }
            }
            if (NULL != count)
            {
                fill_int_reply(ctx.reply, *count);
            }
            return 0;
        }
    }

    int Ardb::SInter(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        SetInter(ctx, keystrs, NULL, NULL);
        return 0;
    }

    int Ardb::SInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetInter(ctx, keystrs, cmd.GetArgument(0), &count);
        return 0;
    }

    int Ardb::SInterCount(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetInter(ctx, keystrs, NULL, &count);
        return 0;
    }

    int Ardb::SetUnion(Context& ctx, StringSet& keys, const std::string* store, int64* count)
    {
        ValueObject dest_meta;
        if (NULL != store)
        {
            int err = GetMetaValue(ctx, *store, SET_META, dest_meta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (dest_meta.meta.Length() != 0)
            {
                DeleteKey(ctx, *store);
                dest_meta.Clear();
                dest_meta.meta.SetEncoding(COLLECTION_ECODING_ZIPSET);
                dest_meta.meta.len = 0;
            }
        }
        ValueObjectArray metas;
        StringSet::iterator sit = keys.begin();
        while (sit != keys.end())
        {
            ValueObject kmeta;
            int err = GetMetaValue(ctx, *sit, SET_META, kmeta);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
            if (0 == err)
            {
                metas.push_back(kmeta);
            }
            sit++;
        }
        BatchWriteGuard guard(GetKeyValueEngine(), NULL != store);
        DataSet tmpset;
        for (uint32 i = 0; i < metas.size(); i++)
        {
            SetIterator iter;
            SetIter(ctx, metas[i], metas[i].meta.min_index, iter, false);
            while (iter.Valid())
            {
                bool is_new = tmpset.insert(*(iter.Element())).second;
                if (is_new)
                {
                    if (NULL != count)
                    {
                        (*count)++;
                    }
                    if (NULL != store)
                    {
                        bool tmp;
                        std::string tmpstr;
                        iter.Element()->GetDecodeString(tmpstr);
                        SetAdd(ctx, dest_meta, tmpstr, tmp);
                    }
                    if (NULL == store && NULL == count)
                    {
                        RedisReply& r = ctx.reply.AddMember();
                        fill_value_reply(r, *(iter.Element()));
                    }
                }
                iter.Next();
            }
        }
        if (NULL == store && NULL == count)
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
        }
        if (NULL != store)
        {
            if (dest_meta.meta.Length() != 0)
            {
                SetKeyValue(ctx, dest_meta);
            }
        }
        if (NULL != count)
        {
            fill_int_reply(ctx.reply, *count);
        }
        return 0;

    }

    int Ardb::SUnion(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        SetUnion(ctx, keystrs, NULL, NULL);
        return 0;
    }

    int Ardb::SUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetUnion(ctx, keystrs, cmd.GetArgument(0), &count);
        return 0;
    }

    int Ardb::SUnionCount(Context& ctx, RedisCommandFrame& cmd)
    {
        StringSet keystrs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keystrs.insert(cmd.GetArguments()[i]);
        }
        int64 count = 0;
        SetUnion(ctx, keystrs, NULL, &count);
        return 0;
    }

    int Ardb::SScan(Context& ctx, RedisCommandFrame& cmd)
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
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], SET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        RedisReply& r1 = ctx.reply.AddMember();
        RedisReply& r2 = ctx.reply.AddMember();
        r2.type = REDIS_REPLY_ARRAY;
        if (err != 0)
        {
            fill_str_reply(r1, "0");
            return 0;
        }
        SetIterator iter;
        std::string scan_start_element;
        const std::string& cursor = cmd.GetArguments()[1];
        if (cursor != "0")
        {
            if (m_cfg.scan_redis_compatible)
            {
                FindElementByRedisCursor(cursor, scan_start_element);
            }
        }
        Data start_element;
        start_element.SetString(scan_start_element, true);
        SetIter(ctx, meta, start_element, iter, false);
        std::string tmpelement;
        while (iter.Valid())
        {
            Data* field = iter.Element();
            tmpelement.clear();
            field->GetDecodeString(tmpelement);
            if (r2.MemberSize() >= limit)
            {
                break;
            }

            if ((pattern.empty()
                    || stringmatchlen(pattern.c_str(), pattern.size(), tmpelement.c_str(), tmpelement.size(), 0) == 1))
            {
                RedisReply& rr = r2.AddMember();
                fill_str_reply(rr, tmpelement);
            }
            iter.Next();
        }
        if (iter.Valid())
        {
            if (m_cfg.scan_redis_compatible)
            {
                fill_str_reply(r1, stringfromll(GetNewRedisCursor(tmpelement)));
            }
            else
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

    int Ardb::SClear(Context& ctx, ValueObject& meta)
    {
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPSET);
        if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPSET)
        {
            SetIterator iter;
            SetIter(ctx, meta, meta.meta.min_index, iter, false);
            while (iter.Valid())
            {
                DelRaw(ctx, iter.CurrentRawKey());
                iter.Next();
            }
        }
        DelKeyValue(ctx, meta.key);
        return 0;
    }

    int Ardb::RenameSet(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey)
    {
        Context tmpctx;
        tmpctx.currentDB = srcdb;
        ValueObject v;
        int err = GetMetaValue(tmpctx, srckey, SET_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_error_reply(ctx.reply, "no such key or some error");
            return 0;
        }
        if (v.meta.Encoding() == COLLECTION_ECODING_ZIPSET)
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
            SetIterator iter;
            SetIter(ctx, v, v.meta.min_index, iter, false);
            tmpctx.currentDB = dstdb;
            ValueObject dstmeta;
            dstmeta.key.type = KEY_META;
            dstmeta.key.key = dstkey;
            dstmeta.type = SET_META;
            dstmeta.meta.SetEncoding(COLLECTION_ECODING_ZIPSET);
            BatchWriteGuard guard(GetKeyValueEngine());
            while (iter.Valid())
            {
                bool tmp;
                std::string tmpstr;
                SetAdd(tmpctx, dstmeta, iter.Element()->GetDecodeString(tmpstr), tmp);
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


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
#include <float.h>

OP_NAMESPACE_BEGIN

    int Ardb::SAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        std::set<std::string> added;
        bool redis_compatible = ctx.flags.redis_compatible;
        if (redis_compatible)
        {
            if (!CheckMeta(ctx, keystr, KEY_SET, meta))
            {
                return 0;
            }
        }
        else
        {
            meta.SetType(KEY_SET);
            meta.SetObjectLen(-1);
        }
        {
            bool meta_changed = false;
            WriteBatchGuard batch(ctx, m_engine);
            ValueObject empty;
            empty.SetType(KEY_SET_MEMBER);
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                KeyObject field(ctx.ns, KEY_SET_MEMBER, keystr);
                const std::string& data = cmd.GetArguments()[i];
                field.SetSetMember(data);
                if (redis_compatible)
                {
                    if ((0 == meta.GetType() || !m_engine->Exists(ctx, field)) && added.count(data) == 0)
                    {
                        SetKeyValue(ctx, field, empty);
                        if (meta.SetMinMaxData(field.GetSetMember()))
                        {
                            meta_changed = true;
                        }
                        added.insert(data);
                    }
                }
                else
                {
                    meta_changed = true;
                    SetKeyValue(ctx, field, empty);
                }
            }
            if (redis_compatible)
            {
                if (0 == meta.GetType())
                {
                    meta.SetType(KEY_SET);
                    meta.SetObjectLen(added.size());
                    meta_changed = true;
                }
                else if (meta.GetObjectLen() >= 0)
                {
                    meta.SetObjectLen(meta.GetObjectLen() + added.size());
                    meta_changed = true;
                }
            }
            if (meta_changed)
            {
                SetKeyValue(ctx, key, meta);
            }
        }
        if (0 != ctx.transc_err)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            if (redis_compatible)
            {
                reply.SetInteger(added.size());
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
        }
        return 0;
    }

    int Ardb::SCard(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_SET, cmd.GetArguments()[0]);
    }

    int Ardb::SIsMember(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject member(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
        member.SetSetMember(cmd.GetArguments()[1]);
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(m_engine->Exists(ctx, member) ? 1 : 0);
        return 0;
    }

    int Ardb::SMembers(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.ReserveMember(0);
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(ctx, key);
        Iterator* iter = m_engine->Find(ctx, key);
        bool checked_meta = false;
        bool need_set_minmax = false;
        ValueObject new_meta;
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (!checked_meta)
            {
                if (field.GetType() == KEY_META && field.GetKey() == key.GetKey())
                {
                    ValueObject& meta = iter->Value();
                    if (meta.GetType() != KEY_SET)
                    {
                        reply.SetErrCode(ERR_WRONG_TYPE);
                        break;
                    }
                    checked_meta = true;
                    if (meta.GetMin().IsNil() && meta.GetMax().IsNil())
                    {
                        need_set_minmax = true;
                        new_meta = meta;
                    }
                    iter->Next();
                    continue;
                }
                else
                {
                    break;
                }
            }
            if (field.GetType() != KEY_SET_MEMBER || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
            {
                break;
            }
            RedisReply& r = reply.AddMember();
            r.SetString(field.GetSetMember());
            iter->Next();
        }
        DELETE(iter);
        if (need_set_minmax && reply.MemberSize() > 0)
        {
            new_meta.SetObjectLen(reply.MemberSize());
            new_meta.GetMin().SetString(reply.MemberAt(0).str, true);
            new_meta.GetMax().SetString(reply.MemberAt(reply.MemberSize() - 1).str, true);
            SetKeyValue(ctx, key, new_meta);
        }
        return 0;
    }

    int Ardb::SMove(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObjectArray ks;
        for (uint32 i = 0; i < 2; i++)
        {
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            ks.push_back(k);
        }
        for (uint32 i = 0; i < 2; i++)
        {
            KeyObject k(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[i]);
            k.SetSetMember(cmd.GetArguments()[2]);
            ks.push_back(k);
        }
        KeysLockGuard guard(ctx, ks[0], ks[1]);
        ValueObjectArray vs;
        ErrCodeArray errs;
        int err = m_engine->MultiGet(ctx, ks, vs, errs);
        if (0 != err)
        {
            reply.SetErrCode(err);
            return 0;
        }
        if (!CheckMeta(ctx, ks[0], KEY_SET, vs[0], false) || !CheckMeta(ctx, ks[1], KEY_SET, vs[1], false))
        {
            return 0;
        }
        reply.SetInteger(0); //default response
        if (vs[0].GetType() == 0)
        {
            return 0;
        }
        if (vs[2].GetType() == KEY_SET_MEMBER)
        {
            WriteBatchGuard batch(ctx, m_engine);
            RemoveKey(ctx, ks[3]);
            if (vs[0].GetObjectLen() > 0)
            {
                vs[0].SetObjectLen(vs[0].GetObjectLen() - 1);
                if (vs[0].GetObjectLen() == 0)
                {
                    RemoveKey(ctx, ks[0]);
                }
                else
                {
                    SetKeyValue(ctx, ks[0], vs[0]);
                }
            }
            bool dest_meta_updated = false;
            if (vs[3].GetType() == 0) //not exist in dest set
            {
                vs[3].SetType(KEY_SET_MEMBER);
                SetKeyValue(ctx, ks[3], vs[3]);
                if (vs[1].GetType() == 0)
                {
                    vs[1].SetType(KEY_SET);
                    vs[1].SetObjectLen(1);
                    vs[1].SetMinMaxData(ks[3].GetSetMember());
                    dest_meta_updated = true;
                }
                else
                {
                    if (vs[1].SetMinMaxData(ks[3].GetSetMember()))
                    {
                        dest_meta_updated = true;
                    }
                    if (vs[1].GetObjectLen() > 0)
                    {
                        vs[1].SetObjectLen(vs[1].GetObjectLen() + 1);
                        dest_meta_updated = true;
                    }
                }
                if (dest_meta_updated)
                {
                    SetKeyValue(ctx, ks[1], vs[1]);
                }
            }
            reply.SetInteger(1);
        }
        if (0 != ctx.transc_err)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        return 0;
    }

    int Ardb::SPop(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool with_count = cmd.GetArguments().size() > 1;
        int64 count = 1;
        int64 removed = 0;
        if (with_count)
        {
            if (!string_toint64(cmd.GetArguments()[1], count))
            {
                reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
            count = std::abs(count);
            reply.ReserveMember(0);
        }
        ValueObject meta;
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(ctx, meta_key);
        if (!CheckMeta(ctx, keystr, KEY_SET, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            return 0;
        }
        bool remove_key = false;
        KeyObject key(ctx.ns, KEY_SET_MEMBER, keystr);
        key.SetSetMember(meta.GetMin());
        Iterator* iter = m_engine->Find(ctx, key);
        //bool ele_removed = false;
        while (iter->Valid())
        {
            KeyObject& field = iter->Key(true);
            if (field.GetType() == KEY_SET_MEMBER && field.GetNameSpace() == key.GetNameSpace() && field.GetKey() == key.GetKey())
            {
                if (removed >= count)
                {
                    //set min data
                    meta.SetMinData(field.GetSetMember());
                    break;
                }
                if(with_count)
                {
                    RedisReply& rr = reply.AddMember();
                    rr.SetString(field.GetSetMember());
                }
                else
                {
                    reply.SetString(field.GetSetMember());
                }
                //RemoveKey(ctx, field);
                IteratorDel(ctx, meta_key, iter);
                removed++;
                if (meta.GetObjectLen() > 0)
                {
                    meta.SetObjectLen(meta.GetObjectLen() - 1);
                    if (meta.GetObjectLen() == 0)
                    {
                        remove_key = true;
                    }
                }
                iter->Next();
            }
            else
            {
                remove_key = true;
                break;
            }
        }
        DELETE(iter);
        if (remove_key)
        {
            RemoveKey(ctx, meta_key);
        }
        else
        {
            SetKeyValue(ctx, meta_key, meta);
        }
        return 0;
    }

    int Ardb::SRandMember(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool with_count = cmd.GetArguments().size() > 1;
        int64 count = 1;
        int64 fetched = 0;
        if (with_count)
        {
            if (!string_toint64(cmd.GetArguments()[1], count))
            {
                reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
            reply.ReserveMember(0);
            if(count == 0)
            {
                return 0;
            }
        }
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_SET, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            return 0;
        }

        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_SET_MEMBER, keystr);
        Iterator* iter = m_engine->Find(ctx, key);
        while (NULL != iter && iter->Valid() && fetched < std::abs(count))
        {
            KeyObject& field = iter->Key();
            if (field.GetType() == KEY_SET_MEMBER && field.GetNameSpace() == key.GetNameSpace() && key.GetKey() == field.GetKey())
            {
                if (with_count)
                {
                    RedisReply& r = reply.AddMember();
                    r.SetString(field.GetSetMember());
                    fetched++;
                    iter->Next();
                }
                else
                {
                    reply.SetString(field.GetSetMember());
                    break;
                }
            }
            else
            {
                break;
            }
        }
        DELETE(iter);
        if(count < 0 && fetched < std::abs(count) && fetched > 0)
        {
            while(fetched < std::abs(count))
            {
                RedisReply& r = reply.MemberAt(random_between_int32(0, reply.MemberSize() -1));
                RedisReply& rr = reply.AddMember();
                rr.SetString(r.GetString());
                fetched++;
            }
        }
        return 0;
    }

    int Ardb::SRem(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(ctx, key);
        if (!ctx.flags.redis_compatible)
        {
            {
                WriteBatchGuard batch(ctx, m_engine);
                for (size_t i = 1; i < cmd.GetArguments().size(); i++)
                {
                    KeyObject field(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                    const std::string& data = cmd.GetArguments()[i];
                    field.SetSetMember(data);
                    RemoveKey(ctx, field);
                }
                meta.SetType(KEY_SET);
                meta.SetObjectLen(-1);
                SetKeyValue(ctx, key, meta);
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
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_SET, meta))
        {
            return 0;
        }
        reply.SetInteger(0); //default response
        if (meta.GetType() == 0)
        {
            return 0;
        }
        int64_t remove_count = 0;
        {
            WriteBatchGuard batch(ctx, m_engine);
            bool meta_changed = false;
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                KeyObject member(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                member.SetSetMember(cmd.GetArguments()[i]);
                if (m_engine->Exists(ctx, member))
                {
                    RemoveKey(ctx, member);
                    if (meta.GetObjectLen() > 0)
                    {
                        meta.SetObjectLen(meta.GetObjectLen() - 1);
                        meta_changed = true;
                    }
                    remove_count++;
                }
            }
            if (meta_changed)
            {
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
        if (ctx.transc_err != 0)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetInteger(remove_count);
        }
        return 0;
    }

    int Ardb::SDiff(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        PointerArray<Iterator*> iters;
        ValueObjectArray metas;
        size_t diff_key_cursor = 0;
        KeyObjectArray keys;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject set_key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            keys.push_back(set_key);
        }
        metas.resize(keys.size());
        iters.resize(keys.size());
        KeysLockGuard guard(ctx, keys);
        if (cmd.GetType() == REDIS_CMD_SDIFFSTORE)
        {
            diff_key_cursor = 1;
            if (!CheckMeta(ctx, keys[0], KEY_SET, metas[0]))
            {
                return 0;
            }
        }
        for (size_t i = diff_key_cursor; i < keys.size(); i++)
        {
            if (0 != GetMinMax(ctx, keys[i], KEY_SET, metas[i], iters[i]))
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
        }
        if (metas[diff_key_cursor].GetType() == 0)
        {
            if (cmd.GetType() == REDIS_CMD_SDIFF)
            {
                reply.ReserveMember(0);
            }
            else
            {
                if (cmd.GetType() == REDIS_CMD_SDIFFSTORE)
                {
                    if (metas[0].GetType() > 0)
                    {
                        DelKey(ctx, keys[0]);
                    }
                }
                reply.SetInteger(0); //REDIS_CMD_SDIFFCOUNT & REDIS_CMD_SDIFFSTORE
            }
            return 0;
        }
        DataSet diff_result;
        while (iters[diff_key_cursor]->Valid())
        {
            KeyObject& k = iters[diff_key_cursor]->Key(true);
            if (k.GetType() != KEY_SET_MEMBER || k.GetKey() != keys[diff_key_cursor].GetKey() || k.GetNameSpace() != keys[diff_key_cursor].GetNameSpace())
            {
                break;
            }
            diff_result.insert(k.GetSetMember());
            iters[diff_key_cursor]->Next();
        }

        for (size_t i = diff_key_cursor + 1; i < metas.size(); i++)
        {

            if (metas[i].GetType() == 0)
            {
                continue;
            }
            if (metas[i].GetMin() > metas[diff_key_cursor].GetMax() || metas[i].GetMax() < metas[diff_key_cursor].GetMin())
            {
                continue;
            }
            Data min = metas[diff_key_cursor].GetMin() > metas[i].GetMin() ? metas[diff_key_cursor].GetMin() : metas[i].GetMin();
            Data max = metas[diff_key_cursor].GetMax() < metas[i].GetMax() ? metas[diff_key_cursor].GetMax() : metas[i].GetMax();
            KeyObject start(ctx.ns, KEY_SET_MEMBER, keys[i].GetKey());
            start.SetSetMember(min);
            iters[i]->Jump(start);

            while (iters[i]->Valid())
            {
                KeyObject& k = iters[i]->Key(false);
                if (k.GetType() != KEY_SET_MEMBER || k.GetKey() != keys[i].GetKey() || k.GetNameSpace() != keys[i].GetNameSpace() || k.GetSetMember() > max)
                {
                    break;
                }
                diff_result.erase(k.GetSetMember());
                iters[i]->Next();
            }
        }
        if (cmd.GetType() == REDIS_CMD_SDIFFSTORE)
        {
            if (metas[0].GetType() > 0)
            {
                Iterator* iter = NULL;
                DelKey(ctx, keys[0], iter);
                DELETE(iter);
            }
            if (!diff_result.empty())
            {
                DataSet::iterator it = diff_result.begin();
                ValueObject dest_meta;
                dest_meta.SetType(KEY_SET);
                dest_meta.SetObjectLen(diff_result.size());
                while (it != diff_result.end())
                {
                    KeyObject element(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                    element.SetSetMember(*it);
                    ValueObject empty;
                    empty.SetType(KEY_SET_MEMBER);
                    SetKeyValue(ctx, element, empty);
                    it++;
                }
                dest_meta.SetMinData(*(diff_result.begin()));
                dest_meta.SetMaxData(*(diff_result.rbegin()));
                SetKeyValue(ctx, keys[0], dest_meta);
            }
            reply.SetInteger(diff_result.size());
        }
        else if (cmd.GetType() == REDIS_CMD_SDIFF)
        {
            DataSet::iterator it = diff_result.begin();
            while (it != diff_result.end())
            {
                RedisReply& r = reply.AddMember();
                r.SetString(*it);
                it++;
            }
        }
        else
        {
            reply.SetInteger(diff_result.size()); //REDIS_CMD_SDIFFCOUNT
        }
        return 0;
    }

    int Ardb::SDiffStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return SDiff(ctx, cmd);
    }
    int Ardb::SDiffCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return SDiff(ctx, cmd);
    }

    int Ardb::SInter(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        PointerArray<Iterator*> iters;
        ValueObjectArray metas;
        size_t inter_key_cursor = 0;
        KeyObjectArray keys;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject set_key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            keys.push_back(set_key);
        }
        metas.resize(keys.size());
        iters.resize(keys.size());
        KeysLockGuard guard(ctx, keys);
        if (cmd.GetType() == REDIS_CMD_SINTERSTORE)
        {
            inter_key_cursor = 1;
            if (!CheckMeta(ctx, keys[0], KEY_SET, metas[0]))
            {
                return 0;
            }
        }
        bool empty_result = false;
        Data min, max;
        for (size_t i = inter_key_cursor; i < keys.size(); i++)
        {
            if (0 != GetMinMax(ctx, keys[i], KEY_SET, metas[i], iters[i]))
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
            if (metas[i].GetType() == 0)
            {
                empty_result = true;
                break;
            }
            if (min.IsNil() || metas[i].GetMin() > min)
            {
                min = metas[i].GetMin();
            }
            if (max.IsNil() || metas[i].GetMax() < max)
            {
                max = metas[i].GetMax();
            }
        }
        if (min > max)
        {
            empty_result = true;
        }

        if (empty_result)
        {
            if (cmd.GetType() == REDIS_CMD_SINTER)
            {
                reply.ReserveMember(0);
            }
            else // REDIS_CMD_SINTERSTORE | REDIS_CMD_SINTERCOUNT
            {
                if (cmd.GetType() == REDIS_CMD_SINTERSTORE)
                {
                    if (metas[0].GetType() > 0)
                    {
                        Iterator* iter = NULL;
                        DelKey(ctx, keys[0], iter);
                        DELETE(iter);
                    }
                }
                reply.SetInteger(0);
            }
            return 0;
        }
        DataSet inter_result[2];
        size_t inter_result_cursor = 1;
        for (size_t i = inter_key_cursor; i < metas.size(); i++)
        {
            KeyObject start(ctx.ns, KEY_SET_MEMBER, keys[i].GetKey());
            start.SetSetMember(min);
            iters[i]->Jump(start);
            inter_result_cursor = 1 - inter_result_cursor;
            inter_result[inter_result_cursor].clear();
            while (iters[i]->Valid())
            {
                KeyObject& k = iters[i]->Key(true);
                if (k.GetType() != KEY_SET_MEMBER || k.GetKey() != keys[i].GetKey() || k.GetNameSpace() != keys[i].GetNameSpace() || k.GetSetMember() > max)
                {
                    break;
                }
                if (i == inter_key_cursor || inter_result[1 - inter_result_cursor].erase(k.GetSetMember()) > 0)
                {
                    inter_result[inter_result_cursor].insert(k.GetSetMember());
                }
                iters[i]->Next();
            }
        }
        if (cmd.GetType() == REDIS_CMD_SINTERSTORE)
        {
            if (!inter_result[inter_result_cursor].empty())
            {
                if (metas[0].GetType() > 0)
                {
                    Iterator* iter = NULL;
                    DelKey(ctx, keys[0], iter);
                    DELETE(iter);
                }
                DataSet::iterator it = inter_result[inter_result_cursor].begin();
                ValueObject dest_meta;
                dest_meta.SetType(KEY_SET);
                dest_meta.SetObjectLen(inter_result[inter_result_cursor].size());
                while (it != inter_result[inter_result_cursor].end())
                {
                    KeyObject element(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                    element.SetSetMember(*it);
                    ValueObject empty;
                    empty.SetType(KEY_SET_MEMBER);
                    SetKeyValue(ctx, element, empty);
                    it++;
                }
                dest_meta.SetMinData(*(inter_result[inter_result_cursor].begin()));
                dest_meta.SetMaxData(*(inter_result[inter_result_cursor].rbegin()));
                SetKeyValue(ctx, keys[0], dest_meta);
            }
            reply.SetInteger(inter_result[inter_result_cursor].size());
        }
        else if (cmd.GetType() == REDIS_CMD_SINTER)
        {
            DataSet::iterator it = inter_result[inter_result_cursor].begin();
            while (it != inter_result[inter_result_cursor].end())
            {
                RedisReply& r = reply.AddMember();
                r.SetString(*it);
                it++;
            }
        }
        else
        {
            reply.SetInteger(inter_result[inter_result_cursor].size()); //REDIS_CMD_SINTERCOUNT
        }
        return 0;
    }

    int Ardb::SInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return SInter(ctx, cmd);
    }

    int Ardb::SInterCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return SInter(ctx, cmd);
    }

    int Ardb::SUnion(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObjectArray metas;
        size_t union_key_cursor = 0;
        KeyObjectArray keys;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject set_key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            keys.push_back(set_key);
        }
        metas.resize(keys.size());
        KeysLockGuard guard(ctx,keys);
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, keys, metas, errs);
        for (size_t i = 0; i < metas.size(); i++)
        {
            if (!CheckMeta(ctx, keys[i], KEY_SET, metas[i], false))
            {
                return 0;
            }
            if (errs[i] != 0 && errs[i] != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(errs[i]);
                return 0;
            }
        }
        if (cmd.GetType() == REDIS_CMD_SUNIONSTORE)
        {
            union_key_cursor = 1;
        }
        DataSet union_result;
        Iterator* iter = NULL;
        ctx.flags.iterate_no_upperbound = 1;
        for (size_t i = union_key_cursor; i < keys.size(); i++)
        {
            if (metas[i].GetType() == 0)
            {
                continue;
            }
            KeyObject ele(ctx.ns, KEY_SET_MEMBER, keys[i].GetKey());
            ele.SetSetMember(metas[i].GetMin());
            if (NULL != iter)
            {
                iter->Jump(ele);
            }
            else
            {
                iter = m_engine->Find(ctx, ele);
            }
            while (NULL != iter && iter->Valid())
            {
                KeyObject& k = iter->Key(true);
                if (k.GetType() != KEY_SET_MEMBER || k.GetKey() != keys[i].GetKey() || k.GetNameSpace() != keys[i].GetNameSpace())
                {
                    break;
                }
                union_result.insert(k.GetSetMember());
                iter->Next();
            }
        }
        DELETE(iter);
        if (cmd.GetType() == REDIS_CMD_SUNIONSTORE)
        {
            if (metas[0].GetType() > 0)
            {
                Iterator* iter = NULL;
                DelKey(ctx, keys[0], iter);
                DELETE(iter);
            }

            if (!union_result.empty())
            {
                DataSet::iterator it = union_result.begin();
                ValueObject dest_meta;
                dest_meta.SetType(KEY_SET);
                dest_meta.SetObjectLen(union_result.size());
                while (it != union_result.end())
                {
                    KeyObject element(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                    element.SetSetMember(*it);
                    ValueObject empty;
                    empty.SetType(KEY_SET_MEMBER);
                    SetKeyValue(ctx, element, empty);
                    it++;
                }
                dest_meta.SetMinData(*(union_result.begin()));
                dest_meta.SetMaxData(*(union_result.rbegin()));
                SetKeyValue(ctx, keys[0], dest_meta);
                reply.SetInteger(union_result.size());
            }

        }
        else if (cmd.GetType() == REDIS_CMD_SUNION)
        {
            DataSet::iterator it = union_result.begin();
            while (it != union_result.end())
            {
                RedisReply& r = reply.AddMember();
                r.SetString(*it);
                it++;
            }
        }
        else
        {
            reply.SetInteger(union_result.size()); //REDIS_CMD_SUNIONCOUNT
        }
        return 0;
    }

    int Ardb::SUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return SUnion(ctx, cmd);
    }

    int Ardb::SUnionCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return SUnion(ctx, cmd);
    }

    int Ardb::SScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }

OP_NAMESPACE_END


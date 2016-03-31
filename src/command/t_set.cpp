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
        KeyLockGuard guard(key);
        ValueObject meta;
        int err = 0;
        std::set<std::string> added;
        bool redis_compatible = cmd.GetType() < REDIS_CMD_MERGE_BEGIN && GetConf().redis_compatible;
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
            TransactionGuard batch(ctx, m_engine);
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
                        m_engine->Put(ctx, field, empty);
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
                    m_engine->Put(ctx, field, empty);
                    added.insert(data);
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
                m_engine->Put(ctx, key, meta);
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
        KeyLockGuard guard(key);
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
                        reply.SetErrCode(ERR_INVALID_TYPE);
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
        if (need_set_minmax)
        {
            new_meta.SetObjectLen(reply.MemberSize());
            new_meta.GetMin().SetString(reply.MemberAt(0).str, true);
            new_meta.GetMax().SetString(reply.MemberAt(reply.MemberSize() - 1).str, true);
            m_engine->Put(ctx, key, new_meta);
        }
        return 0;
    }

    int Ardb::SMove(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_SET, meta))
        {
            return 0;
        }
        reply.SetInteger(0); //default response
        if (meta.GetType() == 0)
        {
            return 0;
        }
        KeyObject member(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
        member.SetSetMember(cmd.GetArguments()[2]);
        if (m_engine->Exists(ctx, member))
        {
            m_engine->Del(ctx, member);
            KeyObject move(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[1]);
        }
        return 0;
    }

    int Ardb::SPop(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, keystr);
        KeyLockGuard guard(meta_key);
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
        bool ele_removed = false;
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() == KEY_SET_MEMBER && field.GetNameSpace() == key.GetNameSpace() && field.GetKey() == key.GetKey())
            {
                if (ele_removed)
                {
                    //set min data
                    meta.SetMinData(field.GetSetMember());
                    break;
                }
                reply.SetString(field.GetSetMember());
                m_engine->Del(ctx, field);
                ele_removed = true;
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
            m_engine->Del(ctx, key);
        }
        else
        {
            m_engine->Put(ctx, key, meta);
        }
        return 0;
    }

    int Ardb::SRandMember(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_SET, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            return 0;
        }
        bool with_count = cmd.GetArguments().size() >= 1;
        int64 count = 1;
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
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_SET_MEMBER, keystr);
        Iterator* iter = m_engine->Find(ctx, key);
        while (NULL != iter && iter->Valid() && count > 0)
        {
            KeyObject& field = iter->Key();
            if (field.GetType() == KEY_SET_MEMBER && field.GetNameSpace() == key.GetNameSpace() && field.GetKey() == field.GetKey())
            {
                if (with_count)
                {
                    RedisReply& r = reply.AddMember();
                    r.SetString(field.GetSetMember());
                    count--;
                    iter->Next();
                }
                else
                {
                    reply.SetString(field.GetSetMember());
                    break;
                }
            }
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::SRem(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            DataArray ms(cmd.GetArguments().size() - 1);
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                ms[i - 1].SetString(cmd.GetArguments()[i], true);
            }
            m_engine->Merge(ctx, key, cmd.GetType(), ms);
            reply.SetStatusCode(STATUS_OK);
            return 0;
        }
        KeyLockGuard guard(key);
        ValueObject meta;
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
            TransactionGuard batch(ctx, m_engine);
            for (size_t i = 0; i < cmd.GetArguments().size(); i++)
            {
                KeyObject member(ctx.ns, KEY_SET_MEMBER, cmd.GetArguments()[0]);
                member.SetSetMember(cmd.GetArguments()[i + 1]);
                if (m_engine->Exists(ctx, member))
                {
                    m_engine->Del(ctx, member);
                    remove_count++;
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
        return 0;
    }

    int Ardb::SDiffStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::SDiffCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SInter(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SInterCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SUnion(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SUnionCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::SScan(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

OP_NAMESPACE_END


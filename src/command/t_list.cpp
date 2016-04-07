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
#include <cmath>

OP_NAMESPACE_BEGIN

    int Ardb::LIndex(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 index;
        if (!string_toint64(cmd.GetArguments()[1], index))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject v;
        int err;
        if (!CheckMeta(ctx, k, KEY_LIST, v))
        {
            return 0;
        }
        if (v.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        if (index < 0)
        {
            index = v.GetObjectLen() + index;
        }
        if (index < 0 || index >= v.GetObjectLen())
        {
            reply.Clear();
            return 0;
        }
        if (v.GetListMeta().sequential)
        {
            KeyObject ele(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
            ele.SetListIndex(v.GetListMinIdx() + index);
            ValueObject ele_value;
            err = m_engine->Get(ctx, ele, ele_value);
            if (0 == err)
            {
                reply.SetString(ele_value.GetListElement());
            }
            else
            {
                reply.SetErrCode(err);
            }
        }
        else
        {
            KeyObject key(ctx.ns, KEY_LIST_ELEMENT, k.GetKey());
            key.SetListIndex(v.GetMin());
            Iterator* iter = m_engine->Find(ctx, key);
            int64 cursor = 0;
            while (NULL != iter && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
                {
                    break;
                }
                if (cursor == index)
                {
                    reply.SetString(iter->Value().GetListElement());
                    break;
                }
                cursor++;
                iter->Next();
            }
            DELETE(iter);
            if (cursor != index)
            {
                reply.Clear();
            }
        }
        return 0;
    }

    int Ardb::LLen(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_LIST, cmd.GetArguments()[0]);
    }

    int Ardb::ListPop(Context& ctx, RedisCommandFrame& cmd, bool lock_key)
    {
        RedisReply& reply = ctx.GetReply();
        bool is_lpop = (cmd.GetType() == REDIS_CMD_LPOP || cmd.GetType() == REDIS_CMD_BLPOP);
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        KeyLockGuard guard(key, lock_key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0 || meta.GetObjectLen() == 0)
        {
            reply.Clear();
            return 0;
        }
        int err = 0;
        {
            TransactionGuard batch(ctx, m_engine);
            if (meta.GetListMeta().sequential)
            {
                KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, keystr);
                ValueObject ele_value;
                ele_key.SetListIndex(is_lpop ? meta.GetListMinIdx() : meta.GetListMaxIdx());
                err = m_engine->Get(ctx, ele_key, ele_value);
                if (0 != err)
                {
                    reply.SetErrCode(err);
                    return 0;
                }
                reply.SetString(ele_value.GetListElement());
                m_engine->Del(ctx, ele_key);
                if (is_lpop)
                {
                    meta.SetListMinIdx(meta.GetListMinIdx() + 1);
                }
                else
                {
                    meta.SetListMaxIdx(meta.GetListMaxIdx() - 1);
                }
            }
            else
            {
                KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, keystr);
                ele_key.SetListIndex(is_lpop ? meta.GetMin() : meta.GetMax());
                Iterator* iter = m_engine->Find(ctx, ele_key);
                if (!is_lpop)
                {
                    if (!iter->Valid())
                    {
                        iter->JumpToLast();
                    }
                }
                if (iter->Valid())
                {
                    KeyObject& field = iter->Key();
                    if (field.GetType() == KEY_LIST_ELEMENT && field.GetNameSpace() == ele_key.GetNameSpace() && field.GetKey() != ele_key.GetKey())
                    {
                        reply.SetString(iter->Value().GetListElement());
                        m_engine->Del(ctx, field);
                        if (meta.GetObjectLen() > 1)
                        {
                            if (is_lpop)
                            {
                                iter->Next();
                            }
                            else
                            {
                                iter->Prev();
                            }
                            if (iter->Valid())
                            {
                                KeyObject& minmax = iter->Key();
                                if (minmax.GetType() == KEY_LIST_ELEMENT && minmax.GetNameSpace() == ele_key.GetNameSpace()
                                        && minmax.GetKey() != ele_key.GetKey())
                                {
                                    if (is_lpop)
                                    {
                                        meta.SetMinData(minmax.GetElement(0), true);
                                    }
                                    else
                                    {
                                        meta.SetMaxData(minmax.GetElement(0), true);
                                    }
                                }
                            }
                        }
                    }
                }
                DELETE(iter);
            }
            meta.SetObjectLen(meta.GetObjectLen() - 1);
            if (meta.GetObjectLen() == 0)
            {
                m_engine->Del(ctx, key);
            }
            else
            {
                m_engine->Put(ctx, key, meta);
            }
        }
        err = ctx.transc_err;
        if (0 != err)
        {
            reply.SetErrCode(err);
            return 0;
        }
        return 0;
    }

    int Ardb::LPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPop(ctx, cmd);
    }

    int Ardb::LInsert(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool head = false;
        if (!strcasecmp(cmd.GetArguments()[1].c_str(), "before"))
        {
            head = true;
        }
        else if (!strcasecmp(cmd.GetArguments()[1].c_str(), "after"))
        {
            head = false;
        }
        else
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        KeyLockGuard guard(key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            reply.SetInteger(0);
            return 0;
        }
        reply.SetInteger(-1); //default response
        Data match;
        match.SetString(cmd.GetArguments()[2], true);
        KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        elekey.SetListIndex(meta.GetMin());
        Iterator* iter = m_engine->Find(ctx, elekey);
        bool found_match = false;
        bool insert_ele_idx_computed = false;
        double insert_ele_idx = 0;
        KeyObject insert(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
            {
                break;
            }
            if (found_match)
            {
                //compute insert_ele_idx
                insert_ele_idx = (field.GetListIndex() + insert_ele_idx) / 2;
                insert_ele_idx_computed = true;
                break;
            }
            if (0 == iter->Value().GetListElement().Compare(match))
            {
                found_match = true;
                insert_ele_idx = field.GetListIndex(); //base index
                if (head)
                {
                    iter->Prev();
                }
                else
                {
                    iter->Next();
                }
            }
            else
            {
                iter->Next();
            }
        }
        DELETE(iter);
        if (found_match)
        {
            if (!insert_ele_idx_computed)
            {
                if (head)
                {
                    insert_ele_idx -= 1;
                }
                else
                {
                    insert_ele_idx += 1;
                }
            }
            TransactionGuard batch(ctx, m_engine);
            ValueObject insert_val;
            insert_val.SetType(KEY_LIST_ELEMENT);
            insert_val.SetListElement(cmd.GetArguments()[3]);
            insert.SetListIndex(insert_ele_idx);
            {
                TransactionGuard batch(ctx, m_engine);
                m_engine->Put(ctx, insert, insert_val);
                meta.SetObjectLen(meta.GetObjectLen() + 1);
                meta.GetListMeta().sequential = false;
                meta.SetMinMaxData(insert_ele_idx);
                m_engine->Put(ctx, key, meta);
            }
            if (0 != ctx.transc_err)
            {
                reply.SetErrCode(ctx.transc_err);
            }
            else
            {
                reply.SetInteger(meta.GetObjectLen());
            }
        }
        return 0;
    }

    int Ardb::ListPush(Context& ctx, RedisCommandFrame& cmd, bool lock_key)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        int err = 0;
        KeyLockGuard guard(key, lock_key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta))
        {
            return 0;
        }
        bool left_push = cmd.GetType() == REDIS_CMD_LPUSH || cmd.GetType() == REDIS_CMD_LPUSHX;
        bool xx = cmd.GetType() == REDIS_CMD_RPUSHX || cmd.GetType() == REDIS_CMD_LPUSHX;
        if (xx && meta.GetType() == 0)
        {
            reply.SetInteger(0);
            return 0;
        }
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_LIST);
            meta.SetObjectLen(0);
            meta.SetListMaxIdx(0);
            meta.SetListMinIdx(0);
            meta.GetListMeta().sequential = true;
        }
        {
            TransactionGuard batch(ctx, m_engine);
            for (size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                KeyObject ele(ctx.ns, KEY_LIST_ELEMENT, keystr);
                ValueObject ele_value;
                ele_value.SetType(KEY_LIST_ELEMENT);
                ele_value.SetListElement(cmd.GetArguments()[i]);
                int64 idx = 0;
                if (left_push)
                {
                    idx = meta.GetListMinIdx() - 1;
                    meta.SetListMinIdx(idx);
                }
                else
                {
                    idx = meta.GetListMaxIdx() + 1;
                    meta.SetListMaxIdx(idx);
                }
                ele.SetListIndex(idx);
                m_engine->Put(ctx, ele, ele_value);
                meta.SetObjectLen(meta.GetObjectLen() + 1);
            }
            m_engine->Put(ctx, key, meta);
        }
        err = ctx.transc_err;
        if (err != 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(meta.GetObjectLen());
        }
        return 0;
    }

    int Ardb::LPush(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPush(ctx, cmd);
    }
    int Ardb::LPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPush(ctx, cmd);
    }

    int Ardb::LRange(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 start, end;
        if (!string_toint64(cmd.GetArguments()[1], start) || !string_toint64(cmd.GetArguments()[2], end))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        if (!CheckMeta(ctx, key, KEY_LIST, meta))
        {
            return 0;
        }
        reply.ReserveMember(0);
        if (meta.GetType() == 0)
        {
            return 0;
        }
        if (start < 0)
            start = meta.GetObjectLen() + start;
        if (end < 0)
            end = meta.GetObjectLen() + end;
        if (start < 0)
            start = 0;
        if (start > end || start >= meta.GetObjectLen())
        {
            return 0;
        }
        if (end >= meta.GetObjectLen())
            end = meta.GetObjectLen() - 1;
        int64_t rangelen = (end - start) + 1;
        reply.ReserveMember(0);
        if (meta.GetListMeta().sequential)
        {
            KeyObjectArray ks;
            for (size_t i = 0; i < rangelen; i++)
            {
                KeyObject key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                key.SetListIndex((int64)(meta.GetListMinIdx() + i + start));
                ks.push_back(key);
            }
            ValueObjectArray vs;
            ErrCodeArray errs;
            m_engine->MultiGet(ctx, ks, vs, errs);
            for (size_t i = 0; i < vs.size(); i++)
            {
                RedisReply& r = reply.AddMember();
                r.SetString(vs[i].GetListElement());
            }
        }
        else
        {
            KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
            ele_key.SetListIndex(meta.GetMin());
            Iterator* iter = m_engine->Find(ctx, ele_key);
            int64 cursor = 0;
            while (NULL != iter && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
                {
                    break;
                }
                if (cursor >= start)
                {
                    RedisReply& r = reply.AddMember();
                    r.SetString(iter->Value().GetListElement());
                }
                if (cursor == end)
                {
                    break;
                }
                cursor++;
                iter->Next();
            }
            DELETE(iter);
        }
        return 0;
    }
    int Ardb::LRem(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 count;
        if (!string_toint64(cmd.GetArguments()[1], count))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        if (count == 0)
        {
            reply.SetInteger(0);
            return 0;
        }
        int64 toremove = std::abs(count);
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            reply.SetInteger(0);
            return 0;
        }
        Iterator* iter = NULL;
        KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        if (count < 0)
        {
            ele_key.SetListIndex(meta.GetMax());
        }
        else
        {
            ele_key.SetListIndex(meta.GetMin());
        }
        iter = m_engine->Find(ctx, ele_key);
        if (count < 0)
        {
            if (!iter->Valid())
            {
                iter->JumpToLast();
            }
        }
        int64 removed = 0;
        Data rem_data;
        rem_data.SetString(cmd.GetArguments()[2], true);
        {
            TransactionGuard batch(ctx, m_engine);
            while (iter != NULL && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != ele_key.GetNameSpace() || field.GetKey() != ele_key.GetKey())
                {
                    break;
                }
                if (iter->Value().GetListElement() == rem_data)
                {
                    m_engine->Del(ctx, field);
                    removed++;
                }
                if (removed == std::abs(count))
                {
                    break;
                }
                if (count > 0)
                {
                    iter->Next();
                }
                else
                {
                    iter->Prev();
                }
            }
            DELETE(iter);
            meta.GetListMeta().sequential = false;
            meta.SetObjectLen(meta.GetObjectLen() - removed);
            if (meta.GetObjectLen() == 0)
            {
                m_engine->Del(ctx, key);
            }
            else
            {
                m_engine->Put(ctx, key, meta);
            }
        }
        if (ctx.transc_err != 0)
        {
            reply.SetErrCode(ctx.transc_err);
        }
        else
        {
            reply.SetInteger(removed);
        }
        return 0;
    }
    int Ardb::LSet(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 index;
        if (!string_toint64(cmd.GetArguments()[1], index))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject v;
        int err = 0;
        KeyLockGuard guard(k);
        if (!CheckMeta(ctx, k, KEY_LIST, v))
        {
            return 0;
        }
        if (v.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        if (index < 0)
        {
            index = v.GetObjectLen() + index;
        }
        if (index < 0 || index >= v.GetObjectLen())
        {
            reply.SetErrCode(ERR_OUTOFRANGE);
            return 0;
        }
        if (v.GetListMeta().sequential)
        {
            KeyObject ele(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
            ele.SetListIndex((int64_t) (v.GetListMinIdx() + index));
            ValueObject ele_value;
            ele_value.SetType(KEY_LIST_ELEMENT);
            ele_value.SetListElement(cmd.GetArguments()[2]);
            err = m_engine->Put(ctx, ele, ele_value);
            if (0 == err)
            {
                reply.SetStatusCode(STATUS_OK);
            }
            else
            {
                reply.SetErrCode(err);
            }
        }
        else
        {
            KeyObject key(ctx.ns, KEY_LIST_ELEMENT, v.GetMin());
            Iterator* iter = m_engine->Find(ctx, key);
            int64 cursor = 0;
            while (NULL != iter && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
                {
                    break;
                }
                if (cursor == index)
                {
                    ValueObject ele_value;
                    ele_value.SetType(KEY_LIST_ELEMENT);
                    ele_value.SetListElement(cmd.GetArguments()[2]);
                    err = m_engine->Put(ctx, field, ele_value);
                    break;
                }
                cursor++;
                iter->Next();
            }
            DELETE(iter);
            if (cursor != index)
            {
                reply.SetErrCode(ERR_OUTOFRANGE);
            }
            else
            {
                if (0 == err)
                {
                    reply.SetStatusCode(STATUS_OK);
                }
                else
                {
                    reply.SetErrCode(err);
                }
            }
        }
        return 0;
    }

    int Ardb::LTrim(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 start, end, ltrim, rtrim;
        if (!string_toint64(cmd.GetArguments()[1], start) || !string_toint64(cmd.GetArguments()[1], end))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        reply.SetStatusCode(STATUS_OK); //default response
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta) || meta.GetType() == 0)
        {
            return 0;
        }
        int64_t llen = meta.GetObjectLen();
        /* convert negative indexes */
        if (start < 0)
            start = llen + start;
        if (end < 0)
            end = llen + end;
        if (start < 0)
            start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen)
        {
            /* Out of range start or start > end result in empty list */
            ltrim = llen;
            rtrim = 0;
        }
        else
        {
            if (end >= llen)
                end = llen - 1;
            ltrim = start;
            rtrim = llen - end - 1;
        }
        if (meta.GetListMeta().sequential)
        {
            for (int64_t i = 0; i < ltrim; i++)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(i + meta.GetListMinIdx());
                m_engine->Del(ctx, elekey);
            }
            for (int64_t i = 0; i < rtrim; i++)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(i + meta.GetListMaxIdx() - i);
                m_engine->Del(ctx, elekey);
            }
            meta.SetObjectLen(meta.GetObjectLen() - ltrim);
            meta.SetObjectLen(meta.GetObjectLen() - rtrim);
            meta.SetListMinIdx(meta.GetListMinIdx() + ltrim);
            meta.SetListMaxIdx(meta.GetListMaxIdx() - rtrim);
        }
        else
        {
            Iterator* iter = NULL;
            bool trim_stop = false;
            if (ltrim > 0)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(meta.GetMin());
                iter = m_engine->Find(ctx, elekey);
                while (NULL != iter && iter->Valid())
                {
                    KeyObject& field = iter->Key();
                    if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != elekey.GetKey())
                    {
                        trim_stop = true;
                        break;
                    }
                    if (ltrim <= 0)
                    {
                        meta.SetMinData(field.GetElement(0), true);
                        break;
                    }
                    m_engine->Del(ctx, field);
                    meta.SetObjectLen(meta.GetObjectLen() - 1);
                    ltrim--;
                    iter->Next();
                }
            }
            if (rtrim > 0 && !trim_stop)
            {
                KeyObject tail(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                tail.SetListIndex(meta.GetMax());
                if (NULL != iter)
                {
                    iter->Jump(tail);
                }
                else
                {
                    iter = m_engine->Find(ctx, tail);
                }
                if (!iter->Valid())
                {
                    iter->JumpToLast();
                }
                while (iter->Valid())
                {
                    KeyObject& field = iter->Key();
                    if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
                    {
                        break;
                    }
                    if (rtrim <= 0)
                    {
                        meta.SetMaxData(field.GetElement(0), true);
                        break;
                    }
                    m_engine->Del(ctx, field);
                    meta.SetObjectLen(meta.GetObjectLen() - 1);
                    rtrim--;
                    iter->Prev();
                }
            }
            DELETE(iter);
        }
        if (0 == meta.GetListMeta().size)
        {
            m_engine->Del(ctx, key);
        }
        else
        {
            m_engine->Put(ctx, key, meta);
        }
        return 0;
    }

    int Ardb::RPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPop(ctx, cmd);
    }
    int Ardb::RPush(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPush(ctx, cmd);
    }
    int Ardb::RPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPush(ctx, cmd);
    }

    int Ardb::RPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject src(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyObject dest(ctx.ns, KEY_META, cmd.GetArguments()[1]);
        KeysLockGuard guard(src, dest);
        RedisCommandFrame rpop;
        rpop.SetCommand("rpop");
        rpop.SetType(REDIS_CMD_LPUSH);
        rpop.AddArg(cmd.GetArguments()[0]);
        ListPop(ctx, rpop, false);
        if (reply.type == REDIS_REPLY_STRING)
        {
            const std::string& data = reply.str;
            RedisCommandFrame lpush;
            lpush.SetCommand("lpush");
            lpush.SetType(REDIS_CMD_LPUSH);
            lpush.AddArg(cmd.GetArguments()[1]);
            lpush.AddArg(data);
            ListPush(ctx, lpush, false);
            if (reply.type != REDIS_REPLY_ERROR)
            {
                reply.SetString(data);
            }
        }
        return 0;
    }

    int Ardb::BLPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::BRPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::BRPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

OP_NAMESPACE_END


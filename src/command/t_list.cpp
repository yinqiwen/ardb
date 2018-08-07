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
        KeyLockGuard guard(ctx, k);
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
        if (v.GetMetaObject().list_sequential)
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
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                        || field.GetKey() != key.GetKey())
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
        KeyLockGuard guard(ctx, key, lock_key);
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
            WriteBatchGuard batch(ctx, m_engine);

            if (meta.GetMetaObject().list_sequential)
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
                RemoveKey(ctx, ele_key);
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
                if (!is_lpop)
                {
                    ctx.flags.iterate_total_order = 1;
                }
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
                    if (field.GetType() == KEY_LIST_ELEMENT && field.GetNameSpace() == ele_key.GetNameSpace()
                            && field.GetKey() == ele_key.GetKey())
                    {
                        reply.SetString(iter->Value().GetListElement());
                        //RemoveKey(ctx, field);
                        IteratorDel(ctx, key, iter);
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
                                if (minmax.GetType() == KEY_LIST_ELEMENT
                                        && minmax.GetNameSpace() == ele_key.GetNameSpace()
                                        && minmax.GetKey() == ele_key.GetKey())
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
                RemoveKey(ctx, key);
            }
            else
            {
                SetKeyValue(ctx, key, meta);
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
        KeyLockGuard guard(ctx, key);
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
            if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                    || field.GetKey() != key.GetKey())
            {
                break;
            }
            if (found_match)
            {
                //compute insert_ele_idx
                insert_ele_idx = (field.GetListIndex() + insert_ele_idx) / 2.0;
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
            WriteBatchGuard batch(ctx, m_engine);
            ValueObject insert_val;
            insert_val.SetType(KEY_LIST_ELEMENT);
            insert_val.SetListElement(cmd.GetArguments()[3]);
            insert.SetListIndex(insert_ele_idx);
            {
                WriteBatchGuard batch(ctx, m_engine);
                SetKeyValue(ctx, insert, insert_val);
                meta.SetObjectLen(meta.GetObjectLen() + 1);
                meta.GetMetaObject().list_sequential = false;
                meta.SetMinMaxData(insert_ele_idx);
                SetKeyValue(ctx, key, meta);
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
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        int err = 0;
        {
            KeyLockGuard guard(ctx, key, lock_key);
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
                meta.GetMetaObject().list_sequential = true;
            }
            {
                WriteBatchGuard batch(ctx, m_engine);
                for (size_t i = 1; i < cmd.GetArguments().size(); i++)
                {
                    KeyObject ele(ctx.ns, KEY_LIST_ELEMENT, keystr);
                    ValueObject ele_value;
                    ele_value.SetType(KEY_LIST_ELEMENT);
                    ele_value.SetListElement(cmd.GetArguments()[i]);
                    int64 idx = 0;
                    if (meta.GetObjectLen() > 0)
                    {
                        if (left_push)
                        {
                            if (meta.GetMin().IsInteger())
                            {
                                idx = meta.GetListMinIdx() - 1;
                            }
                            else
                            {
                                idx = (int64_t) (meta.GetMin().GetFloat64()) - 1;
                            }
                            meta.SetListMinIdx(idx);
                        }
                        else
                        {
                            if (meta.GetMax().IsInteger())
                            {
                                idx = meta.GetListMaxIdx() + 1;
                            }
                            else
                            {
                                idx = (int64_t) (meta.GetMax().GetFloat64()) + 1;
                            }
                            meta.SetListMaxIdx(idx);
                        }
                    }
                    ele.SetListIndex(idx);
                    SetKeyValue(ctx, ele, ele_value);
                    meta.SetObjectLen(meta.GetObjectLen() + 1);
                }
                //meta.SetTTL(0); //clear ttl setting
                SetKeyValue(ctx, key, meta);
            }
            err = ctx.transc_err;
        }
        if (err != 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(meta.GetObjectLen());
            SignalKeyAsReady(ctx, keystr);
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
        if (start < 0) start = meta.GetObjectLen() + start;
        if (end < 0) end = meta.GetObjectLen() + end;
        if (start < 0) start = 0;
        if (start > end || start >= meta.GetObjectLen())
        {
            return 0;
        }
        if (end >= meta.GetObjectLen()) end = meta.GetObjectLen() - 1;
        //int64_t rangelen = (end - start) + 1;
        reply.ReserveMember(0);

        KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        int64 cursor = 0;
        if (meta.GetMetaObject().list_sequential)
        {
            ele_key.SetListIndex(meta.GetListMinIdx() + start);
            cursor = start;
        }
        else
        {
            ele_key.SetListIndex(meta.GetMin());
            cursor = 0;
        }
        ctx.flags.iterate_no_upperbound = 1;
        Iterator* iter = m_engine->Find(ctx, ele_key);
        while (NULL != iter && iter->Valid())
        {
            KeyObject& field = iter->Key();
            if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                    || field.GetKey() != key.GetKey())
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
        //int64 toremove = std::abs(count);
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(ctx, key);
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
        // bookkeeping element key min/max index
        KeyObject min_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        min_key.SetListIndex(meta.GetMin());
        KeyObject max_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        max_key.SetListIndex(meta.GetMax());
        KeyObject ele_key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
        if (count < 0)
        {
            ele_key.SetListIndex(meta.GetMax());
        }
        else if (count > 0)
        {
            ele_key.SetListIndex(meta.GetMin());
        }
        else // "count == 0" means remove all elements equal to value
        {
            ele_key.SetListIndex(meta.GetMin());
            count = meta.GetObjectLen();
        }
        if (count < 0)
        {
            ctx.flags.iterate_total_order = 1;
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
            WriteBatchGuard batch(ctx, m_engine);
            while (iter != NULL && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || ele_key.ComparePrefix(field) != 0)
                {
                    break;
                }
                if (iter->Value().GetListElement() == rem_data)
                {
                    //RemoveKey(ctx, field);
                    IteratorDel(ctx, key, iter);
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
            if (removed)
            {
                // re-collect minmax
                Iterator* min_iter = m_engine->Find(ctx, min_key);
                if (min_iter->Valid() && min_iter->Key().GetType() == KEY_LIST_ELEMENT
                        && min_key.ComparePrefix(min_iter->Key()) == 0)
                {
                    Data min_data = min_iter->Key().GetElement(0);
                    meta.SetMinData(min_data);
                }
                DELETE(min_iter);

                Iterator* max_iter = m_engine->Find(ctx, max_key);
                if (!max_iter->Valid()) max_iter->JumpToLast();
                if (max_iter->Valid() && max_iter->Key().GetType() == KEY_LIST_ELEMENT
                        && max_key.ComparePrefix(max_iter->Key()) == 0)
                {
                    Data max_data = max_iter->Key().GetElement(0);
                    meta.SetMaxData(max_data);
                }
                DELETE(max_iter);
            }
            meta.GetMetaObject().list_sequential = false;
            meta.SetObjectLen(meta.GetObjectLen() - removed);
            if (meta.GetObjectLen() == 0)
            {
                RemoveKey(ctx, key);
            }
            else
            {
                SetKeyValue(ctx, key, meta);
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
        KeyLockGuard guard(ctx, k);
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
        if (v.GetMetaObject().list_sequential)
        {
            KeyObject ele(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
            ele.SetListIndex((int64_t) (v.GetListMinIdx() + index));
            ValueObject ele_value;
            ele_value.SetType(KEY_LIST_ELEMENT);
            ele_value.SetListElement(cmd.GetArguments()[2]);
            err = SetKeyValue(ctx, ele, ele_value);
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
            KeyObject key(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
            key.SetListIndex(v.GetMin());
            Iterator* iter = m_engine->Find(ctx, key);
            int64 cursor = 0;
            while (NULL != iter && iter->Valid())
            {
                KeyObject& field = iter->Key();
                if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                        || field.GetKey() != key.GetKey())
                {
                    break;
                }
                if (cursor == index)
                {
                    ValueObject ele_value;
                    ele_value.SetType(KEY_LIST_ELEMENT);
                    ele_value.SetListElement(cmd.GetArguments()[2]);
                    err = SetKeyValue(ctx, field, ele_value);
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
        if (!string_toint64(cmd.GetArguments()[1], start) || !string_toint64(cmd.GetArguments()[2], end))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        reply.SetStatusCode(STATUS_OK); //default response
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(ctx, key);
        if (!CheckMeta(ctx, key, KEY_LIST, meta) || meta.GetType() == 0)
        {
            return 0;
        }
        int64_t llen = meta.GetObjectLen();
        /* convert negative indexes */
        if (start < 0) start = llen + start;
        if (end < 0) end = llen + end;
        if (start < 0) start = 0;

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
            if (end >= llen) end = llen - 1;
            ltrim = start;
            rtrim = end;
        }
        int64_t trimed_count = 0;
        WriteBatchGuard batch(ctx, m_engine);
        if (meta.GetMetaObject().list_sequential)
        {
            for (int64_t i = 0; i < ltrim; i++)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(i + meta.GetListMinIdx());
                RemoveKey(ctx, elekey);
                trimed_count++;
            }
            for (int64_t i = rtrim + 1; rtrim > 0 && i < llen; i++)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(i + meta.GetListMinIdx());
                RemoveKey(ctx, elekey);
                trimed_count++;
            }
            int64_t last_min = meta.GetListMinIdx();
            if (ltrim > 0)
            {
                meta.SetListMinIdx(last_min + ltrim);
            }
            if (rtrim > 0)
            {
                meta.SetListMaxIdx(last_min + rtrim);
            }
        }
        else
        {
            Iterator* iter = NULL;
            bool trim_stop = false;
            if (ltrim > 0)
            {
                KeyObject elekey(ctx.ns, KEY_LIST_ELEMENT, cmd.GetArguments()[0]);
                elekey.SetListIndex(meta.GetMin());
                ctx.flags.iterate_total_order = 1;
                iter = m_engine->Find(ctx, elekey);
                while (NULL != iter && iter->Valid())
                {
                    KeyObject& field = iter->Key();
                    if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                            || field.GetKey() != elekey.GetKey())
                    {
                        trim_stop = true;
                        break;
                    }
                    if (ltrim <= 0)
                    {
                        meta.SetMinData(field.GetElement(0), true);
                        break;
                    }

                    //RemoveKey(ctx, field);
                    IteratorDel(ctx, key, iter);
                    trimed_count++;
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
                int64_t tail_trim_count = llen - rtrim - 1;
                while (iter->Valid())
                {
                    KeyObject& field = iter->Key();
                    if (field.GetType() != KEY_LIST_ELEMENT || field.GetNameSpace() != key.GetNameSpace()
                            || field.GetKey() != key.GetKey())
                    {
                        break;
                    }
                    if (tail_trim_count <= 0)
                    {
                        meta.SetMaxData(field.GetElement(0), true);
                        break;
                    }
                    RemoveKey(ctx, field);
                    tail_trim_count--;
                    trimed_count++;
                    iter->Prev();
                }
            }
            DELETE(iter);
        }
        meta.SetObjectLen(meta.GetObjectLen() - trimed_count);
        if (0 == meta.GetObjectLen())
        {
            RemoveKey(ctx, key);
        }
        else
        {
            SetKeyValue(ctx, key, meta);
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
        KeysLockGuard guard(ctx, src, dest);
        RedisCommandFrame rpop;
        rpop.SetCommand("rpop");
        rpop.SetType(REDIS_CMD_RPOP);
        rpop.AddArg(cmd.GetArguments()[0]);
        ListPop(ctx, rpop, false);
        if (reply.type == REDIS_REPLY_STRING)
        {
            std::string data = reply.str;
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
        RedisReply& reply = ctx.GetReply();
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        StringArray list_keys;
        for (size_t i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            RedisCommandFrame list_pop;
            list_pop.SetCommand(cmd.GetType() == REDIS_CMD_BRPOP ? "rpop" : "lpop");
            list_pop.SetType(cmd.GetType() == REDIS_CMD_BRPOP ? REDIS_CMD_RPOP : REDIS_CMD_LPOP);
            list_pop.AddArg(cmd.GetArguments()[i]);
            ListPop(ctx, list_pop, true);
            if (!reply.IsNil())
            {
                if (!reply.IsErr())
                {
                    std::string val = reply.str;
                    reply.ReserveMember(0);
                    RedisReply& r1 = reply.AddMember();
                    RedisReply& r2 = reply.AddMember();
                    r1.SetString(cmd.GetArguments()[i]);
                    r2.SetString(val);
                }
                return 0;
            }
            list_keys.push_back(cmd.GetArguments()[i]);
        }
        reply.type = 0; //wait
        AnyArray empty;
        BlockForKeys(ctx, list_keys,empty, KEY_LIST, timeout * 1000);
        return 0;
    }
    int Ardb::BRPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return BLPop(ctx, cmd);
    }
    int Ardb::BRPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        RPopLPush(ctx, cmd);
        if (reply.IsNil())
        {
            StringArray list_keys(1, cmd.GetArguments()[0]);
            AnyArray empty;
            BlockForKeys(ctx, list_keys, empty, KEY_LIST, timeout * 1000);
            KeyPrefix& bkey = ctx.GetBPop().GetListTarget().target;
            bkey.ns = ctx.ns;
            bkey.key.SetString(cmd.GetArguments()[1], false);
            reply.type = 0;
        }
        return 0;
    }

    int Ardb::ServeClientBlockedOnList(Context& ctx, const KeyPrefix& key, const std::string& value)
    {
        Channel* ch = ctx.client->client;
        if (NULL == ch)
        {
            return -1;
        }
        RedisReply* r = NULL;
        NEW(r, RedisReply);
        RedisReply& r1 = r->AddMember();
        RedisReply& r2 = r->AddMember();
        r1.SetString(key.key);
        r2.SetString(value);
        UnblockKeys(ctx, false, r);
        return 0;
    }
    int Ardb::ListPushValue(Context& ctx, const KeyPrefix& key, const std::string& value, bool lpush,
            RedisCommandFrame& push_cmd)
    {
        if (lpush)
        {
            push_cmd.SetCommand("lpush");
            push_cmd.SetType(REDIS_CMD_LPUSH);
        }
        else
        {
            push_cmd.SetCommand("rpush");
            push_cmd.SetType(REDIS_CMD_RPUSH);
        }
        push_cmd.AddArg(key.key.AsString());
        push_cmd.AddArg(value);
        Context tmpctx;
        tmpctx.ns = key.ns;
        tmpctx.flags.block_keys_locked = ctx.flags.block_keys_locked;
        ListPush(tmpctx, push_cmd, true);
        if (tmpctx.GetReply().IsErr())
        {
            ERROR_LOG("Push list failed with error:%s", tmpctx.GetReply().GetString().c_str());
            return -1;
        }
        return 0;
    }

    int Ardb::ListPopValue(Context& ctx, const KeyPrefix& key, bool lpop, std::string& value,
            RedisCommandFrame& list_pop)
    {
        if (lpop)
        {
            list_pop.SetCommand("lpop");
            list_pop.SetType(REDIS_CMD_LPOP);
        }
        else
        {
            list_pop.SetCommand("rpop");
            list_pop.SetType(REDIS_CMD_RPOP);
        }
        list_pop.AddArg(key.key.AsString());
        Context tmpctx;
        tmpctx.ns = key.ns;
        {
            KeyObject list_key(tmpctx.ns, KEY_META, key.key);
            KeyLockGuard keylocker(tmpctx, list_key);
            ListPop(tmpctx, list_pop, false);
        }
        if (tmpctx.GetReply().IsString())
        {
            value = tmpctx.GetReply().GetString();
            return 0;
        }
        else
        {
            return -1;
        }
    }

    int Ardb::WakeClientsBlockingOnList(Context& ctx, const KeyPrefix& ready_key, Context& unblock_client)
    {
        std::string pop_value;
        RedisCommandFrame list_pop;
        int err = 0;
        err = ListPopValue(ctx, ready_key, unblock_client.last_cmdtype == REDIS_CMD_BLPOP, pop_value, list_pop);
        if (0 == err)
        {
            if (NULL != unblock_client.GetBPop().list_target)
            {
                RedisCommandFrame list_push;
                err = ListPushValue(ctx, unblock_client.GetBPop().list_target->target, pop_value, true, list_push);
                if (0 != err)
                {
                    /*
                     * repush value into old list
                     */
                    RedisCommandFrame list_repush;
                    ListPushValue(ctx, ready_key, pop_value, unblock_client.last_cmdtype == REDIS_CMD_BLPOP,
                            list_repush);
                }
                else
                {
                    /*
                     * generate 'lpop/rpop' for replication in master
                     */
                    if (GetConf().master_host.empty())
                    {
                        FeedReplicationBacklog(ctx, ready_key.ns, list_pop);
                        FeedReplicationBacklog(ctx, ready_key.ns, list_push);
                    }
                }
            }
            else
            {
                /*
                 * generate 'lpop/rpop' for replication in master
                 */
                if (GetConf().master_host.empty())
                {
                    FeedReplicationBacklog(ctx, ready_key.ns, list_pop);
                }
            }
        }
        if (0 == err)
        {
            ServeClientBlockedOnList(unblock_client, ready_key, pop_value);
        }
        return err;
    }

OP_NAMESPACE_END


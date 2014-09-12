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

    static Data* GetZipEntry(DataArray& ziplist, int64 index)
    {
        if (index < 0)
        {
            index = ziplist.size() + index;
        }
        if (index < 0 || (uint32) index >= ziplist.size())
        {
            return NULL;
        }
        return &(ziplist[index]);
    }

    int Ardb::ZipListConvert(Context& ctx, ValueObject& meta)
    {
        meta.meta.SetEncoding(COLLECTION_ECODING_RAW);
        meta.meta.SetFlag(COLLECTION_FLAG_SEQLIST);
        meta.meta.min_index.SetInt64(0);
        meta.meta.max_index.SetInt64(meta.meta.ziplist.size() - 1);
        for (uint32 i = 0; i < meta.meta.ziplist.size(); i++)
        {
            ValueObject v;
            v.type = LIST_ELEMENT;
            v.element = meta.meta.ziplist[i];
            v.key.type = LIST_ELEMENT;
            v.key.key = meta.key.key;
            v.key.db = ctx.currentDB;
            v.key.score.SetInt64(i);
            SetKeyValue(ctx, v);
        }
        meta.meta.len = meta.meta.ziplist.size();
        meta.meta.ziplist.clear();
        return 0;
    }
    int Ardb::LIndex(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 index;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], index))
        {
            return 0;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (err == ERR_NOT_EXIST)
        {
            return 0;
        }
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            Data* entry = GetZipEntry(meta.meta.ziplist, index);
            if (NULL != entry)
            {
                fill_value_reply(ctx.reply, *entry);
            }
            else
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            return 0;
        }
        else
        {
            if ((index >= 0 && index >= meta.meta.Length()) || (index < 0 && (meta.meta.Length() + index) < 0))
            {
                ctx.reply.type = REDIS_REPLY_NIL;
                return 0;
            }
            ListIterator iter;
            if (meta.meta.IsSequentialList())
            {
                err = SequencialListIter(ctx, meta, iter, index);
                if (0 == err)
                {
                    if (iter.Valid())
                    {
                        fill_value_reply(ctx.reply, *(iter.Element()));
                        return 0;
                    }
                }
            }
            else
            {
                err = ListIter(ctx, meta, iter, index < 0);
                uint32 cursor = index < 0 ? 1 : 0;
                while (iter.Valid())
                {
                    if (cursor == std::abs(index))
                    {
                        fill_value_reply(ctx.reply, *(iter.Element()));
                        return 0;
                    }
                    cursor++;
                    if (index >= 0)
                    {
                        iter.Next();
                    }
                    else
                    {
                        iter.Prev();
                    }
                }
            }
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        return 0;
    }

    int Ardb::ListLen(Context& ctx, const Slice& key)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        fill_int_reply(ctx.reply, meta.meta.Length());
        return 0;
    }

    int Ardb::LLen(Context& ctx, RedisCommandFrame& cmd)
    {
        ListLen(ctx, cmd.GetArguments()[0]);
        return 0;
    }

    int Ardb::ListPop(Context& ctx, const std::string& key, bool lpop)
    {
        ValueObject meta;
        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, key);
        int err = GetMetaValue(ctx, key, LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
            return 0;
        }
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPLIST);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            if (!meta.meta.ziplist.empty())
            {
                if (lpop)
                {
                    Data& data = meta.meta.ziplist.front();
                    fill_value_reply(ctx.reply, data);
                    meta.meta.ziplist.pop_front();
                }
                else
                {
                    Data& data = meta.meta.ziplist.back();
                    fill_value_reply(ctx.reply, data);
                    meta.meta.ziplist.pop_back();
                }
                if (meta.meta.ziplist.empty())
                {
                    DelKeyValue(ctx, meta.key);
                }
                else
                {
                    SetKeyValue(ctx, meta);
                }
            }
            else
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            return 0;
        }
        else
        {
            bool poped = false;
            if (meta.meta.IsSequentialList())
            {
                if (meta.meta.Length() > 0)
                {
                    ValueObject lkv;
                    lkv.key.type = LIST_ELEMENT;
                    lkv.key.key = meta.key.key;
                    lkv.key.db = ctx.currentDB;
                    lkv.key.score = lpop ? meta.meta.min_index : meta.meta.max_index;
                    if (0 == GetKeyValue(ctx, lkv.key, &lkv))
                    {
                        DelKeyValue(ctx, lkv.key);
                        if (lpop)
                        {
                            meta.meta.min_index.IncrBy(1);
                        }
                        else
                        {
                            meta.meta.max_index.IncrBy(-1);
                        }
                        meta.meta.len--;
                        poped = true;
                        fill_value_reply(ctx.reply, lkv.element);
                    }
                }
            }
            else
            {
                ListIterator iter;
                err = ListIter(ctx, meta, iter, !lpop);
                while (iter.Valid())
                {
                    if (!poped)
                    {
                        fill_value_reply(ctx.reply, *(iter.Element()));
                        poped = true;
                        meta.meta.len--;
                        KeyObject k;
                        k.type = LIST_ELEMENT;
                        k.key = meta.key.key;
                        k.db = ctx.currentDB;
                        k.score = *(iter.Score());
                        DelKeyValue(ctx, k);
                    }
                    else
                    {
                        if (lpop)
                        {
                            meta.meta.min_index = *(iter.Score());
                        }
                        else
                        {
                            meta.meta.max_index = *(iter.Score());
                        }
                        break;
                    }
                    if (lpop)
                    {
                        iter.Next();
                    }
                    else
                    {
                        iter.Prev();
                    }
                }
            }
            if (poped)
            {
                if (meta.meta.Length() > 0)
                {
                    SetKeyValue(ctx, meta);
                }
                else
                {
                    DelKeyValue(ctx, meta.key);
                }
            }
            else
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            return 0;
        }
    }

    int Ardb::LPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPop(ctx, cmd.GetArguments()[0], true);
    }

    int Ardb::ListInsert(Context& ctx, ValueObject& meta, const std::string* match, const std::string& value, bool head,
            bool abort_nonexist)
    {
        if (WakeBlockList(ctx, meta.key.key, value))
        {
            fill_int_reply(ctx.reply, 1);
            return 0;
        }
        if (NULL != match)
        {
            if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
            {
                Data element;
                element.SetString(value, true);
                DataArray::iterator zit = meta.meta.ziplist.begin();
                while (zit != meta.meta.ziplist.end())
                {
                    std::string tmp;
                    zit->GetDecodeString(tmp);
                    if (tmp == *match)
                    {
                        break;
                    }
                    zit++;
                }
                if (zit == meta.meta.ziplist.end())
                {
                    fill_int_reply(ctx.reply, 0);
                    return 0;
                }
                if (head)
                {
                    meta.meta.ziplist.insert(zit, element);
                }
                else
                {
                    zit++;
                    if (zit != meta.meta.ziplist.end())
                    {
                        meta.meta.ziplist.insert(zit, element);
                    }
                    else
                    {
                        meta.meta.ziplist.push_back(element);
                    }
                }
                if (meta.meta.Length() >= m_cfg.list_max_ziplist_entries
                        || element.StringLength() >= m_cfg.list_max_ziplist_value)
                {
                    //convert to non ziplist
                    ZipListConvert(ctx, meta);
                }
            }
            else
            {
                ListIterator iter;
                ListIter(ctx, meta, iter, false);
                std::string tmp;
                Data prev, next;
                Data current;
                bool matched = false;
                while (iter.Valid())
                {
                    if (iter.Element()->GetDecodeString(tmp) == (*match))
                    {
                        current = *(iter.Score());
                        matched = true;
                        if (head)
                        {
                            break;
                        }
                    }
                    if (head)
                    {
                        prev = *(iter.Score());
                        iter.Next();
                    }
                    else
                    {
                        if (matched)
                        {
                            next = *(iter.Score());
                            break;
                        }
                    }
                    iter.Next();
                }
                if (!matched)
                {
                    fill_int_reply(ctx.reply, 0);
                    return 0;
                }
                Data score;
                if (head)
                {
                    if (prev.IsNil())
                    {
                        score = current.IncrBy(-1);
                    }
                    else
                    {
                        score.SetDouble((prev.NumberValue() + current.NumberValue()) / 2);
                        meta.meta.SetFlag(COLLECTION_FLAG_NORMAL);
                    }
                }
                else
                {
                    if (next.IsNil())
                    {
                        score = current.IncrBy(1);
                    }
                    else
                    {
                        score.SetDouble((next.NumberValue() + current.NumberValue()) / 2);
                        meta.meta.SetFlag(COLLECTION_FLAG_NORMAL);
                    }
                }

                meta.meta.len++;
                ValueObject v;
                v.type = LIST_ELEMENT;
                v.element.SetString(value, true);
                v.key.db = meta.key.db;
                v.key.key = meta.key.key;
                v.key.type = LIST_ELEMENT;
                v.key.score = score;
                SetKeyValue(ctx, v);
            }
            fill_int_reply(ctx.reply, meta.meta.Length());
            return 0;
        }
        else
        {
            if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
            {
                Data element;
                element.SetString(value, true);
                if (head)
                {
                    meta.meta.ziplist.push_front(element);
                }
                else
                {
                    meta.meta.ziplist.push_back(element);
                }
                if (meta.meta.Length() >= m_cfg.list_max_ziplist_entries
                        || element.StringLength() >= m_cfg.list_max_ziplist_value)
                {
                    //convert to non ziplist
                    ZipListConvert(ctx, meta);
                }
            }
            else
            {
                meta.meta.len++;
                ValueObject v;
                v.type = LIST_ELEMENT;
                v.element.SetString(value, true);
                v.key.db = meta.key.db;
                v.key.key = meta.key.key;
                v.key.type = LIST_ELEMENT;

                if (head)
                {
                    v.key.score = meta.meta.min_index.IncrBy(-1);
                }
                else
                {
                    v.key.score = meta.meta.max_index.IncrBy(1);
                }
                SetKeyValue(ctx, v);
            }
            fill_int_reply(ctx.reply, meta.meta.Length());
            return 0;
        }
    }

    struct BlockConnectionTimeout: public Runnable
    {
            Context* ctx;
            BlockConnectionTimeout(Context* cc) :
                    ctx(cc)
            {
            }
            void Run()
            {
                ctx->block->blocking_timer_task_id = -1;
                ctx->block->waked_value.clear();
                Ardb::WakeBlockedConnCallback(ctx->client, ctx);
            }
    };

    void Ardb::WakeBlockedConnCallback(Channel* ch, void* data)
    {
        if (NULL != ch)
        {
            Context* ctx = (Context*) data;
            if (-1 != ctx->block->blocking_timer_task_id)
            {
                ch->GetService().GetTimer().Cancel(ctx->block->blocking_timer_task_id);
                ctx->block->blocking_timer_task_id = -1;
            }
            if (ctx->block->dest_key.empty())
            {
                ctx->reply.type = REDIS_REPLY_ARRAY;
                if (!ctx->block->waked_value.empty())
                {
                    RedisReply& r1 = ctx->reply.AddMember();
                    RedisReply& r2 = ctx->reply.AddMember();
                    fill_str_reply(r1, ctx->block->waked_key.key);
                    fill_str_reply(r2, ctx->block->waked_value);
                    RedisCommandFrame lpop("lpop");
                    lpop.AddArg(ctx->block->waked_key.key);
                    g_db->m_master.FeedSlaves(ctx->currentDB, lpop);
                }
            }
            else
            {
                if (!ctx->block->waked_value.empty())
                {
                    fill_str_reply(ctx->reply, ctx->block->waked_value);
                    RedisCommandFrame lpush("lpush");
                    lpush.AddArg(ctx->block->dest_key);
                    lpush.AddArg(ctx->block->waked_value);
                    g_db->LPush(*ctx, lpush);
                    RedisCommandFrame lpop("lpop");
                    lpop.AddArg(ctx->block->waked_key.key);
                    g_db->m_master.FeedSlaves(ctx->currentDB, lpop);
                    g_db->m_master.FeedSlaves(ctx->currentDB, lpush);
                }
                else
                {
                    ctx->reply.type = REDIS_REPLY_NIL;
                }
            }
            ch->Write(ctx->reply);
            g_db->ClearBlockKeys(*ctx);
            ch->AttachFD();
        }
    }

    bool Ardb::WakeBlockList(Context& ctx, const Slice& key, const std::string& value)
    {
        DBItemKey kk;
        kk.db = ctx.currentDB;
        kk.key.assign(key.data(), key.size());
        WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
        BlockContextTable::iterator fit = m_block_context_table.find(kk);
        if (fit != m_block_context_table.end())
        {
            while (!fit->second.empty())
            {
                Context* cctx = fit->second.front();
                if (NULL
                        != cctx&& cctx->block != NULL && NULL != cctx->client && cctx->block->block_state == BLOCK_STATE_BLOCKED)
                {
                    cctx->block->waked_key = kk;
                    cctx->block->waked_value = value;
                    cctx->block->block_state = BLOCK_STATE_WAKING;
                    cctx->client->GetService().AsyncIO(cctx->client->GetID(), WakeBlockedConnCallback, cctx);
                    fit->second.pop_front();
                    return true;
                }
                fit->second.pop_front();
            }
        }
        return false;
    }

    int Ardb::ListInsert(Context& ctx, const std::string& key, const std::string* match, const std::string& value,
            bool head, bool abort_nonexist)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPLIST);
        if (0 != err && abort_nonexist)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        ListInsert(ctx, meta, match, value, head, abort_nonexist);
        SetKeyValue(ctx, meta);
        return 0;
    }

    int Ardb::LInsert(Context& ctx, RedisCommandFrame& cmd)
    {
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
            fill_error_reply(ctx.reply, "Syntax error");
            return 0;
        }
        return ListInsert(ctx, cmd.GetArguments()[0], &(cmd.GetArguments()[2]), cmd.GetArguments()[3], head, true);
    }
    int Ardb::LPush(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        KeyLockerGuard lock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        BatchWriteGuard guard(GetKeyValueEngine(), cmd.GetArguments().size() > 2);
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            ListInsert(ctx, meta, NULL, cmd.GetArguments()[i], true, false);
        }
        SetKeyValue(ctx, meta);
        return 0;
    }
    int Ardb::LPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListInsert(ctx, cmd.GetArguments()[0], NULL, cmd.GetArguments()[1], true, true);
    }

    int Ardb::ListRange(Context& ctx, const Slice& key, int64 start, int64 end)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, key, LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        /* convert negative indexes */
        if (start < 0)
            start = meta.meta.Length() + start;
        if (end < 0)
            end = meta.meta.Length() + end;
        if (start < 0)
            start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (start > end || start >= meta.meta.Length())
        {
            return 0;
        }
        if (end >= meta.meta.Length())
            end = meta.meta.Length() - 1;
        int64 rangelen = (end - start) + 1;
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            uint32 i = start;
            while (rangelen--)
            {
                RedisReply& r = ctx.reply.AddMember();
                fill_value_reply(r, meta.meta.ziplist[i++]);
            }
        }
        else
        {
            ListIterator iter;
            if (meta.meta.IsSequentialList())
            {
                SequencialListIter(ctx, meta, iter, start);
                uint32 count = 0;
                while (iter.Valid() && count < rangelen)
                {
                    RedisReply& r = ctx.reply.AddMember();
                    fill_str_reply(r, iter.Element()->ToString());
                    count++;
                    iter.Next();
                }
            }
            else
            {
                if (start > meta.meta.len / 2)
                {
                    start -= meta.meta.len;
                    end -= meta.meta.len;
                }

                ListIter(ctx, meta, iter, start < 0);
                int64 cursor = start < 0 ? -1 : 0;
                while (iter.Valid())
                {
                    if (cursor >= start && cursor <= end)
                    {
                        RedisReply& r = ctx.reply.AddMember();
                        fill_str_reply(r, iter.Element()->ToString());
                    }
                    if (start < 0)
                    {
                        if (cursor < start)
                        {
                            break;
                        }
                        cursor--;
                    }
                    else
                    {
                        if (cursor > end)
                        {
                            break;
                        }
                        cursor++;
                    }
                    iter.Next();
                }
            }
        }
        return 0;
    }

    int Ardb::LRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        ListRange(ctx, cmd.GetArguments()[0], start, end);
        return 0;
    }
    int Ardb::LRem(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 count;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], count))
        {
            return 0;
        }
        int64 toremove = std::abs(count);
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_int_reply(ctx.reply, 0);
            return 0;
        }
        Data element;
        element.SetString(cmd.GetArguments()[2], true);
        KeyLockerGuard lock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            uint32 oldlen = meta.meta.ziplist.size();
            int64 removed = 0;
            DataArray newzip;
            if (count >= 0)
            {
                for (uint32 i = 0; i < oldlen; i++)
                {
                    if (meta.meta.ziplist[i] == element)
                    {
                        if (toremove == 0 || removed < toremove)
                        {
                            removed++;
                            continue;
                        }
                    }
                    newzip.push_back(meta.meta.ziplist[i]);
                }
            }
            else
            {
                for (uint32 i = 0; i < oldlen; i++)
                {
                    if (meta.meta.ziplist[oldlen - 1 - i] == element)
                    {
                        if (toremove == 0 || removed < toremove)
                        {
                            removed++;
                            continue;
                        }
                    }
                    newzip.push_front(meta.meta.ziplist[i]);
                }
            }
            if (removed > 0)
            {
                meta.meta.ziplist = newzip;
                SetKeyValue(ctx, meta);
            }
            fill_int_reply(ctx.reply, removed);
            return 0;
        }
        BatchWriteGuard guard(GetKeyValueEngine());
        ListIterator iter;
        ListIter(ctx, meta, iter, count < 0);
        int64 remove = 0;
        while (iter.Valid())
        {
            if (iter.Element()->Compare(element) == 0)
            {
                meta.meta.len--;
                meta.meta.SetFlag(COLLECTION_FLAG_NORMAL);
                KeyObject k;
                k.db = meta.key.db;
                k.key = meta.key.key;
                k.type = LIST_ELEMENT;
                k.score = *(iter.Score());
                DelRaw(ctx, iter.CurrentRawKey());
                //DelKeyValue(ctx, k);
                remove++;
                if (remove == toremove)
                {
                    break;
                }
            }
            if (count < 0)
            {
                iter.Prev();
            }
            else
            {
                iter.Next();
            }
        }
        if (remove > 0)
        {
            SetKeyValue(ctx, meta);
        }
        fill_int_reply(ctx.reply, remove);
        return 0;
    }
    int Ardb::LSet(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 index;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], index))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_error_reply(ctx.reply, "no such key");
            return 0;
        }
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            Data* entry = GetZipEntry(meta.meta.ziplist, index);
            if (NULL == entry)
            {
                fill_error_reply(ctx.reply, "index out of range");
                return 0;
            }
            else
            {
                entry->SetString(cmd.GetArguments()[2], true);
                SetKeyValue(ctx, meta);
                fill_status_reply(ctx.reply, "OK");
                return 0;
            }
        }
        else
        {
            if (index >= meta.meta.Length() || (-index) > meta.meta.Length())
            {
                fill_error_reply(ctx.reply, "index out of range");
                return 0;
            }

            if (meta.meta.IsSequentialList())
            {
                ValueObject list_element;
                list_element.key.db = meta.key.db;
                list_element.key.key = meta.key.key;
                list_element.key.type = LIST_ELEMENT;
                list_element.key.score = meta.meta.min_index;
                if (index >= 0)
                {
                    list_element.key.score.IncrBy(index);
                }
                else
                {
                    list_element.key.score.IncrBy(index + meta.meta.Length());
                }
                if (0 == GetKeyValue(ctx, list_element.key, &list_element))
                {
                    list_element.element.SetString(cmd.GetArguments()[2], true);
                    SetKeyValue(ctx, list_element);
                    fill_status_reply(ctx.reply, "OK");
                    return 0;
                }
            }
            else
            {
                ListIterator iter;
                ListIter(ctx, meta, iter, index < 0);
                int64 cursor = index >= 0 ? 0 : -1;
                while (iter.Valid())
                {
                    if (cursor == index)
                    {
                        ValueObject v;
                        v.key.db = meta.key.db;
                        v.key.key = meta.key.key;
                        v.key.type = LIST_ELEMENT;
                        v.key.score = *(iter.Score());
                        v.type = LIST_ELEMENT;
                        v.element.SetString(cmd.GetArguments()[2], true);
                        SetKeyValue(ctx, v);
                        fill_status_reply(ctx.reply, "OK");
                        return 0;
                    }
                    if (cursor >= 0)
                    {
                        cursor++;
                    }
                    else
                    {
                        cursor--;
                    }
                    if (index < 0)
                    {
                        iter.Prev();
                    }
                    else
                    {
                        iter.Next();
                    }
                }
            }
            fill_error_reply(ctx.reply, "index out of range");
        }
        return 0;
    }

    int Ardb::LTrim(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_status_reply(ctx.reply, "OK");
            return 0;
        }
        /* convert negative indexes */
        if (start < 0)
            start = meta.meta.Length() + start;
        if (end < 0)
            end = meta.meta.Length() + end;
        if (start < 0)
            start = 0;
        if (end >= meta.meta.Length())
            end = meta.meta.Length() - 1;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= meta.meta.Length())
        {
            /* Out of range start or start > end result in empty list */
            DeleteKey(ctx, cmd.GetArguments()[0]);
            return 0;
        }
        if (meta.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
        {
            DataArray newzip;
            for (int64 i = start; i <= end; i++)
            {
                newzip.push_back(meta.meta.ziplist[i]);
            }
            meta.meta.ziplist = newzip;
            SetKeyValue(ctx, meta);
        }
        else
        {
            BatchWriteGuard guard(GetKeyValueEngine());
            if (meta.meta.IsSequentialList())
            {
                int64 listlen = meta.meta.Length();
                for (int64 s = 0; s < listlen; s++)
                {
                    if (s == start)
                    {
                        s = end;
                        continue;
                    }
                    KeyObject lk;
                    lk.db = meta.key.db;
                    lk.key = meta.key.key;
                    lk.type = LIST_ELEMENT;
                    lk.score = meta.meta.min_index.IncrBy(s);
                    meta.meta.len--;
                    DelKeyValue(ctx, lk);
                }
                meta.meta.max_index = meta.meta.min_index;
                meta.meta.min_index.IncrBy(start);
                meta.meta.max_index.IncrBy(end);
            }
            else
            {
                ListIterator iter;
                ListIter(ctx, meta, iter, false);
                int64 cursor = 0;
                while (iter.Valid())
                {
                    if (cursor < start || cursor > end)
                    {
                        DelRaw(ctx, iter.CurrentRawKey());
                        meta.meta.len--;
                    }
                    if (cursor == start)
                    {
                        meta.meta.min_index = *(iter.Element());
                    }
                    else if (cursor == end)
                    {
                        meta.meta.max_index = *(iter.Element());
                    }
                    cursor++;
                    iter.Next();
                }
            }
            SetKeyValue(ctx, meta);
        }
        return 0;
    }

    int Ardb::RPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListPop(ctx, cmd.GetArguments()[0], false);
    }
    int Ardb::RPush(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        int err = GetMetaValue(ctx, cmd.GetArguments()[0], LIST_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        KeyLockerGuard lock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        BatchWriteGuard guard(GetKeyValueEngine(), cmd.GetArguments().size() > 2);
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            ListInsert(ctx, meta, NULL, cmd.GetArguments()[i], false, false);
        }
        SetKeyValue(ctx, meta);
        return 0;
    }
    int Ardb::RPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        return ListInsert(ctx, cmd.GetArguments()[0], NULL, cmd.GetArguments()[1], false, true);
    }

    int Ardb::RPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        if (ListPop(ctx, cmd.GetArguments()[0], false) == 0 && ctx.reply.type == REDIS_REPLY_STRING)
        {
            std::string value = ctx.reply.str;
            ListInsert(ctx, cmd.GetArguments()[1], NULL, ctx.reply.str, false, false);
            fill_str_reply(ctx.reply, value);
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        return 0;
    }

    void Ardb::ClearBlockKeys(Context& ctx)
    {
        if (NULL != ctx.block)
        {
            if (ctx.block->blocking_timer_task_id != -1)
            {
                ctx.client->GetService().GetTimer().Cancel(ctx.block->blocking_timer_task_id);
            }
            WatchKeySet::iterator it = ctx.GetBlockContext().keys.begin();
            while (it != ctx.GetBlockContext().keys.end())
            {
                WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
                BlockContextTable::iterator fit = m_block_context_table.find(*it);
                if (fit != m_block_context_table.end())
                {
                    fit->second.erase(std::remove(fit->second.begin(), fit->second.end(), &ctx), fit->second.end());
                    if (fit->second.empty())
                    {
                        m_block_context_table.erase(fit);
                    }
                }
            }
            ctx.ClearBlockContext();
        }
    }

    void Ardb::AddBlockKey(Context& ctx, const std::string& key)
    {
        DBItemKey kk(ctx.currentDB, key);
        ctx.GetBlockContext().keys.insert(kk);
        WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
        m_block_context_table[kk].push_back(&ctx);
    }

    int Ardb::BLPop(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            if (ListPop(ctx, cmd.GetArguments()[i], true) == 0 && ctx.reply.type == REDIS_REPLY_STRING)
            {
                RedisReply& r1 = ctx.reply.AddMember();
                RedisReply& r2 = ctx.reply.AddMember();
                fill_str_reply(r1, cmd.GetArguments()[i]);
                fill_str_reply(r2, ctx.reply.str);
                return 0;
            }
        }
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            AddBlockKey(ctx, cmd.GetArguments()[i]);
        }
        if (NULL != ctx.client)
        {
            ctx.client->DetachFD();
        }
        if (timeout > 0)
        {
            ctx.block->blocking_timer_task_id = ctx.client->GetService().GetTimer().ScheduleHeapTask(
                    new BlockConnectionTimeout(&ctx), timeout, -1, SECONDS);
        }
        return 0;
    }
    int Ardb::BRPop(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            if (ListPop(ctx, cmd.GetArguments()[i], false) == 0 && ctx.reply.type == REDIS_REPLY_STRING)
            {
                RedisReply& r1 = ctx.reply.AddMember();
                RedisReply& r2 = ctx.reply.AddMember();
                fill_str_reply(r1, cmd.GetArguments()[i]);
                fill_str_reply(r2, ctx.reply.str);
                return 0;
            }
        }
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            AddBlockKey(ctx, cmd.GetArguments()[i]);
        }
        if (NULL != ctx.client)
        {
            ctx.client->DetachFD();
            if (timeout > 0)
            {
                ctx.block->blocking_timer_task_id = ctx.client->GetService().GetTimer().ScheduleHeapTask(
                        new BlockConnectionTimeout(&ctx), timeout, -1, SECONDS);
            }
        }
        return 0;
    }
    int Ardb::BRPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        RPopLPush(ctx, cmd);
        if (ctx.reply.type == REDIS_REPLY_NIL)
        {
            //block;
            AddBlockKey(ctx, cmd.GetArguments()[0]);
            if (NULL != ctx.client)
            {
                ctx.client->DetachFD();
                if (timeout > 0)
                {
                    ctx.block->blocking_timer_task_id = ctx.client->GetService().GetTimer().ScheduleHeapTask(
                            new BlockConnectionTimeout(&ctx), timeout, -1, SECONDS);
                }
            }
            ctx.reply.type = 0;
        }
        return 0;
    }

    int Ardb::LClear(Context& ctx, ValueObject& meta)
    {
        BatchWriteGuard guard(GetKeyValueEngine(), meta.meta.Encoding() != COLLECTION_ECODING_ZIPLIST);
        if (meta.meta.Encoding() != COLLECTION_ECODING_ZIPLIST)
        {
            ListIterator iter;
            meta.meta.len = 0;
            ListIter(ctx, meta, iter, false);
            while (iter.Valid())
            {
                KeyObject fk;
                fk.db = ctx.currentDB;
                fk.key = meta.key.key;
                fk.type = LIST_ELEMENT;
                fk.score = *(iter.Score());
                DelKeyValue(ctx, fk);
                iter.Next();
            }
        }
        DelKeyValue((ctx), meta.key);
        return 0;
    }

    int Ardb::RenameList(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey)
    {
        Context tmpctx;
        tmpctx.currentDB = srcdb;
        ValueObject v;
        int err = GetMetaValue(tmpctx, srckey, LIST_META, v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, err);
        if (0 != err)
        {
            fill_error_reply(ctx.reply, "no such key or some error");
            return 0;
        }
        if (v.meta.Encoding() == COLLECTION_ECODING_ZIPLIST)
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
            ListIterator iter;
            ListIter(ctx, v, iter, false);
            tmpctx.currentDB = dstdb;
            ValueObject dstmeta;
            dstmeta.key.type = KEY_META;
            dstmeta.key.key = dstkey;
            dstmeta.type = LIST_META;
            dstmeta.meta.SetFlag(COLLECTION_FLAG_SEQLIST);
            dstmeta.meta.SetEncoding(COLLECTION_ECODING_ZIPLIST);
            BatchWriteGuard guard(GetKeyValueEngine());
            while (iter.Valid())
            {
                std::string tmpstr;
                ListInsert(tmpctx, dstmeta, NULL, iter.Element()->GetDecodeString(tmpstr), false, false);
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


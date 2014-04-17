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
#include <algorithm>

#define BLOCK_LIST_RPOP 0
#define BLOCK_LIST_LPOP 1
#define BLOCK_LIST_RPOPLPUSH 2

namespace ardb
{

    void ArdbServer::BlockTimeoutTask::Run()
    {
        if (event_service->GetChannel(conn_id) != NULL && ctx->conn->GetID() == conn_id)
        {
            ctx->reply.type = REDIS_REPLY_NIL;
            ctx->conn->Write(ctx->reply);
            server->UnblockConn(*ctx);
            ctx->reply.Clear();
        }
    }

    struct IsCtxOp
    {
            ArdbConnContext* _ctx;
            bool operator()(const BlockConnContext& ctx)
            {
                return ctx.ctx == _ctx;
            }
    };

    static void erase_context_in_table(BlockContextTable& table, const WatchKey& key, ArdbConnContext* ctx)
    {
        BlockContextTable::iterator fit = table.find(key);
        if (fit != table.end())
        {
            IsCtxOp pred;
            pred._ctx = ctx;
            fit->second.erase(std::remove_if(fit->second.begin(), fit->second.end(), pred), fit->second.end());
            if (fit->second.empty())
            {
                table.erase(fit);
            }
        }
    }

    void ArdbServer::BlockConn(ArdbConnContext& ctx, uint32 timeout)
    {
        if (timeout > 0)
        {
            NEW2(task, BlockTimeoutTask, BlockTimeoutTask(this, &ctx));
            ctx.GetBlockList().blocking_timer_task_id = ctx.conn->GetService().GetTimer().ScheduleHeapTask(task,
                    timeout, -1, SECONDS);
        }
        ctx.conn->BlockRead();
    }
    void ArdbServer::UnblockConn(ArdbConnContext& ctx)
    {
        if (ctx.GetBlockList().blocking_timer_task_id > 0)
        {
            ctx.conn->GetService().GetTimer().Cancel(ctx.GetBlockList().blocking_timer_task_id);
            ctx.GetBlockList().blocking_timer_task_id = -1;
        }
        LockGuard<ThreadMutex> guard(m_block_mutex);
        WatchKeySet::iterator it = ctx.GetBlockList().keys.begin();
        while (it != ctx.GetBlockList().keys.end())
        {
            erase_context_in_table(m_blocking_conns, *it, &ctx);
            it++;
        }
        ctx.ClearBlockList();
        ctx.conn->UnblockRead();
    }

    void ArdbServer::ClearClosedConnContext(ArdbConnContext& ctx)
    {
        ClearWatchKeys(ctx);
        ClearSubscribes(ctx);
        {
            if (ctx.GetBlockList().blocking_timer_task_id > 0)
            {
                if (!ctx.conn->GetService().GetTimer().Cancel(ctx.GetBlockList().blocking_timer_task_id))
                {
                    WARN_LOG("Failed to cancel block timeout task by id:%d", ctx.GetBlockList().blocking_timer_task_id);
                }
                ctx.GetBlockList().blocking_timer_task_id = -1;
            }
            LockGuard<ThreadMutex> guard(m_block_mutex);
            WatchKeySet::iterator it = ctx.GetBlockList().keys.begin();
            while (it != ctx.GetBlockList().keys.end())
            {
                erase_context_in_table(m_blocking_conns, *it, &ctx);
                it++;
            }
            ctx.ClearBlockList();
        }
    }
    void ArdbServer::WakeBlockedConnOnList(Channel* ch, void * data)
    {
        BlockConnWakeContext* bctx = (BlockConnWakeContext*) data;
        RETURN_IF_NULL(bctx);
        if (NULL == ch || ch->IsClosed())
        {
            DELETE(bctx);
            return;
        }
        ArdbConnContext* ctx = bctx->ctx;
        Ardb* db = bctx->server->m_db;
        WatchKey& key = bctx->key;
        std::string v;
        if (ctx->GetBlockList().pop_type == BLOCK_LIST_LPOP)
        {
            db->LPop(key.db, key.key, v);
        }
        else if (ctx->GetBlockList().pop_type == BLOCK_LIST_RPOP)
        {
            db->RPop(key.db, key.key, v);
        }
        else if (ctx->GetBlockList().pop_type == BLOCK_LIST_RPOPLPUSH)
        {
            db->RPopLPush(key.db, key.key, ctx->GetBlockList().dest_key, v);
        }
        if (!v.empty())
        {
            LockGuard<ThreadMutex> guard(bctx->server->m_block_mutex);
            WatchKeySet::iterator cit = ctx->GetBlockList().keys.begin();
            while (cit != ctx->GetBlockList().keys.end())
            {
                erase_context_in_table(bctx->server->m_blocking_conns, *cit, ctx);
                cit++;
            }
            ctx->reply.type = REDIS_REPLY_ARRAY;
            ctx->reply.elements.push_back(RedisReply(key.key));
            ctx->reply.elements.push_back(RedisReply(v));
        }
        else
        {
            ctx->reply.type = REDIS_REPLY_NIL;
        }
        ctx->conn->UnblockRead();
        ctx->conn->Write(ctx->reply);
        ctx->reply.Clear();
        if (ctx->GetBlockList().blocking_timer_task_id > 0)
        {
            ctx->conn->GetService().GetTimer().Cancel(ctx->GetBlockList().blocking_timer_task_id);
            ctx->GetBlockList().blocking_timer_task_id = -1;
        }
        ctx->ClearBlockList();
        DELETE(bctx);
    }

    void ArdbServer::CheckBlockingConnectionsByListKey(const WatchKey& key)
    {
        LockGuard<ThreadMutex> guard(m_block_mutex);
        BlockContextTable::iterator found = m_blocking_conns.find(key);
        if (found != m_blocking_conns.end() && !found->second.empty())
        {
            if (m_db->LLen(key.db, key.key) > 0)
            {
                BlockConnContext& ctx = found->second.front();
                BlockConnWakeContext* wakeCtx = NULL;
                NEW(wakeCtx, BlockConnWakeContext);
                wakeCtx->ctx = ctx.ctx;
                wakeCtx->key = key;
                wakeCtx->server = this;
                ctx.eventService->AsyncIO(ctx.connId, WakeBlockedConnOnList, wakeCtx);
                found->second.pop_front();
            }
        }
    }
    int ArdbServer::LIndex(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int index;
        if (!string_toint32(cmd.GetArguments()[1], index))
        {
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        std::string v;
        int ret = m_db->LIndex(ctx.currentDB, cmd.GetArguments()[0], index, v);
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

    int ArdbServer::LInsert(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->LInsert(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2],
                cmd.GetArguments()[3]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        if (ret > 0)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            CheckBlockingConnectionsByListKey(key);
        }
        return 0;
    }

    int ArdbServer::LLen(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->LLen(ctx.currentDB, cmd.GetArguments()[0]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int ArdbServer::LPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int ret = m_db->LPop(ctx.currentDB, cmd.GetArguments()[0], v);
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
    int ArdbServer::LPush(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int count = 0;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            count = m_db->LPush(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[i]);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, count);
        }
        if (count < 0)
        {
            count = 0;
        }
        fill_int_reply(ctx.reply, count);
        if (count > 0)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            CheckBlockingConnectionsByListKey(key);
        }
        return 0;
    }
    int ArdbServer::LPushx(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->LPushx(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        if (ret > 0)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            CheckBlockingConnectionsByListKey(key);
        }
        return 0;
    }

    int ArdbServer::LRange(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int start, stop;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], stop))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        ValueDataArray vs;
        int ret = m_db->LRange(ctx.currentDB, cmd.GetArguments()[0], start, stop, vs);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_array_reply(ctx.reply, vs);
        return 0;
    }
    int ArdbServer::LRem(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int count;
        if (!string_toint32(cmd.GetArguments()[1], count))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->LRem(ctx.currentDB, cmd.GetArguments()[0], count, cmd.GetArguments()[2]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }
    int ArdbServer::LSet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int index;
        if (!string_toint32(cmd.GetArguments()[1], index))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->LSet(ctx.currentDB, cmd.GetArguments()[0], index, cmd.GetArguments()[2]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        if (ret < 0)
        {
            fill_error_reply(ctx.reply, "index out of range");
        }
        else
        {
            fill_status_reply(ctx.reply, "OK");
        }
        return 0;
    }

    int ArdbServer::LTrim(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int start, stop;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], stop))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->LTrim(ctx.currentDB, cmd.GetArguments()[0], start, stop);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::RPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int ret = m_db->RPop(ctx.currentDB, cmd.GetArguments()[0], v);
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
    int ArdbServer::RPush(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int count = 0;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            count = m_db->RPush(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[i]);
            CHECK_ARDB_RETURN_VALUE(ctx.reply, count);
        }
        if (count < 0)
        {
            count = 0;
        }
        fill_int_reply(ctx.reply, count);
        if (count > 0)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            CheckBlockingConnectionsByListKey(key);
        }
        return 0;
    }
    int ArdbServer::RPushx(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->RPushx(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, ret);
        if (ret > 0)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            CheckBlockingConnectionsByListKey(key);
        }
        return 0;
    }

    int ArdbServer::RPopLPush(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int ret = m_db->RPopLPush(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], v);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        if (0 == ret)
        {
            fill_str_reply(ctx.reply, v);
            WatchKey key(ctx.currentDB, cmd.GetArguments()[1]);
            CheckBlockingConnectionsByListKey(key);
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        return 0;
    }

    int ArdbServer::BLPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        std::string v;
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            v.clear();
            if (m_db->LPop(ctx.currentDB, cmd.GetArguments()[i], v) >= 0 && !v.empty())
            {
                ctx.reply.type = REDIS_REPLY_ARRAY;
                ctx.reply.elements.push_back(RedisReply(cmd.GetArguments()[i]));
                ctx.reply.elements.push_back(RedisReply(v));
                return 0;
            }
        }
        LockGuard<ThreadMutex> guard(m_block_mutex);
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            BlockConnContext blockCtx;
            blockCtx.connId = ctx.conn_id;
            blockCtx.ctx = &ctx;
            blockCtx.eventService = &(ctx.conn->GetService());
            WatchKey key(ctx.currentDB, cmd.GetArguments()[i]);
            m_blocking_conns[key].push_back(blockCtx);
            ctx.GetBlockList().keys.insert(key);
        }
        ctx.GetBlockList().pop_type = BLOCK_LIST_LPOP;
        BlockConn(ctx, timeout);
        return 0;
    }
    int ArdbServer::BRPop(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        std::string v;
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            v.clear();
            if (m_db->RPop(ctx.currentDB, cmd.GetArguments()[i], v) >= 0 && !v.empty())
            {
                ctx.reply.type = REDIS_REPLY_ARRAY;
                ctx.reply.elements.push_back(RedisReply(cmd.GetArguments()[i]));
                ctx.reply.elements.push_back(RedisReply(v));
                return 0;
            }
        }
        LockGuard<ThreadMutex> guard(m_block_mutex);
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            BlockConnContext blockCtx;
            blockCtx.connId = ctx.conn_id;
            blockCtx.ctx = &ctx;
            blockCtx.eventService = &(ctx.conn->GetService());
            WatchKey key(ctx.currentDB, cmd.GetArguments()[i]);
            m_blocking_conns[key].push_back(blockCtx);
            ctx.GetBlockList().keys.insert(key);
        }
        ctx.GetBlockList().pop_type = BLOCK_LIST_RPOP;
        BlockConn(ctx, timeout);
        return 0;
    }
    int ArdbServer::BRPopLPush(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        std::string v;
        if (0 == m_db->RPopLPush(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], v) && !v.empty())
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
            ctx.reply.elements.push_back(RedisReply(cmd.GetArguments()[1]));
            ctx.reply.elements.push_back(RedisReply(v));
        }
        else
        {
            LockGuard<ThreadMutex> guard(m_block_mutex);
            ctx.GetBlockList().pop_type = BLOCK_LIST_RPOP;
            BlockConnContext blockCtx;
            blockCtx.connId = ctx.conn_id;
            blockCtx.ctx = &ctx;
            blockCtx.eventService = &(ctx.conn->GetService());
            WatchKey key(ctx.currentDB, cmd.GetArguments()[0]);
            m_blocking_conns[key].push_back(blockCtx);
            ctx.GetBlockList().keys.insert(key);
            ctx.GetBlockList().dest_key = cmd.GetArguments()[1];
            BlockConn(ctx, timeout);
        }
        return 0;
    }

    int ArdbServer::LClear(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->LClear(ctx.currentDB, cmd.GetArguments()[0]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    ListMetaValue* Ardb::GetListMeta(const DBID& db, const Slice& key, int& err, bool& created)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != LIST_META)
        {
            DELETE(meta);
            err = ERR_INVALID_TYPE;
            return NULL;
        }
        if (NULL == meta)
        {
            meta = new ListMetaValue;
            err = ERR_NOT_EXIST;
            created = true;
        }
        else
        {
            created = false;
        }
        return (ListMetaValue*) meta;
    }

    int Ardb::ListPush(const DBID& db, const Slice& key, const Slice& value, bool athead, bool onlyexist)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* lmeta = GetListMeta(db, key, err, createList);
        if (NULL == lmeta)
        {
            return err;
        }
        if (createList && onlyexist)
        {
            DELETE(lmeta);
            return ERR_NOT_EXIST;
        }
        bool zip_save = lmeta->ziped;
        CommonValueObject lv;
        lv.data.SetValue(value, true);
        if (zip_save)
        {
            if (lv.data.type == BYTES_VALUE && lv.data.bytes_value.size() >= (uint32) m_config.list_max_ziplist_value)
            {
                zip_save = false;
            }
            if (lmeta->zipvs.size() >= (uint32) m_config.list_max_ziplist_entries)
            {
                zip_save = false;
            }
        }
        /*
         * save to ziplist
         */
        if (zip_save)
        {
            if (athead)
            {
                lmeta->zipvs.push_front(lv.data);
            }
            else
            {
                lmeta->zipvs.push_back(lv.data);
            }
            lmeta->size++;
            err = SetMeta(db, key, *lmeta);
            if (err == 0)
            {
                err = lmeta->size;
            }
            DELETE(lmeta);
            return err;
        }

        /*
         * Convert from ziplist and save new element
         */
        if (lmeta->ziped)
        {
            BatchWriteGuard guard(GetEngine());
            lmeta->ziped = false;
            ValueDataDeque::iterator it = lmeta->zipvs.begin();
            int64 score = 0;
            while (it != lmeta->zipvs.end())
            {
                ListKeyObject lk(key, score, db);
                CommonValueObject element(*it);
                SetKeyValueObject(lk, element);
                score++;
                it++;
            }
            lmeta->min_score.SetIntValue(0);
            lmeta->max_score.SetIntValue(score - 1 >= 0 ? score : 0);
            lmeta->size = lmeta->zipvs.size() + 1;
            if (athead)
            {
                score = -1;
                lmeta->min_score.SetIntValue(-1);
            }
            else
            {
                lmeta->max_score.SetIntValue(score);
            }
            ListKeyObject lk(key, score, db);
            SetKeyValueObject(lk, lv);
            lmeta->zipvs.clear();
            err = SetMeta(db, key, *lmeta);
            if (err == 0)
            {
                err = lmeta->size;
            }
            DELETE(lmeta);
            return err;
        }

        /*
         * save element
         */
        int64 score = 0;
        if (!createList)
        {
            lmeta->size++;
            if (athead)
            {
                lmeta->min_score.Incrby(-1);
                score = (int64) (lmeta->min_score.NumberValue());
            }
            else
            {
                lmeta->max_score.Incrby(1);
                score = (int64) (lmeta->max_score.NumberValue());
            }
        }
        else
        {
            lmeta->size++;
            score = 0;
        }
        BatchWriteGuard guard(GetEngine());
        ListKeyObject lk(key, score, db);
        if (0 == SetKeyValueObject(lk, lv))
        {
            int ret = SetMeta(db, key, *lmeta);
            if (ret == 0)
            {
                ret = lmeta->size;
            }
            DELETE(lmeta);
            return ret;
        }
        DELETE(lmeta);
        return -1;
    }
    int Ardb::RPush(const DBID& db, const Slice& key, const Slice& value)
    {
        return ListPush(db, key, value, false, false);
    }

    int Ardb::RPushx(const DBID& db, const Slice& key, const Slice& value)
    {
        int len = ListPush(db, key, value, false, true);
        return len < 0 ? 0 : len;
    }

    int Ardb::LPush(const DBID& db, const Slice& key, const Slice& value)
    {
        return ListPush(db, key, value, true, false);
    }

    int Ardb::LPushx(const DBID& db, const Slice& key, const Slice& value)
    {
        int len = ListPush(db, key, value, true, true);
        return len < 0 ? 0 : len;
    }

    int Ardb::LInsert(const DBID& db, const Slice& key, const Slice& opstr, const Slice& pivot, const Slice& value)
    {
        bool before;
        if (!strncasecmp(opstr.data(), "before", opstr.size()))
            before = true;
        else if (!strncasecmp(opstr.data(), "after", opstr.size()))
            before = false;
        else
        {
            return ERR_INVALID_OPERATION;
        }
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        CommonValueObject element(value);
        if (meta->ziped)
        {
            ValueData cmp(pivot);
            ValueDataDeque::iterator it = meta->zipvs.begin();
            bool found = false;
            while (it != meta->zipvs.end())
            {
                if (cmp.Compare(*it) == 0)
                {
                    found = true;
                    if (before)
                    {
                        break;
                    }
                    else
                    {
                        it++;
                        break;
                    }
                }
                it++;
            }
            if (found)
            {
                if (it != meta->zipvs.end())
                {
                    meta->zipvs.insert(it, element.data);
                }
                else
                {
                    meta->zipvs.push_back(element.data);
                }
                meta->size++;
                bool convert_to_nonzip = false;
                if (element.data.type == BYTES_VALUE
                        && element.data.bytes_value.size() >= (uint32) m_config.list_max_ziplist_value)
                {
                    convert_to_nonzip = true;
                }
                if (meta->zipvs.size() >= (uint32) m_config.list_max_ziplist_entries)
                {
                    convert_to_nonzip = true;
                }
                BatchWriteGuard guard(GetEngine());
                if (convert_to_nonzip)
                {
                    int64 score = 0;
                    ValueDataDeque::iterator it = meta->zipvs.begin();
                    while (it != meta->zipvs.end())
                    {
                        ListKeyObject lk(key, score, db);
                        CommonValueObject element(*it);
                        SetKeyValueObject(lk, element);
                        score++;
                        it++;
                    }
                    meta->min_score.SetIntValue(0);
                    meta->max_score.SetIntValue(score - 1 >= 0 ? score : 0);
                    meta->zipvs.clear();
                }
                SetMeta(db, key, *meta);
            }
            DELETE(meta);
            return found ? 0 : ERR_NOT_EXIST;
        }
        ListKeyObject lk(key, -FLT_MAX, db);
        struct LInsertWalk: public WalkHandler
        {
                bool before_pivot;
                ValueData pivot_score;
                ValueData next_score;
                bool found;
                ValueData cmp_value;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    CommonValueObject* cv = (CommonValueObject*) v;
                    ListKeyObject* sek = (ListKeyObject*) k;
                    if (found && !before_pivot)
                    {
                        next_score = sek->score;
                        return -1;
                    }
                    if (cv->data.Compare(cmp_value) == 0)
                    {
                        pivot_score = sek->score;
                        found = true;
                        if (before_pivot)
                        {
                            return -1;
                        }
                    }
                    else
                    {
                        if (before_pivot)
                        {
                            next_score = sek->score;
                        }
                    }
                    return 0;
                }
                LInsertWalk(bool flag, const Slice& value) :
                        before_pivot(flag), pivot_score(-FLT_MAX), next_score(-FLT_MAX), found(false)
                {
                    cmp_value.SetValue(value, true);
                }
        } walk(before, pivot);
        Walk(lk, false, true, &walk);
        if (!walk.found)
        {
            DELETE(meta);
            return ERR_NOT_EXIST;
        }
        ValueData score((int64) 0);
        if (walk.next_score.type != EMPTY_VALUE)
        {
            double sv = (walk.next_score.NumberValue() + walk.pivot_score.NumberValue()) / 2;
            if ((int64) sv < walk.next_score.NumberValue() && (int64) sv < walk.pivot_score.NumberValue())
            {
                score.SetIntValue((int64) sv);
            }
            else
            {
                score.SetDoubleValue(sv);
            }
        }
        else
        {
            if (before)
            {
                walk.pivot_score.Incrby(-1);
                score.SetIntValue((int64) walk.pivot_score.NumberValue());
            }
            else
            {
                walk.pivot_score.Incrby(1);
                score.SetIntValue((int64) walk.pivot_score.NumberValue());
            }
        }
        BatchWriteGuard guard(GetEngine());
        ListKeyObject nlk(key, score, db);
        SetKeyValueObject(nlk, element);
        meta->size++;
        SetMeta(db, key, *meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::ListPop(const DBID& db, const Slice& key, bool athead, std::string& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            meta->size--;
            if (athead)
            {
                meta->zipvs.front().ToString(value);
                meta->zipvs.pop_front();
            }
            else
            {
                meta->zipvs.back().ToString(value);
                meta->zipvs.pop_back();
            }
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 0;
        }

        ValueData score;
        if (meta->size <= 0)
        {
            DELETE(meta);
            return ERR_NOT_EXIST;
        }
        meta->size--;
        if (athead)
        {
            score = meta->min_score;
        }
        else
        {
            score = meta->max_score;
        }
        if (meta->size == 0)
        {
            meta->min_score.SetIntValue(0);
            meta->max_score.SetIntValue(0);
        }
        ListKeyObject lk(key, score, db);
        BatchWriteGuard guard(GetEngine());
        struct LPopWalk: public WalkHandler
        {
                Ardb* ldb;
                std::string& pop_value;
                ListMetaValue& lmeta;
                bool reverse;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    CommonValueObject* cv = (CommonValueObject*) v;
                    if (cursor == 0)
                    {
                        ListKeyObject* lk = (ListKeyObject*) k;
                        cv->data.ToString(pop_value);
                        ldb->DelValue(*lk);
                        return 0;
                    }
                    else
                    {
                        ListKeyObject* lk = (ListKeyObject*) k;
                        if (reverse)
                        {
                            lmeta.max_score = lk->score;
                        }
                        else
                        {
                            lmeta.min_score = lk->score;
                        }
                        return -1;
                    }
                }
                LPopWalk(Ardb* db, std::string& v, ListMetaValue& meta, bool r) :
                        ldb(db), pop_value(v), lmeta(meta), reverse(r)
                {
                }
        } walk(this, value, *meta, !athead);
        Walk(lk, !athead, true, &walk);
        int ret = SetMeta(db, key, *meta);
        DELETE(meta);
        return ret;
    }

    int Ardb::LPop(const DBID& db, const Slice& key, std::string& value)
    {
        return ListPop(db, key, true, value);
    }
    int Ardb::RPop(const DBID& db, const Slice& key, std::string& v)
    {
        return ListPop(db, key, false, v);
    }

    int Ardb::LIndex(const DBID& db, const Slice& key, int index, std::string& v)
    {
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        err = 0;
        if (meta->ziped)
        {
            if (index > 0 && meta->zipvs.size() < (uint32) index)
            {
                err = ERR_NOT_EXIST;
            }
            if (index < 0)
            {
                if (meta->zipvs.size() < (uint32) (0 - index))
                {
                    err = ERR_NOT_EXIST;
                }
                else
                {
                    index = meta->zipvs.size() + index;
                }
            }
            if (0 == err)
            {
                meta->zipvs[index].ToString(v);
            }
            DELETE(meta);
            return err;
        }
        DELETE(meta);
        ListKeyObject lk(key, -FLT_MAX, db);
        bool reverse = false;
        if (index < 0)
        {
            index = 0 - index;
            lk.score = FLT_MAX;
            reverse = true;
        }
        struct LIndexWalk: public WalkHandler
        {
                uint32 lcursor;
                uint32 index;
                std::string& found_value;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    CommonValueObject* cv = (CommonValueObject*) v;
                    if (cursor == index)
                    {
                        cv->data.ToString(found_value);
                        return -1;
                    }
                    lcursor++;
                    return 0;
                }
                LIndexWalk(uint32 i, std::string& v) :
                        lcursor(0), index(i), found_value(v)
                {
                }
        } walk((uint32) index, v);
        Walk(lk, reverse, true, &walk);
        if (walk.lcursor == walk.index)
        {
            return 0;
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::LRange(const DBID& db, const Slice& key, int start, int end, ValueDataArray& values)
    {
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList)
        {
            DELETE(meta);
            return err;
        }
        int len = meta->size;
        if (start < 0)
        {
            start += len;
        }
        if (end < 0)
        {
            end += len;
        }
        if (start < 0)
        {
            start = 0;
        }
        if (start >= len)
        {
            return 0;
        }
        if (end < 0)
        {
            end = 0;
        }
        if (meta->ziped)
        {
            for (uint32 cursor = start; cursor < meta->zipvs.size() && cursor < (uint32) end; cursor++)
            {
                values.push_back(meta->zipvs[cursor]);
            }
            DELETE(meta);
            return 0;
        }

        ListKeyObject lk(key, meta->min_score, db);
        struct LRangeWalk: public WalkHandler
        {
                uint32 l_start;
                uint32 l_stop;
                ValueDataArray& found_values;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    CommonValueObject* cv = (CommonValueObject*) v;
                    if (cursor >= l_start && cursor <= l_stop)
                    {
                        found_values.push_back(cv->data);
                    }
                    if (cursor >= l_stop)
                    {
                        return -1;
                    }
                    return 0;
                }
                LRangeWalk(int start, int stop, ValueDataArray& vs) :
                        l_start(start), l_stop(stop), found_values(vs)
                {
                }
        } walk(start, end, values);
        Walk(lk, false, true, &walk);
        DELETE(meta);
        return 0;
    }

    int Ardb::LClear(const DBID& db, const Slice& key)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        if (!meta->ziped)
        {
            ListKeyObject lk(key, -FLT_MAX, db);
            struct LClearWalk: public WalkHandler
            {
                    Ardb* z_db;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        ListKeyObject* sek = (ListKeyObject*) k;
                        z_db->DelValue(*sek);
                        return 0;
                    }
                    LClearWalk(Ardb* db) :
                            z_db(db)
                    {
                    }
            } walk(this);
            Walk(lk, false, false, &walk);
        }
        DelMeta(db, key, meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::LRem(const DBID& db, const Slice& key, int count, const Slice& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        ListKeyObject lk(key, meta->min_score, db);
        int total = count;
        bool fromhead = true;
        if (count < 0)
        {
            fromhead = false;
            total = 0 - count;
            lk.score = meta->max_score;
        }

        if (meta->ziped)
        {
            ValueData element(value);
            ValueDataDeque::iterator it = meta->zipvs.begin();
            ValueDataDeque new_zip_vs;

            while (it != meta->zipvs.end())
            {
                if (element.Compare(*it) != 0)
                {
                    new_zip_vs.push_back(*it);
                }
                else
                {
                    total--;
                }
                if (total == 0)
                {
                    break;
                }
                it++;
            }
            int ret = 0;
            if (new_zip_vs.size() != meta->zipvs.size())
            {
                ret = meta->zipvs.size() - new_zip_vs.size();
                meta->zipvs = new_zip_vs;
                meta->size = new_zip_vs.size();
                SetMeta(db, key, *meta);
            }
            DELETE(meta);
            return ret;
        }

        struct LRemWalk: public WalkHandler
        {
                Ardb* z_db;
                ValueData cmp_value;
                int remcount;
                int rem_total;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    CommonValueObject* cv = (CommonValueObject*) v;
                    ListKeyObject* sek = (ListKeyObject*) k;
                    if (cv->data.Compare(cmp_value) == 0)
                    {
                        z_db->DelValue(*sek);
                        remcount++;
                        if (rem_total == remcount)
                        {
                            return -1;
                        }
                    }
                    return 0;
                }
                LRemWalk(Ardb* db, const Slice& v, int total) :
                        z_db(db), remcount(0), rem_total(total)
                {
                    cmp_value.SetValue(v, true);
                }
        } walk(this, value, total);
        BatchWriteGuard guard(GetEngine());
        Walk(lk, !fromhead, true, &walk);
        meta->size -= walk.remcount;
        SetMeta(db, key, *meta);
        DELETE(meta);
        return walk.remcount;
    }

    int Ardb::LSet(const DBID& db, const Slice& key, int index, const Slice& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        if (index < 0)
        {
            index += meta->size;
        }
        if ((uint32) index >= meta->size)
        {
            DELETE(meta);
            return ERR_NOT_EXIST;
        }
        if (meta->ziped)
        {
            meta->zipvs[index].SetValue(value, true);
            SetMeta(db, key, *meta);
            DELETE(meta);
            return 0;
        }
        DELETE(meta);
        ListKeyObject lk(key, -FLT_MAX, db);
        struct LSetWalk: public WalkHandler
        {
                Ardb* z_db;
                const Slice& set_value;
                uint32 lcursor;
                uint32 dst_idx;
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ListKeyObject* sek = (ListKeyObject*) k;
                    if (cursor == dst_idx)
                    {
                        CommonValueObject cv;
                        cv.data.SetValue(set_value, true);
                        z_db->SetKeyValueObject(*sek, cv);
                        return -1;
                    }
                    lcursor++;
                    return 0;
                }
                LSetWalk(Ardb* db, const Slice& v, uint32 index) :
                        z_db(db), set_value(v), lcursor(0), dst_idx(index)
                {
                }
        } walk(this, value, (uint32) index);
        Walk(lk, false, false, &walk);
        if (walk.lcursor != walk.dst_idx)
        {
            return ERR_NOT_EXIST;
        }
        return 0;
    }

    int Ardb::LTrim(const DBID& db, const Slice& key, int start, int stop)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        int err = 0;
        bool createList = false;
        ListMetaValue* meta = GetListMeta(db, key, err, createList);
        if (NULL == meta || createList || meta->size == 0)
        {
            DELETE(meta);
            return err;
        }
        int len = meta->size;
        if (len <= 0)
        {
            DELETE(meta);
            return 0;
        }
        if (start < 0)
        {
            start += len;
        }
        if (stop < 0)
        {
            stop += len;
        }
        if (start < 0)
        {
            start = 0;
        }
        if (stop < 0)
        {
            stop = 0;
        }
        if (start >= len)
        {
            DELETE(meta);
            return LClear(db, key);
        }
        if (meta->ziped)
        {
            ValueDataDeque new_zip_vs;
            for (uint32 i = start; i < meta->zipvs.size() && i <= (uint32) stop; i++)
            {
                new_zip_vs.push_back(meta->zipvs[i]);
            }
            if (new_zip_vs.size() != meta->zipvs.size())
            {
                meta->zipvs = new_zip_vs;
                meta->size = new_zip_vs.size();
                SetMeta(db, key, *meta);
            }
            DELETE(meta);
            return 0;
        }

        ListKeyObject lk(key, meta->min_score, db);
        struct LTrimWalk: public WalkHandler
        {
                Ardb* ldb;
                uint32 lstart, lstop;
                ListMetaValue& lmeta;
                LTrimWalk(Ardb* db, int start, int stop, ListMetaValue& meta) :
                        ldb(db), lstart(start), lstop(stop), lmeta(meta)
                {
                }
                int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                {
                    ListKeyObject* lek = (ListKeyObject*) k;
                    if (cursor >= lstart && cursor <= lstop)
                    {
                        if (COMPARE_NUMBER(lek->score.NumberValue(), lmeta.min_score.NumberValue()) < 0)
                        {
                            lmeta.min_score = lek->score;
                        }
                        if (COMPARE_NUMBER(lek->score.NumberValue(), lmeta.max_score.NumberValue()) > 0)
                        {
                            lmeta.max_score = lek->score;
                        }
                        return 0;
                    }
                    else
                    {
                        lmeta.size--;
                        ldb->DelValue(*lek);
                    }
                    return 0;
                }
        } walk(this, start, stop, *meta);
        BatchWriteGuard guard(GetEngine());
        Walk(lk, false, false, &walk);
        SetMeta(db, key, *meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::LLen(const DBID& db, const Slice& key)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != LIST_META)
        {
            DELETE(meta);
            return ERR_INVALID_TYPE;
        }
        ListMetaValue* lmeta = (ListMetaValue*) meta;
        int size = lmeta->size;
        DELETE(meta);
        return size;
    }

    int Ardb::RPopLPush(const DBID& db, const Slice& key1, const Slice& key2, std::string& v)
    {
        if (0 == RPop(db, key1, v))
        {
            Slice sv(v.c_str(), v.size());
            return RPush(db, key2, sv);
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::RenameList(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, ListMetaValue* meta)
    {
        BatchWriteGuard guard(GetEngine());
        if (meta->ziped)
        {
            DelMeta(db1, key1, meta);
            SetMeta(db2, key2, *meta);
        }
        else
        {
            ListKeyObject lk(key1, meta->min_score, db1);
            ListMetaValue lmeta;
            lmeta.ziped = false;
            struct LSetWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    ListMetaValue& lm;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        CommonValueObject* cv = (CommonValueObject*) v;
                        ListKeyObject* sek = (ListKeyObject*) k;
                        sek->key = dst;
                        sek->db = dstdb;
                        z_db->SetKeyValueObject(*sek, *cv);
                        lm.size++;
                        return 0;
                    }
                    LSetWalk(Ardb* db, const DBID& dbid, const Slice& v, ListMetaValue& l) :
                            z_db(db), dstdb(dbid), dst(v), lm(l)
                    {
                    }
            } walk(this, db2, key2, lmeta);
            Walk(lk, false, true, &walk);
            SetMeta(db2, key2, lmeta);
            LClear(db1, key1);
        }
        return 0;
    }
}


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
#include "thread/thread_mutex.hpp"

namespace ardb
{
    int Ardb::Multi(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (ctx.InTransaction())
        {
            reply.SetErrorReason("MULTI calls can not be nested");
            return 0;
        }
        ctx.GetTransaction().started = true;
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::Discard(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (!ctx.InTransaction())
        {
            reply.SetErrorReason("DISCARD without MULTI");
            return 0;
        }
        DiscardTransaction(ctx);
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::Exec(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (!ctx.InTransaction())
        {
            reply.SetErrorReason("EXEC without MULTI");
        }
        else if (ctx.GetTransaction().abort || ctx.GetTransaction().cas)
        {
            if (ctx.GetTransaction().abort)
            {
                reply.SetErrCode(ERR_EXEC_ABORT);
            }
            else
            {
                reply.ReserveMember(-1);
            }
            DiscardTransaction(ctx);
        }
        else
        {
            UnwatchKeys(ctx);
            RedisCommandFrameArray::iterator it = ctx.GetTransaction().cached_cmds.begin();
            Context transc_ctx;
            transc_ctx.ns = ctx.ns;
            while (it != ctx.GetTransaction().cached_cmds.end())
            {
                RedisReply& r = reply.AddMember();
                RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(*it);
                if (NULL != setting)
                {
                    transc_ctx.GetReply().Clear();
                    DoCall(transc_ctx, *setting, *it);
                    r.Clone(transc_ctx.GetReply());
                }
                else
                {
                    r.SetErrorReason("unknown command");
                }
                it++;
            }
            ctx.ns = transc_ctx.ns;
            DiscardTransaction(ctx);
        }
        /* Send EXEC to clients waiting data from MONITOR. We do it here
         * since the natural order of commands execution is actually:
         * MUTLI, EXEC, ... commands inside transaction ...
         * Instead EXEC is flagged as CMD_SKIP_MONITOR in the command
         * table, and we do it here with correct ordering. */
        if (NULL != m_monitors && !IsLoadingData())
        {
            FeedMonitors(ctx, ctx.ns, cmd);
        }
        return 0;
    }

    int Ardb::DiscardTransaction(Context& ctx)
    {
        UnwatchKeys(ctx);
        ctx.ClearTransaction();
        return 0;
    }

    int Ardb::TouchWatchedKeysOnFlush(Context& ctx, const Data& ns)
    {
        if (NULL == m_watched_ctxs)
        {
            return 0;
        }
        LockGuard<SpinMutexLock> guard(m_watched_keys_lock);
        if (!m_watched_ctxs->empty())
        {
            WatchedContextTable::iterator wit = m_watched_ctxs->begin();
            while (wit != m_watched_ctxs->end())
            {
                if (ns.IsNil() || wit->first.ns == ns)
                {
                    ContextSet::iterator cit = wit->second.begin();
                    while (cit != wit->second.end())
                    {
                        Context* watch_ctx = *cit;
                        watch_ctx->GetTransaction().cas = true;
                        cit++;
                    }
                }
                wit++;
            }
        }
        return 0;
    }

    int Ardb::TouchWatchKey(Context& ctx, const KeyObject& key)
    {
        if (NULL == m_watched_ctxs)
        {
            return 0;
        }
        LockGuard<SpinMutexLock> guard(m_watched_keys_lock);
        if (NULL != m_watched_ctxs && !m_watched_ctxs->empty())
        {
            KeyPrefix prefix;
            prefix.ns = key.GetNameSpace();
            prefix.key = key.GetKey();
            WatchedContextTable::iterator found = m_watched_ctxs->find(prefix);
            if (found != m_watched_ctxs->end())
            {
                ContextSet::iterator cit = found->second.begin();
                while (cit != found->second.end())
                {
                    Context* watch_ctx = *cit;
                    watch_ctx->GetTransaction().cas = true;
                    cit++;
                }
            }
        }
        return 0;
    }

    int Ardb::WatchForKey(Context& ctx, const std::string& key)
    {
        LockGuard<SpinMutexLock> guard(m_watched_keys_lock);
        if (NULL == m_watched_ctxs)
        {
            NEW(m_watched_ctxs, WatchedContextTable);
        }
        KeyPrefix prefix;
        prefix.ns = ctx.ns;
        prefix.key.SetString(key, false);
        (*m_watched_ctxs)[prefix].insert(&ctx);
        ctx.GetTransaction().watched_keys.insert(prefix);
        return 0;
    }

    int Ardb::UnwatchKeys(Context& ctx)
    {
        if (NULL == m_watched_ctxs)
        {
            return 0;
        }
        LockGuard<SpinMutexLock> guard(m_watched_keys_lock);
        if (NULL != m_watched_ctxs && ctx.transc != NULL && !m_watched_ctxs->empty())
        {
            TransactionContext::WatchKeySet::iterator it = ctx.GetTransaction().watched_keys.begin();
            while (it != ctx.GetTransaction().watched_keys.end())
            {
                const KeyPrefix& prefix = *it;
                WatchedContextTable::iterator fit = m_watched_ctxs->find(prefix);
                if (fit != m_watched_ctxs->end())
                {
                    ContextSet& cset = fit->second;
                    cset.erase(&ctx);
                    if (cset.empty())
                    {
                        m_watched_ctxs->erase(fit);
                    }
                }
                else
                {
                    WARN_LOG("No found in global watch contexts");
                }
                it++;
            }
        }
        if (NULL != m_watched_ctxs && m_watched_ctxs->empty())
        {
            DELETE(m_watched_ctxs);
        }
        return 0;
    }

    int Ardb::Watch(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (ctx.InTransaction())
        {
            reply.SetErrorReason("WATCH inside MULTI is not allowed");
            return 0;
        }
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            WatchForKey(ctx, cmd.GetArguments()[i]);
        }
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }
    int Ardb::UnWatch(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetStatusCode(STATUS_OK);
        UnwatchKeys(ctx);
        return 0;
    }
}


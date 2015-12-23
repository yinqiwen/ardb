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
    static ThreadMutex g_transc_mutex;

    int Ardb::Multi(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (ctx.InTransaction())
        {
            reply.SetErrorReason("MULTI calls can not be nested");
            return 0;
        }
        ctx.GetTransaction();
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
        ctx.ClearTransaction();
        reply.SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::Exec(Context& ctx, RedisCommandFrame& cmd)
    {
//        if (!ctx.InTransc())
//        {
//            fill_error_reply(ctx.reply, "EXEC without MULTI");
//            return 0;
//        }
//        if (ctx.GetTransc().abort)
//        {
//            ctx.reply.type = REDIS_REPLY_NIL;
//            ctx.ClearTransc();
//            return 0;
//        }
//        LockGuard<ThreadMutex> guard(g_transc_mutex); //only one transc allowed exec at the same time in multi threads
//        RedisCommandFrameArray::iterator it = ctx.GetTransc().cached_cmds.begin();
//        Context transc_ctx;
//        transc_ctx.currentDB = ctx.currentDB;
//        while (it != ctx.GetTransc().cached_cmds.end())
//        {
//            RedisReply& r = ctx.reply.AddMember();
//            RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(*it);
//            if(NULL != setting)
//            {
//                transc_ctx.reply.Clear();
//                DoCall(transc_ctx, *setting, *it);
//                r.Clone(transc_ctx.reply);
//            }
//            else
//            {
//                fill_error_reply(r, "unknown command '%s'", it->GetCommand().c_str());
//            }
//            it++;
//        }
//        ctx.currentDB = transc_ctx.currentDB;
//        ctx.ClearTransc();
//        UnwatchKeys(ctx);
        return 0;
    }

    int Ardb::AbortWatchKey(Context& ctx, const std::string& key)
    {
//        if(NULL != m_watched_ctx)
//        {
//            DBItemKey k(ctx.currentDB, key);
//            WatchedContextTable::iterator found = m_watched_ctx->find(k);
//            if(found != m_watched_ctx->end())
//            {
//                ContextSet::iterator cit = found->second.begin();
//                while(cit != found->second.end())
//                {
//                    (*cit)->GetTransc().abort = true;
//                    cit++;
//                }
//            }
//        }
        return 0;
    }

    int Ardb::WatchForKey(Context& ctx, const std::string& key)
    {
//        WriteLockGuard<SpinRWLock> guard(m_watched_keys_lock);
//        if (NULL == m_watched_ctx)
//        {
//            m_watched_ctx = new WatchedContextTable;
//        }
//        DBItemKey k(ctx.currentDB, key);
//        (*m_watched_ctx)[k].insert(&ctx);
//        ctx.GetTransc().watched_keys.insert(k);
        return 0;
    }

    int Ardb::UnwatchKeys(Context& ctx)
    {
//        if(NULL != m_watched_ctx && ctx.transc != NULL)
//        {
//            WriteLockGuard<SpinRWLock> guard(m_watched_keys_lock);
//            WatchKeySet::iterator it = ctx.GetTransc().watched_keys.begin();
//            while(it != ctx.GetTransc().watched_keys.end())
//            {
//                (*m_watched_ctx)[*it].erase(&ctx);
//                if((*m_watched_ctx)[*it].size() == 0)
//                {
//                    m_watched_ctx->erase(*it);
//                }
//                it++;
//            }
//            ctx.ClearTransc();
//            if(m_watched_ctx->empty())
//            {
//                DELETE(m_watched_ctx);
//            }
//        }
        return 0;
    }

    int Ardb::Watch(Context& ctx, RedisCommandFrame& cmd)
    {
//        if (ctx.InTransc())
//        {
//            fill_error_reply(ctx.reply, "WATCH inside MULTI is not allowed");
//            return 0;
//        }
//        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
//        {
//            WatchForKey(ctx, cmd.GetArguments()[i]);
//        }
//        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    int Ardb::UnWatch(Context& ctx, RedisCommandFrame& cmd)
    {
//        ctx.GetReply().SetStatusCode(STATUS_OK);
//        UnwatchKeys(ctx);
        return 0;
    }
}


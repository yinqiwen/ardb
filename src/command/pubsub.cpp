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

namespace ardb
{
    int Ardb::SubscribeChannel(Context& ctx, const std::string& channel, bool notify)
    {
//        ctx.GetPubsub().pubsub_channels.insert(channel);
//        {
//            WriteLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
//            m_pubsub_channels[channel].insert(&ctx);
//        }
//
//        if (notify && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "subscribe");
//            fill_str_reply(r2, channel);
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
        return 0;
    }
    int Ardb::UnsubscribeChannel(Context& ctx, const std::string& channel, bool notify)
    {
//        ctx.GetPubsub().pubsub_channels.erase(channel);
//        WriteLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
//        PubsubContextTable::iterator it = m_pubsub_channels.find(channel);
//        int ret = 0;
//        if (it != m_pubsub_channels.end())
//        {
//            it->second.erase(&ctx);
//            if (it->second.empty())
//            {
//                m_pubsub_channels.erase(it);
//            }
//            ret = 1;
//        }
//        if (notify && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "unsubscribe");
//            fill_str_reply(r2, channel);
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
//        return ret;
    }

    int Ardb::UnsubscribeAll(Context& ctx, bool notify)
    {
//        if(NULL == ctx.pubsub)
//        {
//            return 0;
//        }
//        StringSet tmp = ctx.GetPubsub().pubsub_channels;
//        StringSet::iterator it = tmp.begin();
//        int count = 0;
//        while (it != tmp.end())
//        {
//            count += UnsubscribeChannel(ctx, *it, notify);
//            it++;
//        }
//        if (notify && count == 0 && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "unsubscribe");
//            r2.type = REDIS_REPLY_NIL;
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
        return 0;
    }

    int Ardb::PSubscribeChannel(Context& ctx, const std::string& pattern, bool notify)
    {
//        ctx.GetPubsub().pubsub_patterns.insert(pattern);
//        {
//            WriteLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
//            m_pubsub_patterns[pattern].insert(&ctx);
//        }
//
//        if (notify && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "psubscribe");
//            fill_str_reply(r2, pattern);
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
        return 0;
    }
    int Ardb::PUnsubscribeChannel(Context& ctx, const std::string& pattern, bool notify)
    {
//        ctx.GetPubsub().pubsub_patterns.erase(pattern);
//        WriteLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
//        PubsubContextTable::iterator it = m_pubsub_patterns.find(pattern);
//        int ret = 0;
//        if (it != m_pubsub_patterns.end())
//        {
//            it->second.erase(&ctx);
//            if (it->second.empty())
//            {
//                m_pubsub_patterns.erase(it);
//            }
//            ret = 1;
//        }
//        if (notify && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "punsubscribe");
//            fill_str_reply(r2, pattern);
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
//        return ret;
    }

    int Ardb::Subscribe(Context& ctx, RedisCommandFrame& cmd)
    {
//        ctx.client->GetWritableOptions().max_write_buffer_size = (int32)(m_cfg.pubsub_client_output_buffer_limit);
//        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
//        {
//            SubscribeChannel(ctx, cmd.GetArguments()[i], true);
//        }
        return 0;
    }

    int Ardb::UnSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() == 0)
        {
            UnsubscribeAll(ctx, true);
        }
        else
        {
            for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
            {
                UnsubscribeChannel(ctx, cmd.GetArguments()[i], true);
            }
        }
        return 0;
    }
    int Ardb::PSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
//        ctx.client->GetWritableOptions().max_write_buffer_size = (int32)(m_cfg.pubsub_client_output_buffer_limit);
//        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
//        {
//            PSubscribeChannel(ctx, cmd.GetArguments()[i], true);
//        }
        return 0;
    }
    int Ardb::PUnSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() == 0)
        {
            PUnsubscribeAll(ctx, true);
        }
        else
        {
            for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
            {
                PUnsubscribeChannel(ctx, cmd.GetArguments()[i], true);
            }
        }
        return 0;
    }

    int Ardb::PUnsubscribeAll(Context& ctx, bool notify)
    {
//        if(NULL == ctx.pubsub)
//        {
//            return 0;
//        }
//        StringSet tmp = ctx.GetPubsub().pubsub_patterns;
//        StringSet::iterator it = tmp.begin();
//        int count = 0;
//        while (it != tmp.end())
//        {
//            count += PUnsubscribeChannel(ctx, *it, notify);
//            it++;
//        }
//        if (notify && count == 0 && NULL != ctx.client)
//        {
//            RedisReply r;
//            RedisReply& r1 = r.AddMember();
//            RedisReply& r2 = r.AddMember();
//            RedisReply& r3 = r.AddMember();
//            fill_str_reply(r1, "punsubscribe");
//            r2.type = REDIS_REPLY_NIL;
//            fill_int_reply(r3, ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size());
//            ctx.client->Write(r);
//        }
        return 0;
    }

    static void async_write_message(Channel* ch, void * data)
    {
        RedisReply* r = (RedisReply*) data;
        if(!ch->Write(*r))
        {
            ch->Close();
        }
        DELETE(r);
    }

    int Ardb::PublishMessage(Context& ctx, const std::string& channel, const std::string& message)
    {
//        ReadLockGuard<SpinRWLock> guard(m_pubsub_ctx_lock);
//        PubsubContextTable::iterator fit = m_pubsub_channels.find(channel);
//
//        int receiver = 0;
//        if (fit != m_pubsub_channels.end())
//        {
//            ContextSet::iterator cit = fit->second.begin();
//            while (cit != fit->second.end())
//            {
//                Context* cc = *cit;
//                if (NULL != cc && cc->client != NULL)
//                {
//                    RedisReply* r = NULL;
//                    NEW(r, RedisReply);
//                    RedisReply& r1 = r->AddMember();
//                    RedisReply& r2 = r->AddMember();
//                    RedisReply& r3 = r->AddMember();
//                    fill_str_reply(r1, "message");
//                    fill_str_reply(r2, channel);
//                    fill_str_reply(r3, message);
//                    cc->client->GetService().AsyncIO(cc->client->GetID(), async_write_message, r);
//                    receiver++;
//                }
//                cit++;
//            }
//        }
//        PubsubContextTable::iterator pit = m_pubsub_patterns.begin();
//        while (pit != m_pubsub_patterns.end())
//        {
//            const std::string& pattern = pit->first;
//            if (stringmatchlen(pattern.c_str(), pattern.size(), channel.c_str(), channel.size(), 0))
//            {
//                ContextSet::iterator cit = pit->second.begin();
//                while (cit != pit->second.end())
//                {
//                    Context* cc = *cit;
//                    if (NULL != cc && cc->client != NULL)
//                    {
//                        RedisReply* r = NULL;
//                        NEW(r, RedisReply);
//                        RedisReply& r1 = r->AddMember();
//                        RedisReply& r2 = r->AddMember();
//                        RedisReply& r3 = r->AddMember();
//                        RedisReply& r4 = r->AddMember();
//                        fill_str_reply(r1, "pmessage");
//                        fill_str_reply(r2, pattern);
//                        fill_str_reply(r3, channel);
//                        fill_str_reply(r4, message);
//                        cc->client->GetService().AsyncIO(cc->client->GetID(), async_write_message, r);
//                        receiver++;
//                    }
//                    cit++;
//                }
//            }
//            pit++;
//        }
//        return receiver;
    }

    int Ardb::Publish(Context& ctx, RedisCommandFrame& cmd)
    {
//        const std::string& channel = cmd.GetArguments()[0];
//        const std::string& message = cmd.GetArguments()[1];
//        int count = 0;
//        count += PublishMessage(ctx, channel, message);
//        fill_int_reply(ctx.reply, count);
        return 0;
    }
}


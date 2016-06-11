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
    int Ardb::SubscribeChannel(Context& ctx, const std::string& channel, bool is_pattern)
    {
        if (is_pattern)
        {
            ctx.GetPubsub().pubsub_patterns.insert(channel);
        }
        else
        {
            ctx.GetPubsub().pubsub_channels.insert(channel);
        }
        {
            WriteLockGuard<SpinRWLock> guard(m_pubsub_lock);
            if (is_pattern)
            {
                m_pubsub_patterns[channel].insert(&ctx);
            }
            else
            {
                m_pubsub_channels[channel].insert(&ctx);
            }

        }
        ctx.flags.pubsub = 1;
        RedisReply r;
        RedisReply& r1 = r.AddMember();
        RedisReply& r2 = r.AddMember();
        RedisReply& r3 = r.AddMember();
        r1.SetString(is_pattern ? "psubscribe" : "subscribe");
        r2.SetString(channel);
        r3.SetInteger((int64) (ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size()));
        WriteReply(ctx, &r, false);
        return 0;
    }
    int Ardb::UnsubscribeChannel(Context& ctx, const std::string& channel, bool is_pattern, bool notify)
    {
        if (NULL == ctx.pubsub)
        {
            return 0;
        }
        PubSubChannelTable* tables = NULL;
        if (is_pattern)
        {
            ctx.GetPubsub().pubsub_patterns.erase(channel);
            tables = &m_pubsub_patterns;
        }
        else
        {
            ctx.GetPubsub().pubsub_channels.erase(channel);
            tables = &m_pubsub_channels;
        }
        WriteLockGuard<SpinRWLock> guard(m_pubsub_lock);
        PubSubChannelTable::iterator it = tables->find(channel);
        int ret = 0;
        if (it != tables->end())
        {
            it->second.erase(&ctx);
            if (it->second.empty())
            {
                tables->erase(it);
            }
            ret = 1;
        }
        if (notify)
        {
            RedisReply r;
            RedisReply& r1 = r.AddMember();
            RedisReply& r2 = r.AddMember();
            RedisReply& r3 = r.AddMember();
            r1.SetString(is_pattern ? "punsubscribe" : "unsubscribe");
            r2.SetString(channel);
            r3.SetInteger((int64) (ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size()));
            WriteReply(ctx, &r, false);
        }
        return ret;
    }

    int Ardb::UnsubscribeAll(Context& ctx, bool is_pattern, bool notify)
    {
        if (NULL == ctx.pubsub)
        {
            return 0;
        }
        int count = 0;
        StringTreeSet channels;
        if (is_pattern)
        {
            channels = (ctx.GetPubsub().pubsub_patterns);
        }
        else
        {
            channels = (ctx.GetPubsub().pubsub_channels);
        }
        StringTreeSet::iterator it = channels.begin();
        while (it != channels.end())
        {
            //printf("@@@%s\n", it->c_str());
            count += UnsubscribeChannel(ctx, *it, is_pattern, notify);
            it++;
        }
        if (notify && count == 0)
        {
            RedisReply r;
            RedisReply& r1 = r.AddMember();
            RedisReply& r2 = r.AddMember();
            RedisReply& r3 = r.AddMember();
            r1.SetString(is_pattern ? "punsubscribe" : "unsubscribe");
            r2.Clear();
            r3.SetInteger((int64) (ctx.GetPubsub().pubsub_channels.size() + ctx.GetPubsub().pubsub_patterns.size()));
            WriteReply(ctx, &r, false);
        }
        return 0;
    }

    int Ardb::Subscribe(Context& ctx, RedisCommandFrame& cmd)
    {
//        ctx.client->client->GetWritableOptions().max_write_buffer_size = (int32) (m_cfg.pubsub_client_output_buffer_limit);
        ctx.GetReply().SetEmpty(); //let cleint not write this reply
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            const std::string& channel = cmd.GetArguments()[i];
            SubscribeChannel(ctx, channel, false);
        }
        return 0;
    }

    int Ardb::UnSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.GetReply().type = 0;
        if (cmd.GetArguments().size() == 0)
        {
            UnsubscribeAll(ctx, false, true);
        }
        else
        {
            for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
            {
                UnsubscribeChannel(ctx, cmd.GetArguments()[i], false, true);
            }
        }
        if (ctx.SubscriptionsCount() == 0)
        {
            ctx.ClearPubsub();
            ctx.flags.pubsub = 0;
        }
        return 0;
    }
    int Ardb::PSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
//        ctx.client->GetWritableOptions().max_write_buffer_size = (int32)(m_cfg.pubsub_client_output_buffer_limit);
        ctx.GetReply().SetEmpty(); //let cleint not write this reply
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            const std::string& pattern = cmd.GetArguments()[i];
            SubscribeChannel(ctx, pattern, true);
        }
        return 0;
    }
    int Ardb::PUnSubscribe(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.GetReply().type = 0;
        if (cmd.GetArguments().size() == 0)
        {
            UnsubscribeAll(ctx, true, true);
        }
        else
        {
            for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
            {
                UnsubscribeChannel(ctx, cmd.GetArguments()[i], true, true);
            }
        }
        if (ctx.SubscriptionsCount() == 0)
        {
            ctx.ClearPubsub();
            ctx.flags.pubsub = 0;
        }
        return 0;
    }

    int Ardb::PublishMessage(Context& ctx, const std::string& channel, const std::string& message)
    {
        ReadLockGuard<SpinRWLock> guard(m_pubsub_lock);
        PubSubChannelTable::iterator fit = m_pubsub_channels.find(channel);

        int receiver = 0;
        if (fit != m_pubsub_channels.end())
        {
            ContextSet::iterator cit = fit->second.begin();
            while (cit != fit->second.end())
            {
                Context* cc = *cit;
                if (NULL != cc && cc->client != NULL)
                {
                    RedisReply* r = NULL;
                    NEW(r, RedisReply);
                    RedisReply& r1 = r->AddMember();
                    RedisReply& r2 = r->AddMember();
                    RedisReply& r3 = r->AddMember();
                    r1.SetString("message");
                    r2.SetString(channel);
                    r3.SetString(message);
                    WriteReply(*cc, r, true);
                    receiver++;
                }
                cit++;
            }
        }
        PubSubChannelTable::iterator pit = m_pubsub_patterns.begin();
        while (pit != m_pubsub_patterns.end())
        {
            const std::string& pattern = pit->first;
            if (stringmatchlen(pattern.c_str(), pattern.size(), channel.c_str(), channel.size(), 0))
            {
                ContextSet::iterator cit = pit->second.begin();
                while (cit != pit->second.end())
                {
                    Context* cc = *cit;
                    if (NULL != cc && cc->client != NULL)
                    {
                        RedisReply* r = NULL;
                        NEW(r, RedisReply);
                        RedisReply& r1 = r->AddMember();
                        RedisReply& r2 = r->AddMember();
                        RedisReply& r3 = r->AddMember();
                        RedisReply& r4 = r->AddMember();
                        r1.SetString("pmessage");
                        r2.SetString(pattern);
                        r3.SetString(channel);
                        r4.SetString(message);
                        WriteReply(*cc, r, true);
                        receiver++;
                    }
                    cit++;
                }
            }
            pit++;
        }
        return receiver;
    }

    int Ardb::Publish(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& channel = cmd.GetArguments()[0];
        const std::string& message = cmd.GetArguments()[1];
        int count = 0;
        count += PublishMessage(ctx, channel, message);
        ctx.GetReply().SetInteger(count);
        return 0;
    }

    int Ardb::Pubsub(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& subcommand = cmd.GetArguments()[0];
        if (!strcasecmp(subcommand.c_str(), "channels") && (cmd.GetArguments().size() == 1 || cmd.GetArguments().size() == 2))
        {
            ReadLockGuard<SpinRWLock> guard(m_pubsub_lock);
            reply.ReserveMember(0);
            PubSubChannelTable::iterator fit = m_pubsub_channels.begin();
            while (fit != m_pubsub_channels.end())
            {
                const std::string& channel = fit->first;
                if (cmd.GetArguments().size() == 2)
                {
                    const std::string& pattern = cmd.GetArguments()[1];
                    if (stringmatchlen(pattern.c_str(), pattern.size(), channel.c_str(), channel.size(), 0) != 1)
                    {
                        fit++;
                        continue;
                    }
                }
                RedisReply& rr = reply.AddMember();
                rr.SetString(channel);
                fit++;
            }
        }
        else if (!strcasecmp(subcommand.c_str(), "numsub") && (cmd.GetArguments().size() >= 1))
        {
            ReadLockGuard<SpinRWLock> guard(m_pubsub_lock);
            reply.ReserveMember(0);
            for(size_t i = 1; i < cmd.GetArguments().size(); i++)
            {
                RedisReply& r1 = reply.AddMember();
                RedisReply& r2 = reply.AddMember();
                r1.SetString(cmd.GetArguments()[i]);
                PubSubChannelTable::iterator found = m_pubsub_channels.find(cmd.GetArguments()[i]);
                r2.SetInteger(found == m_pubsub_channels.end()? 0 : found->second.size());
            }
        }
        else if (!strcasecmp(subcommand.c_str(), "numpat") && (cmd.GetArguments().size() == 1))
        {
            ReadLockGuard<SpinRWLock> guard(m_pubsub_lock);
            reply.SetInteger(m_pubsub_patterns.size());
        }
        else
        {
            reply.SetErrorReason("Unknown PUBSUB subcommand or wrong number of arguments for " + cmd.GetArguments()[0]);
        }
        return 0;
    }
}


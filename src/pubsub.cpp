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

#include "ardb_server.hpp"
#include <fnmatch.h>

namespace ardb
{
    int ArdbServer::Subscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);
        ArgumentArray::iterator it = cmd.GetArguments().begin();
        while (it != cmd.GetArguments().end())
        {
            RedisReply r;
            r.type = REDIS_REPLY_ARRAY;
            r.elements.push_back(RedisReply("subscribe"));
            r.elements.push_back(RedisReply(*it));

            ctx.GetPubSub().pubsub_channle_set.insert(*it);
            m_pubsub_context_table[*it].insert(&ctx);
            r.elements.push_back(RedisReply(ctx.SubChannelSize()));
            ctx.conn->Write(r);
            it++;
        }
        return 0;
    }

    void ArdbServer::ClearSubscribes(ArdbConnContext& ctx)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);
        PubSubChannelSet::iterator it = ctx.GetPubSub().pubsub_channle_set.begin();
        while (it != ctx.GetPubSub().pubsub_channle_set.end())
        {
            PubSubContextTable::iterator found = m_pubsub_context_table.find(*it);
            if (found != m_pubsub_context_table.end())
            {
                found->second.erase(&ctx);
            }
            it++;
        }
        it = ctx.GetPubSub().pattern_pubsub_channle_set.begin();
        while (it != ctx.GetPubSub().pattern_pubsub_channle_set.end())
        {
            PubSubContextTable::iterator found = m_pattern_pubsub_context_table.find(*it);
            if (found != m_pattern_pubsub_context_table.end())
            {
                found->second.erase(&ctx);
            }
            it++;
        }
        ctx.ClearPubSub();
    }

    template<typename T>
    static void unsubscribe(ArdbConnContext& ctx, ArgumentArray& cmd, bool is_pattern, T& submap)
    {
        PubSubChannelSet& uset = is_pattern ? ctx.GetPubSub().pattern_pubsub_channle_set : ctx.GetPubSub().pubsub_channle_set;
        if (cmd.empty())
        {
            if (uset.empty())
            {
                ctx.reply.type = REDIS_REPLY_ARRAY;
                ctx.reply.elements.push_back(RedisReply(is_pattern ? "punsubscribe" : "unsubscribe"));
                RedisReply nil;
                nil.type = REDIS_REPLY_NIL;
                ctx.reply.elements.push_back(nil);
                ctx.reply.elements.push_back(RedisReply(ctx.SubChannelSize()));
            }
            else
            {
                PubSubChannelSet::iterator it = uset.begin();
                uint32 i = 1;
                while (it != uset.end())
                {
                    //uset->erase(*it);
                    typename T::iterator found = submap.find(*it);
                    if (found != submap.end())
                    {
                        found->second.erase(&ctx);
                    }
                    RedisReply r;
                    r.type = REDIS_REPLY_ARRAY;
                    r.elements.push_back(RedisReply(is_pattern ? "punsubscribe" : "unsubscribe"));
                    r.elements.push_back(RedisReply(*it));
                    r.elements.push_back(RedisReply(ctx.SubChannelSize() - i));
                    ctx.conn->Write(r);
                    it++;
                }
                uset.clear();
            }
        }
        else
        {
            ArgumentArray::iterator it = cmd.begin();
            while (it != cmd.end())
            {
                uset.erase(*it);
                typename T::iterator found = submap.find(*it);
                if (found != submap.end())
                {
                    found->second.erase(&ctx);
                }
                RedisReply r;
                r.type = REDIS_REPLY_ARRAY;
                r.elements.push_back(RedisReply(is_pattern ? "punsubscribe" : "unsubscribe"));
                r.elements.push_back(RedisReply(*it));
                r.elements.push_back(RedisReply(ctx.SubChannelSize()));
                ctx.conn->Write(r);
                it++;
            }
        }
    }

    int ArdbServer::UnSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);
        unsubscribe(ctx, cmd.GetArguments(), false, m_pubsub_context_table);
        return 0;
    }
    int ArdbServer::PSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);

        ArgumentArray::iterator it = cmd.GetArguments().begin();
        while (it != cmd.GetArguments().end())
        {
            RedisReply r;
            r.type = REDIS_REPLY_ARRAY;
            r.elements.push_back(RedisReply("psubscribe"));
            r.elements.push_back(RedisReply(*it));
            ctx.GetPubSub().pattern_pubsub_channle_set.insert(*it);
            m_pattern_pubsub_context_table[*it].insert(&ctx);
            r.elements.push_back(RedisReply(ctx.SubChannelSize()));
            ctx.conn->Write(r);
            it++;
        }
        return 0;
    }
    int ArdbServer::PUnSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);
        unsubscribe(ctx, cmd.GetArguments(), true, m_pattern_pubsub_context_table);
        return 0;
    }

    static void async_write_message(Channel* ch, void * data)
    {
        RedisReply* r = (RedisReply*) data;
        ch->Write(*r);
        DELETE(r);
    }

    static void publish_message(ContextSet& set, const std::string& message, const std::string& channel)
    {
        ContextSet::iterator it = set.begin();
        while (it != set.end())
        {
            NEW2(r, RedisReply, RedisReply);
            r->type = REDIS_REPLY_ARRAY;
            r->elements.push_back(RedisReply("message"));
            r->elements.push_back(RedisReply(channel));
            r->elements.push_back(RedisReply(message));
            (*it)->conn->AsyncWrite(async_write_message, r);
            it++;
        }
    }

    static void publish_pattern_message(ContextSet& set, const std::string& message, const std::string& pattern,
            const std::string& channel)
    {
        ContextSet::iterator it = set.begin();
        while (it != set.end())
        {
            NEW2(r, RedisReply, RedisReply);
            r->type = REDIS_REPLY_ARRAY;
            r->elements.push_back(RedisReply("pmessage"));
            r->elements.push_back(RedisReply(pattern));
            r->elements.push_back(RedisReply(channel));
            r->elements.push_back(RedisReply(message));
            (*it)->conn->AsyncWrite(async_write_message, r);
            it++;
        }
    }

    int ArdbServer::Publish(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        LockGuard<ThreadMutex> guard(m_pubsub_mutex);
        const std::string& channel = cmd.GetArguments()[0];
        const std::string& message = cmd.GetArguments()[1];
        PubSubContextTable::iterator found = m_pubsub_context_table.find(channel);
        int size = 0;
        if (found != m_pubsub_context_table.end())
        {
            size = found->second.size();
            publish_message(found->second, message, channel);
        }
        PubSubContextTable::iterator it = m_pattern_pubsub_context_table.begin();
        while (it != m_pattern_pubsub_context_table.end())
        {
            if (fnmatch(it->first.c_str(), channel.c_str(), 0) == 0)
            {
                publish_pattern_message(it->second, message, it->first, channel);
                size += it->second.size();
            }
            it++;
        }
        ctx.reply.integer = size;
        ctx.reply.type = REDIS_REPLY_INTEGER;
        return 0;
    }
}


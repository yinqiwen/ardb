/*
 * pubsub.cpp
 *
 *  Created on: 2013-4-25
 *      Author: wqy
 */

#include "ardb_server.hpp"
#include <fnmatch.h>

namespace ardb
{
	int ArdbServer::Subscribe(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if (NULL == ctx.pubsub_channle_set)
		{
			ctx.pubsub_channle_set = new PubSubChannelSet;
		}
		ArgumentArray::iterator it = cmd.begin();
		while (it != cmd.end())
		{
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(RedisReply("subscribe"));
			r.elements.push_back(RedisReply(*it));

			ctx.pubsub_channle_set->insert(*it);
			m_pubsub_context_table[*it].insert(&ctx);
			r.elements.push_back(RedisReply(ctx.SubChannelSize()));
			ctx.conn->Write(r);
			it++;
		}
		return 0;
	}

	void ArdbServer::ClearSubscribes(ArdbConnContext& ctx)
	{
		if (NULL != ctx.pubsub_channle_set)
		{
			PubSubChannelSet::iterator it = ctx.pubsub_channle_set->begin();
			while (it != ctx.pubsub_channle_set->end())
			{
				PubSubContextTable::iterator found =
						m_pubsub_context_table.find(*it);
				if (found != m_pubsub_context_table.end())
				{
					found->second.erase(&ctx);
				}
				it++;
			}
		}
		if (NULL != ctx.pattern_pubsub_channle_set)
		{
			PubSubChannelSet::iterator it =
					ctx.pattern_pubsub_channle_set->begin();
			while (it != ctx.pattern_pubsub_channle_set->end())
			{
				PubSubContextTable::iterator found =
						m_pattern_pubsub_context_table.find(*it);
				if (found != m_pattern_pubsub_context_table.end())
				{
					found->second.erase(&ctx);
				}
				it++;
			}
		}
		DELETE(ctx.pubsub_channle_set);
		DELETE(ctx.pattern_pubsub_channle_set);
	}

	template<typename T>
	static void unsubscribe(ArdbConnContext& ctx, ArgumentArray& cmd,
			bool is_pattern, T& submap)
	{
		PubSubChannelSet* uset =
				is_pattern ?
						ctx.pattern_pubsub_channle_set : ctx.pubsub_channle_set;
		if (cmd.empty())
		{
			if (NULL == uset)
			{
				ctx.reply.type = REDIS_REPLY_ARRAY;
				ctx.reply.elements.push_back(
						RedisReply(
								is_pattern ? "punsubscribe" : "unsubscribe"));
				RedisReply nil;
				nil.type = REDIS_REPLY_NIL;
				ctx.reply.elements.push_back(nil);
				ctx.reply.elements.push_back(RedisReply(ctx.SubChannelSize()));
			} else
			{
				PubSubChannelSet::iterator it = uset->begin();
				uint32 i = 1;
				while (it != uset->end())
				{
					//uset->erase(*it);
					typename T::iterator found = submap.find(*it);
					if (found != submap.end())
					{
						found->second.erase(&ctx);
					}
					RedisReply r;
					r.type = REDIS_REPLY_ARRAY;
					r.elements.push_back(
							RedisReply(
									is_pattern ?
											"punsubscribe" : "unsubscribe"));
					r.elements.push_back(RedisReply(*it));
					r.elements.push_back(RedisReply(ctx.SubChannelSize() - i));
					ctx.conn->Write(r);
					it++;
				}
				if (is_pattern)
				{
					DELETE(ctx.pattern_pubsub_channle_set);
				} else
				{
					DELETE(ctx.pubsub_channle_set);
				}
			}
		} else
		{
			ArgumentArray::iterator it = cmd.begin();
			while (it != cmd.end())
			{
				if (NULL != uset)
				{
					uset->erase(*it);
					if (uset->empty())
					{
						if (is_pattern)
						{
							DELETE(ctx.pattern_pubsub_channle_set);
						} else
						{
							DELETE(ctx.pubsub_channle_set);
						}
					}
				}
				typename T::iterator found = submap.find(*it);
				if (found != submap.end())
				{
					found->second.erase(&ctx);
				}
				RedisReply r;
				r.type = REDIS_REPLY_ARRAY;
				r.elements.push_back(
						RedisReply(
								is_pattern ? "punsubscribe" : "unsubscribe"));
				r.elements.push_back(RedisReply(*it));
				r.elements.push_back(RedisReply(ctx.SubChannelSize()));
				ctx.conn->Write(r);
				it++;
			}
		}
	}

	int ArdbServer::UnSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		unsubscribe(ctx, cmd, false, m_pubsub_context_table);
		return 0;
	}
	int ArdbServer::PSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if (NULL == ctx.pattern_pubsub_channle_set)
		{
			ctx.pattern_pubsub_channle_set = new PubSubChannelSet;
		}
		ArgumentArray::iterator it = cmd.begin();
		while (it != cmd.end())
		{
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(RedisReply("psubscribe"));
			r.elements.push_back(RedisReply(*it));

			ctx.pattern_pubsub_channle_set->insert(*it);
			m_pattern_pubsub_context_table[*it].insert(&ctx);
			r.elements.push_back(RedisReply(ctx.SubChannelSize()));
			ctx.conn->Write(r);
			it++;
		}
		return 0;
	}
	int ArdbServer::PUnSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		unsubscribe(ctx, cmd, true, m_pattern_pubsub_context_table);
		return 0;
	}

	static void publish_message(ContextSet& set, const std::string& message,
			const std::string& channel)
	{
		ContextSet::iterator it = set.begin();
		while (it != set.end())
		{
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(RedisReply("message"));
			r.elements.push_back(RedisReply(channel));
			r.elements.push_back(RedisReply(message));
			(*it)->conn->Write(r);
			it++;
		}
	}

	static void publish_pattern_message(ContextSet& set,
			const std::string& message, const std::string& pattern,
			const std::string& channel)
	{
		ContextSet::iterator it = set.begin();
		while (it != set.end())
		{
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(RedisReply("pmessage"));
			r.elements.push_back(RedisReply(pattern));
			r.elements.push_back(RedisReply(channel));
			r.elements.push_back(RedisReply(message));
			(*it)->conn->Write(r);
			it++;
		}
	}

	int ArdbServer::Publish(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		const std::string& channel = cmd[0];
		const std::string& message = cmd[1];
		PubSubContextTable::iterator found = m_pubsub_context_table.find(
				channel);
		int size = 0;
		if (found != m_pubsub_context_table.end())
		{
			size = found->second.size();
			publish_message(found->second, message, channel);
		}
		PubSubContextTable::iterator it =
				m_pattern_pubsub_context_table.begin();
		while (it != m_pattern_pubsub_context_table.end())
		{
			if (fnmatch(it->first.c_str(), channel.c_str(), 0) == 0)
			{
				publish_pattern_message(it->second, message, it->first,
						channel);
				size += it->second.size();
			}
			it++;
		}
		ctx.reply.integer = size;
		ctx.reply.type = REDIS_REPLY_INTEGER;
		return 0;
	}
}


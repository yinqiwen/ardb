/*
 * transaction.cpp
 *
 *  Created on: 2013-5-16
 *      Author: wqy
 */
#include "ardb_server.hpp"

namespace ardb
{
	int ArdbServer::Multi(ArdbConnContext& ctx, RedisCommandFrame& cmd)
	{
		ctx.in_transaction = true;
		if (NULL == ctx.transaction_cmds)
		{
			ctx.transaction_cmds = new TransactionCommandQueue;
		}
		ctx.reply.type = REDIS_REPLY_STATUS;
		ctx.reply.str = "OK";
		return 0;
	}

	int ArdbServer::Discard(ArdbConnContext& ctx, RedisCommandFrame& cmd)
	{
		ctx.in_transaction = false;
		DELETE(ctx.transaction_cmds);
		ctx.reply.type = REDIS_REPLY_STATUS;
		ctx.reply.str = "OK";
		return 0;
	}

	int ArdbServer::Exec(ArdbConnContext& ctx, RedisCommandFrame& cmd)
	{
		if (ctx.fail_transc || !ctx.in_transaction)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			if (NULL != ctx.transaction_cmds)
			{
				for (uint32 i = 0; i < ctx.transaction_cmds->size(); i++)
				{
					RedisCommandFrame& cmd = ctx.transaction_cmds->at(i);
					DoRedisCommand(ctx,
							FindRedisCommandHandlerSetting(cmd.GetCommand()),
							cmd);
					r.elements.push_back(ctx.reply);
				}
			}
			ctx.reply = r;
		}
		ctx.in_transaction = false;
		DELETE(ctx.transaction_cmds);
		ClearWatchKeys(ctx);
		return 0;
	}

	void ArdbServer::ClearWatchKeys(ArdbConnContext& ctx)
	{
		if (NULL != ctx.watch_key_set)
		{
			LockGuard<ThreadMutex> guard(m_watch_mutex);
			WatchKeySet::iterator it = ctx.watch_key_set->begin();
			while (it != ctx.watch_key_set->end())
			{
				WatchKeyContextTable::iterator found =
						m_watch_context_table.find(*it);
				if (found != m_watch_context_table.end())
				{
					ContextSet& list = found->second;
					ContextSet::iterator lit = list.begin();
					while (lit != list.end())
					{
						if (*lit == &ctx)
						{
							list.erase(lit);
							break;
						}
						lit++;
					}
					if (list.empty())
					{
						m_watch_context_table.erase(found);
					}
				}
				it++;
			}
			if (m_watch_context_table.empty())
			{
				m_db->RegisterKeyWatcher(NULL);
			}
		}
		DELETE(ctx.watch_key_set);
	}

	int ArdbServer::OnKeyUpdated(const DBID& dbid, const Slice& key)
	{
		LockGuard<ThreadMutex> guard(m_watch_mutex);
		if (!m_watch_context_table.empty())
		{
			WatchKey k(dbid, std::string(key.data(), key.size()));
			WatchKeyContextTable::iterator found = m_watch_context_table.find(
					k);
			if (found != m_watch_context_table.end())
			{
				ContextSet& list = found->second;
				ContextSet::iterator lit = list.begin();
				while (lit != list.end())
				{
					if (*lit != m_ctx_local.GetValue())
					{
						(*lit)->fail_transc = true;
					}
					lit++;
				}
			}
		}
		return 0;
	}
	int ArdbServer::OnAllKeyDeleted(const DBID& dbid)
	{
		LockGuard<ThreadMutex> guard(m_watch_mutex);
		WatchKeyContextTable::iterator it = m_watch_context_table.begin();
		while (it != m_watch_context_table.end())
		{
			if (it->first.db == dbid)
			{
				OnKeyUpdated(it->first.db, it->first.key);
			}
			it++;
		}
		return 0;
	}

	int ArdbServer::Watch(ArdbConnContext& ctx, RedisCommandFrame& cmd)
	{
		ctx.reply.type = REDIS_REPLY_STATUS;
		ctx.reply.str = "OK";
		m_db->RegisterKeyWatcher(this);
		if (NULL == ctx.watch_key_set)
		{
			ctx.watch_key_set = new WatchKeySet;
		}
		ArgumentArray::iterator it = cmd.GetArguments().begin();
		while (it != cmd.GetArguments().end())
		{
			WatchKey k(ctx.currentDB, *it);
			ctx.watch_key_set->insert(k);
			LockGuard<ThreadMutex> guard(m_watch_mutex);
			WatchKeyContextTable::iterator found = m_watch_context_table.find(
					k);
			if (found == m_watch_context_table.end())
			{
				ContextSet set;
				set.insert(&ctx);
				m_watch_context_table.insert(
						WatchKeyContextTable::value_type(k, set));
			} else
			{
				ContextSet& set = found->second;
				ContextSet::iterator lit = set.begin();
				while (lit != set.end())
				{
					if (*lit == &ctx)
					{
						set.erase(lit);
						break;
					}
					lit++;
				}
				set.insert(&ctx);
			}
			it++;
		}
		return 0;
	}
	int ArdbServer::UnWatch(ArdbConnContext& ctx, RedisCommandFrame& cmd)
	{
		ctx.reply.type = REDIS_REPLY_STATUS;
		ctx.reply.str = "OK";
		ClearWatchKeys(ctx);
		return 0;
	}
}


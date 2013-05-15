/*
 * ardb_server.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */
#include "ardb_server.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <fnmatch.h>
#include <sstream>

//#define __USE_KYOTOCABINET__ 1
//#define __USE_LMDB__ 1
#ifdef __USE_KYOTOCABINET__
#include "engine/kyotocabinet_engine.hpp"
typedef ardb::KCDBEngineFactory SelectedDBEngineFactory;
#elif defined __USE_LMDB__
#include "engine/lmdb_engine.hpp"
typedef ardb::LMDBEngineFactory SelectedDBEngineFactory;
#else
#include "engine/leveldb_engine.hpp"
typedef ardb::LevelDBEngineFactory SelectedDBEngineFactory;
#endif

namespace ardb
{
	static inline void fill_error_reply(RedisReply& reply, const char* fmt, ...)
	{
		va_list ap;
		va_start(ap, fmt);
		char buf[1024];
		vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
		va_end(ap);
		reply.type = REDIS_REPLY_ERROR;
		reply.str = buf;
	}

	static inline void fill_status_reply(RedisReply& reply, const char* s)
	{
		reply.type = REDIS_REPLY_STATUS;
		reply.str = s;
	}

	static inline void fill_int_reply(RedisReply& reply, int64 v)
	{
		reply.type = REDIS_REPLY_INTEGER;
		reply.integer = v;
	}
	static inline void fill_double_reply(RedisReply& reply, double v)
	{
		reply.type = REDIS_REPLY_DOUBLE;
		reply.double_value = v;
	}

	static inline void fill_str_reply(RedisReply& reply, const std::string& v)
	{
		reply.type = REDIS_REPLY_STRING;
		reply.str = v;
	}

	template<typename T>
	static inline void fill_array_reply(RedisReply& reply, T& v)
	{
		reply.type = REDIS_REPLY_ARRAY;
		typename T::iterator it = v.begin();
		while (it != v.end())
		{
			const ValueObject& vo = *it;
			RedisReply r;
			if (vo.type == EMPTY)
			{
				r.type = REDIS_REPLY_NIL;
			} else
			{
				std::string str;
				vo.ToString(str);
				fill_str_reply(r, str);
			}
			reply.elements.push_back(r);
			it++;
		}
	}

	template<typename T>
	static inline void fill_str_array_reply(RedisReply& reply, T& v)
	{
		reply.type = REDIS_REPLY_ARRAY;
		typename T::iterator it = v.begin();
		while (it != v.end())
		{
			RedisReply r;
			fill_str_reply(r, *it);
			reply.elements.push_back(r);
			it++;
		}
	}

	template<typename T>
	static inline void fill_int_array_reply(RedisReply& reply, T& v)
	{
		reply.type = REDIS_REPLY_ARRAY;
		typename T::iterator it = v.begin();
		while (it != v.end())
		{
			RedisReply r;
			fill_int_reply(r, *it);
			reply.elements.push_back(r);
			it++;
		}
	}

	int ArdbServer::ParseConfig(const Properties& props, ArdbServerConfig& cfg)
	{
		conf_get_int64(props, "port", cfg.listen_port);
		conf_get_int64(props, "tcp-keepalive", cfg.tcp_keepalive);
		conf_get_int64(props, "unixsocketperm", cfg.unixsocketperm);
		conf_get_int64(props, "slowlog-log-slower-than",
				cfg.slowlog_log_slower_than);
		conf_get_int64(props, "slowlog-max-len", cfg.slowlog_max_len);
		conf_get_int64(props, "auto_batch_flush_period",
				cfg.batch_flush_period);
		std::string on = "on";
		conf_get_string(props, "auto_batch_write_enable", on);
		cfg.batch_write_enable = on == "on";
		conf_get_int64(props, "maxclients", cfg.max_clients);
		conf_get_string(props, "bind", cfg.listen_host);
		conf_get_string(props, "unixsocket", cfg.listen_unix_path);
		conf_get_string(props, "dir", cfg.data_base_path);
		conf_get_string(props, "backup-dir", cfg.backup_dir);
		conf_get_string(props, "repl-dir", cfg.repl_data_dir);
		conf_get_string(props, "loglevel", cfg.loglevel);
		conf_get_string(props, "logfile", cfg.logfile);
		std::string daemonize, single;
		conf_get_string(props, "daemonize", daemonize);
		conf_get_string(props, "single", single);

		conf_get_int64(props, "repl-ping-slave-period",
				cfg.repl_ping_slave_period);
		conf_get_int64(props, "repl-timeout", cfg.repl_timeout);
		conf_get_int64(props, "repl-sync-state-persist-period",
				cfg.repl_syncstate_persist_period);

		std::string slaveof;
		if (conf_get_string(props, "slaveof", slaveof))
		{
			std::vector<std::string> ss = split_string(slaveof, ":");
			if (ss.size() == 2)
			{
				cfg.master_host = ss[0];
				if (!string_touint32(ss[1], cfg.master_port))
				{
					cfg.master_host = "";
					ERROR_LOG("Invalid 'slaveof' config.");
				}
			} else
			{
				ERROR_LOG("Invalid 'slaveof' config.");
			}
		}

		daemonize = string_tolower(daemonize);
		if (daemonize == "yes")
		{
			cfg.daemonize = true;
		}
		if (single == "yes")
		{
			cfg.single = true;
		}
		if (cfg.data_base_path.empty())
		{
			cfg.data_base_path = ".";
		}
		ArdbLogger::SetLogLevel(cfg.loglevel);
		return 0;
	}

	ArdbServer::ArdbServer() :
			m_service(NULL), m_db(NULL), m_engine(NULL), m_slowlog_handler(
					m_cfg), m_repli_serv(this), m_current_ctx(NULL), m_slave_client(
					this)
	{
		struct RedisCommandHandlerSetting settingTable[] =
			{
				{ "ping", &ArdbServer::Ping, 0, 0, 0 },
				{ "multi", &ArdbServer::Multi, 0, 0, 0 },
				{ "discard", &ArdbServer::Discard, 0, 0, 0 },
				{ "exec", &ArdbServer::Exec, 0, 0, 0 },
				{ "watch", &ArdbServer::Watch, 0, -1, 0 },
				{ "unwatch", &ArdbServer::UnWatch, 0, 0, 0 },
				{ "subscribe", &ArdbServer::Subscribe, 1, -1, 0 },
				{ "psubscribe", &ArdbServer::PSubscribe, 1, -1, 0 },
				{ "unsubscribe", &ArdbServer::UnSubscribe, 0, -1, 0 },
				{ "punsubscribe", &ArdbServer::PUnSubscribe, 0, -1, 0 },
				{ "publish", &ArdbServer::Publish, 2, 2, 0 },
				{ "info", &ArdbServer::Info, 0, 1, 0 },
				{ "save", &ArdbServer::Save, 0, 0, 0 },
				{ "bgsave", &ArdbServer::BGSave, 0, 0, 0 },
				{ "lastsave", &ArdbServer::LastSave, 0, 0, 0 },
				{ "slowlog", &ArdbServer::SlowLog, 1, 2, 0 },
				{ "dbsize", &ArdbServer::DBSize, 0, 0, 0 },
				{ "config", &ArdbServer::Config, 1, 3, 0 },
				{ "client", &ArdbServer::Client, 1, 3, 0 },
				{ "flushdb", &ArdbServer::FlushDB, 0, 0, 1 },
				{ "flushall", &ArdbServer::FlushAll, 0, 0, 1 },
				{ "time", &ArdbServer::Time, 0, 0, 0 },
				{ "echo", &ArdbServer::Echo, 1, 1, 0 },
				{ "quit", &ArdbServer::Quit, 0, 0, 0 },
				{ "shutdown", &ArdbServer::Shutdown, 0, 1, 0 },
				{ "slaveof", &ArdbServer::Slaveof, 2, 2, 0 },
				{ "replconf", &ArdbServer::ReplConf, 0, -1, 0 },
				{ "sync", &ArdbServer::Sync, 0, 2, 0 },
				{ "arsync", &ArdbServer::ARSync, 2, 2, 0 },
				{ "select", &ArdbServer::Select, 1, 1, 1 },
				{ "append", &ArdbServer::Append, 2, 2 },
				{ "get", &ArdbServer::Get, 1, 1, 0 },
				{ "set", &ArdbServer::Set, 2, 7, 1 },
				{ "del", &ArdbServer::Del, 1, -1, 1 },
				{ "exists", &ArdbServer::Exists, 1, 1, 0 },
				{ "expire", &ArdbServer::Expire, 2, 2, 1 },
				{ "expireat", &ArdbServer::Expireat, 2, 2, 1 },
				{ "persist", &ArdbServer::Persist, 1, 1, 1 },
				{ "type", &ArdbServer::Type, 1, 1, 0 },
				{ "bitcount", &ArdbServer::Bitcount, 1, 3, 0 },
				{ "bitop", &ArdbServer::Bitop, 3, -1, 1 },
				{ "decr", &ArdbServer::Decr, 1, 1, 1 },
				{ "decrby", &ArdbServer::Decrby, 2, 2, 1 },
				{ "getbit", &ArdbServer::GetBit, 2, 2, 0 },
				{ "getrange", &ArdbServer::GetRange, 3, 3, 0 },
				{ "getset", &ArdbServer::GetSet, 2, 2, 1 },
				{ "incr", &ArdbServer::Incr, 1, 1, 1 },
				{ "incrby", &ArdbServer::Incrby, 2, 2, 1 },
				{ "incrbyfloat", &ArdbServer::IncrbyFloat, 2, 2, 1 },
				{ "mget", &ArdbServer::MGet, 1, -1, 0 },
				{ "mset", &ArdbServer::MSet, 2, -1, 1 },
				{ "msetnx", &ArdbServer::MSetNX, 2, -1, 1 },
				{ "psetex", &ArdbServer::MSetNX, 3, 3, 1 },
				{ "setbit", &ArdbServer::SetBit, 3, 3, 1 },
				{ "setex", &ArdbServer::SetEX, 3, 3, 1 },
				{ "setnx", &ArdbServer::SetNX, 2, 2, 1 },
				{ "setrange", &ArdbServer::SetRange, 3, 3, 1 },
				{ "strlen", &ArdbServer::Strlen, 1, 1, 0 },
				{ "hdel", &ArdbServer::HDel, 2, -1, 1 },
				{ "hexists", &ArdbServer::HExists, 2, 2, 0 },
				{ "hget", &ArdbServer::HGet, 2, 2, 0 },
				{ "hgetall", &ArdbServer::HGetAll, 1, 1, 0 },
				{ "hincr", &ArdbServer::HIncrby, 3, 3, 1 },
				{ "hmincr", &ArdbServer::HMIncrby, 3, -1, 1 },
				{ "hincrbyfloat", &ArdbServer::HIncrbyFloat, 3, 3, 1 },
				{ "hkeys", &ArdbServer::HKeys, 1, 1, 0 },
				{ "hlen", &ArdbServer::HLen, 1, 1, 0 },
				{ "hvals", &ArdbServer::HVals, 1, 1, 0 },
				{ "hmget", &ArdbServer::HMGet, 2, -1, 0 },
				{ "hset", &ArdbServer::HSet, 3, 3, 1 },
				{ "hsetnx", &ArdbServer::HSetNX, 3, 3, 1 },
				{ "hmset", &ArdbServer::HMSet, 3, -1, 1 },
				{ "scard", &ArdbServer::SCard, 1, 1, 0 },
				{ "sadd", &ArdbServer::SAdd, 2, -1, 1 },
				{ "sdiff", &ArdbServer::SDiff, 2, -1, 0 },
				{ "sdiffcount", &ArdbServer::SDiffCount, 2, -1, 0 },
				{ "sdiffstore", &ArdbServer::SDiffStore, 3, -1, 1 },
				{ "sinter", &ArdbServer::SInter, 2, -1, 0 },
				{ "sintercount", &ArdbServer::SInterCount, 2, -1, 0 },
				{ "sinterstore", &ArdbServer::SInterStore, 3, -1, 1 },
				{ "sismember", &ArdbServer::SIsMember, 2, 2, 0 },
				{ "smembers", &ArdbServer::SMembers, 1, 1, 0 },
				{ "smove", &ArdbServer::SMove, 3, 3, 1 },
				{ "spop", &ArdbServer::SPop, 1, 1, 1 },
				{ "sranmember", &ArdbServer::SRandMember, 1, 2, 0 },
				{ "srem", &ArdbServer::SRem, 2, -1, 1 },
				{ "sunion", &ArdbServer::SUnion, 2, -1, 0 },
				{ "sunionstore", &ArdbServer::SUnionStore, 3, -1, 1 },
				{ "sunioncount", &ArdbServer::SUnionCount, 2, -1, 0 },
				{ "zadd", &ArdbServer::ZAdd, 3, -1, 1 },
				{ "zcard", &ArdbServer::ZCard, 1, 1, 0 },
				{ "zcount", &ArdbServer::ZCount, 3, 3, 0 },
				{ "zincrby", &ArdbServer::ZIncrby, 3, 3, 1 },
				{ "zrange", &ArdbServer::ZRange, 3, 4, 0 },
				{ "zrangebyscore", &ArdbServer::ZRangeByScore, 3, 7, 0 },
				{ "zrank", &ArdbServer::ZRank, 2, 2, 0 },
				{ "zrem", &ArdbServer::ZRem, 2, -1, 1 },
				{ "zremrangebyrank", &ArdbServer::ZRemRangeByRank, 3, 3, 1 },
				{ "zremrangebyscore", &ArdbServer::ZRemRangeByScore, 3, 3, 1 },
				{ "zrevrange", &ArdbServer::ZRevRange, 3, 4, 0 },
				{ "zrevrangebyscore", &ArdbServer::ZRevRangeByScore, 3, 7, 0 },
				{ "zinterstore", &ArdbServer::ZInterStore, 3, -1, 1 },
				{ "zunionstore", &ArdbServer::ZUnionStore, 3, -1, 1 },
				{ "zrevrank", &ArdbServer::ZRevRank, 2, 2, 0 },
				{ "zscore", &ArdbServer::ZScore, 2, 2, 0 },
				{ "lindex", &ArdbServer::LIndex, 2, 2, 0 },
				{ "linsert", &ArdbServer::LInsert, 4, 4, 1 },
				{ "llen", &ArdbServer::LLen, 1, 1, 0 },
				{ "lpop", &ArdbServer::LPop, 1, 1, 1 },
				{ "lpush", &ArdbServer::LPush, 2, -1, 1 },
				{ "lpushx", &ArdbServer::LPushx, 2, 2, 1 },
				{ "lrange", &ArdbServer::LRange, 3, 3, 0 },
				{ "lrem", &ArdbServer::LRem, 3, 3, 1 },
				{ "lset", &ArdbServer::LSet, 3, 3, 1 },
				{ "ltrim", &ArdbServer::LTrim, 3, 3, 1 },
				{ "rpop", &ArdbServer::RPop, 1, 1, 1 },
				{ "rpush", &ArdbServer::RPush, 2, -1, 1 },
				{ "rpushx", &ArdbServer::RPushx, 2, 2, 1 },
				{ "rpoplpush", &ArdbServer::RPopLPush, 2, 2, 1 },
				{ "hclear", &ArdbServer::HClear, 1, 1, 1 },
				{ "zclear", &ArdbServer::ZClear, 1, 1, 1 },
				{ "sclear", &ArdbServer::SClear, 1, 1, 1 },
				{ "lclear", &ArdbServer::LClear, 1, 1, 1 },
				{ "tclear", &ArdbServer::TClear, 1, 1, 1 },
				{ "move", &ArdbServer::Move, 2, 2, 1 },
				{ "rename", &ArdbServer::Rename, 2, 2, 1 },
				{ "renamenx", &ArdbServer::RenameNX, 2, 2, 1 },
				{ "sort", &ArdbServer::Sort, 1, -1, 2 },
				{ "keys", &ArdbServer::Keys, 1, 1, 0 },
				{ "__set__", &ArdbServer::RawSet, 2, 2, 1 },
				{ "__del__", &ArdbServer::RawDel, 1, 1, 1 }, };

		uint32 arraylen = arraysize(settingTable);
		for (uint32 i = 0; i < arraylen; i++)
		{
			m_handler_table[settingTable[i].name] = settingTable[i];
		}
	}
	ArdbServer::~ArdbServer()
	{

	}

	int ArdbServer::RawSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->RawSet(ctx.currentDB, cmd[0], cmd[1]);
		return 0;
	}
	int ArdbServer::RawDel(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->RawDel(ctx.currentDB, cmd[0]);
		return 0;
	}

	int ArdbServer::Multi(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ctx.in_transaction = true;
		if (NULL == ctx.transaction_cmds)
		{
			ctx.transaction_cmds = new TransactionCommandQueue;
		}
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::Discard(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ctx.in_transaction = false;
		DELETE(ctx.transaction_cmds);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::Exec(ArdbConnContext& ctx, ArgumentArray& cmd)
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
					if (*lit != m_current_ctx)
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

	int ArdbServer::Watch(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "OK");
		m_db->RegisterKeyWatcher(this);
		if (NULL == ctx.watch_key_set)
		{
			ctx.watch_key_set = new WatchKeySet;
		}
		ArgumentArray::iterator it = cmd.begin();
		while (it != cmd.end())
		{
			WatchKey k(ctx.currentDB, *it);
			ctx.watch_key_set->insert(k);
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
	int ArdbServer::UnWatch(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "OK");
		ClearWatchKeys(ctx);
		return 0;
	}

	int ArdbServer::Keys(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		StringSet keys;
		m_db->Keys(ctx.currentDB, cmd[0], keys);
		fill_str_array_reply(ctx.reply, keys);
		return 0;
	}
	int ArdbServer::HClear(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->HClear(ctx.currentDB, cmd[0]);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::SClear(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->SClear(ctx.currentDB, cmd[0]);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::ZClear(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->ZClear(ctx.currentDB, cmd[0]);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::LClear(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->LClear(ctx.currentDB, cmd[0]);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::TClear(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->TClear(ctx.currentDB, cmd[0]);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::Save(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_repli_serv.Save();
		if (ret == 0)
		{
			fill_status_reply(ctx.reply, "OK");
		} else
		{
			fill_error_reply(ctx.reply, "ERR Save error");
		}
		return 0;
	}

	int ArdbServer::LastSave(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_repli_serv.LastSave();
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::BGSave(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_repli_serv.BGSave();
		if (ret == 0)
		{
			fill_status_reply(ctx.reply, "OK");
		} else
		{
			fill_error_reply(ctx.reply, "ERR Save error");
		}
		return 0;
	}

	int ArdbServer::Info(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string info;
		info.append("# Server\r\n");
		info.append("ardb_version:").append(ARDB_VERSION).append("\r\n");
		info.append("engine:").append(m_engine->GetName()).append("\r\n");
		info.append("# Databases\r\n");
		DBIDSet dbs;
		m_db->ListAllDB(dbs);
		DBIDSet::iterator it = dbs.begin();
		while (it != dbs.end())
		{
			info.append("DB:").append(*it).append("\r\n");
			it++;
		}

		info.append("# Disk\r\n");
		int64 filesize = file_size(m_cfg.data_base_path);
		char tmp[256];
		sprintf(tmp, "%lld", filesize);
		info.append("db_used_space:").append(tmp).append("\r\n");

		if (!m_cfg.single)
		{
			info.append("# Oplogs\r\n");
			std::ostringstream oss;
			oss << m_repli_serv.GetOpLogs().MemCacheSize();
			info.append("oplogs_cache_size:").append(oss.str()).append("\r\n");
		}

		fill_str_reply(ctx.reply, info);
		return 0;
	}

	int ArdbServer::DBSize(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string path = m_cfg.data_base_path + "/" + ctx.currentDB;
		fill_int_reply(ctx.reply, file_size(path));
		return 0;
	}

	int ArdbServer::Client(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string subcmd = string_tolower(cmd[0]);
		if (subcmd == "kill")
		{
			if (cmd.size() != 2)
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			Channel* conn = m_clients_holder.GetConn(cmd[1]);
			if (NULL == conn)
			{
				fill_error_reply(ctx.reply, "ERR No such client");
				return 0;
			}
			fill_status_reply(ctx.reply, "OK");
			if (conn == ctx.conn)
			{
				return -1;
			} else
			{
				conn->Close();
			}
		} else if (subcmd == "setname")
		{
			if (cmd.size() != 2)
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			m_clients_holder.SetName(ctx.conn, cmd[1]);
			fill_status_reply(ctx.reply, "OK");
			return 0;
		} else if (subcmd == "getname")
		{
			if (cmd.size() != 1)
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			fill_str_reply(ctx.reply, m_clients_holder.GetName(ctx.conn));
		} else if (subcmd == "list")
		{
			if (cmd.size() != 1)
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			m_clients_holder.List(ctx.reply);
		} else if (subcmd == "stat")
		{
			if (cmd.size() != 2)
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			if (!strcasecmp(cmd[1].c_str(), "on"))
			{
				m_clients_holder.SetStatEnable(true);
			} else if (!strcasecmp(cmd[1].c_str(), "on"))
			{
				m_clients_holder.SetStatEnable(false);
			} else
			{
				fill_error_reply(ctx.reply,
						"ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name | STAT on/off)");
				return 0;
			}
			fill_status_reply(ctx.reply, "OK");
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR CLIENT subcommand must be one of LIST, GETNAME, SETNAME, KILL, STAT");
		}
		return 0;
	}

	int ArdbServer::SlowLog(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string subcmd = string_tolower(cmd[0]);
		if (subcmd == "len")
		{
			fill_int_reply(ctx.reply, m_slowlog_handler.Size());
		} else if (subcmd == "reset")
		{
			fill_status_reply(ctx.reply, "OK");
		} else if (subcmd == "get")
		{
			if (cmd.size() != 2)
			{
				fill_error_reply(ctx.reply,
						"ERR Wrong number of arguments for SLOWLOG GET");
			}
			uint32 len = 0;
			if (!string_touint32(cmd[1], len))
			{
				fill_error_reply(ctx.reply,
						"ERR value is not an integer or out of range.");
				return 0;
			}
			m_slowlog_handler.GetSlowlog(len, ctx.reply);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR SLOWLOG subcommand must be one of GET, LEN, RESET");
		}
		return 0;
	}

	int ArdbServer::Config(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string arg0 = string_tolower(cmd[0]);
		if (arg0 != "get" && arg0 != "set" && arg0 != "resetstat")
		{
			fill_error_reply(ctx.reply,
					"ERR CONFIG subcommand must be one of GET, SET, RESETSTAT");
			return 0;
		}
		if (arg0 == "resetstat")
		{
			if (cmd.size() != 1)
			{
				fill_error_reply(ctx.reply,
						"ERR Wrong number of arguments for CONFIG RESETSTAT");
				return 0;
			}
		} else if (arg0 == "get")
		{
			if (cmd.size() != 2)
			{
				fill_error_reply(ctx.reply,
						"ERR Wrong number of arguments for CONFIG GET");
				return 0;
			}
			ctx.reply.type = REDIS_REPLY_ARRAY;
			Properties::iterator it = m_cfg_props.begin();
			while (it != m_cfg_props.end())
			{
				if (fnmatch(cmd[1].c_str(), it->first.c_str(), 0) == 0)
				{
					ctx.reply.elements.push_back(RedisReply(it->first));
					ctx.reply.elements.push_back(RedisReply(it->second));
				}
				it++;
			}
		} else if (arg0 == "set")
		{
			if (cmd.size() != 3)
			{
				fill_error_reply(ctx.reply,
						"RR Wrong number of arguments for CONFIG SET");
				return 0;
			}
			m_cfg_props[cmd[1]] = cmd[2];
			ParseConfig(m_cfg_props, m_cfg);
			fill_status_reply(ctx.reply, "OK");
		}
		return 0;
	}

	int ArdbServer::Time(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		uint64 micros = get_current_epoch_micros();
		ValueArray vs;
		vs.push_back(ValueObject((int64) micros / 1000000));
		vs.push_back(ValueObject((int64) micros % 1000000));
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::FlushDB(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->FlushDB(ctx.currentDB);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::FlushAll(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->FlushAll();
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::Rename(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->Rename(ctx.currentDB, cmd[0], cmd[1]);
		if (ret < 0)
		{
			fill_error_reply(ctx.reply, "ERR no such key");
		} else
		{
			fill_status_reply(ctx.reply, "OK");
		}
		return 0;
	}

	int ArdbServer::Sort(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ValueArray vs;
		std::string key = cmd[0];
		int ret = m_db->Sort(ctx.currentDB, key, cmd, vs);
		if (ret < 0)
		{
			fill_error_reply(ctx.reply,
					"Invalid SORT command or invalid state for SORT.");
		} else
		{
			fill_array_reply(ctx.reply, vs);
		}
		return 0;
	}

	int ArdbServer::RenameNX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->RenameNX(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret < 0 ? 0 : 1);
		return 0;
	}

	int ArdbServer::Move(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->Move(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret < 0 ? 0 : 1);
		return 0;
	}

	int ArdbServer::Type(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->Type(ctx.currentDB, cmd[0]);
		switch (ret)
		{
			case SET_ELEMENT:
			{
				fill_status_reply(ctx.reply, "set");
				break;
			}
			case LIST_META:
			{
				fill_status_reply(ctx.reply, "list");
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				fill_status_reply(ctx.reply, "zset");
				break;
			}
			case HASH_FIELD:
			{
				fill_status_reply(ctx.reply, "hash");
				break;
			}
			case KV:
			{
				fill_status_reply(ctx.reply, "string");
				break;
			}
			case TABLE_META:
			{
				fill_status_reply(ctx.reply, "table");
				break;
			}
			default:
			{
				fill_status_reply(ctx.reply, "none");
				break;
			}
		}
		return 0;
	}

	int ArdbServer::Persist(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->Persist(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret == 0 ? 1 : 0);
		return 0;
	}

	int ArdbServer::Expire(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_int_reply(ctx.reply, 1);
		return 0;
	}

	int ArdbServer::Expireat(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_int_reply(ctx.reply, 1);
		return 0;
	}

	int ArdbServer::Exists(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		bool ret = m_db->Exists(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret ? 1 : 0);
		return 0;
	}

	int ArdbServer::Del(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray array;
		ArgumentArray::iterator it = cmd.begin();
		while (it != cmd.end())
		{
			array.push_back(*it);
			it++;
		}
		m_db->Del(ctx.currentDB, array);
		fill_int_reply(ctx.reply, array.size());
		return 0;
	}

	int ArdbServer::Set(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		const std::string& key = cmd[0];
		const std::string& value = cmd[1];
		int ret = 0;
		if (cmd.size() == 2)
		{
			ret = m_db->Set(ctx.currentDB, key, value);
		} else
		{
			uint32 i = 0;
			uint64 px = 0, ex = 0;
			for (i = 2; i < cmd.size(); i++)
			{
				std::string tmp = string_tolower(cmd[i]);
				if (tmp == "ex" || tmp == "px")
				{
					int64 iv;
					if (!raw_toint64(cmd[i + 1].c_str(), cmd[i + 1].size(), iv)
							|| iv < 0)
					{
						fill_error_reply(ctx.reply,
								"ERR value is not an integer or out of range");
						return 0;
					}
					if (tmp == "px")
					{
						px = iv;
					} else
					{
						ex = iv;
					}
					i++;
				} else
				{
					break;
				}
			}
			bool hasnx, hasxx;
			bool syntaxerror = false;
			if (i < cmd.size() - 1)
			{
				syntaxerror = true;
			}
			if (i == cmd.size() - 1)
			{
				std::string cmp = string_tolower(cmd[i]);
				if (cmp != "nx" && cmp != "xx")
				{
					syntaxerror = true;
				} else
				{
					hasnx = cmp == "nx";
					hasxx = cmp == "xx";
				}
			}
			if (syntaxerror)
			{
				fill_error_reply(ctx.reply, "ERR syntax error");
				return 0;
			}
			int nxx = 0;
			if (hasnx)
			{
				nxx = -1;
			}
			if (hasxx)
			{
				nxx = 1;
			}
			ret = m_db->Set(ctx.currentDB, key, value, ex, px, nxx);
		}
		if (0 == ret)
		{
			fill_status_reply(ctx.reply, "OK");
		} else
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		}
		return 0;
	}

	int ArdbServer::Get(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		const std::string& key = cmd[0];
		std::string value;
		if (0 == m_db->Get(ctx.currentDB, key, &value))
		{
			fill_str_reply(ctx.reply, value);
			//ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		}
		return 0;
	}

	int ArdbServer::SetEX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		uint32 secs;
		if (!string_touint32(cmd[1], secs))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_db->SetEx(ctx.currentDB, cmd[0], cmd[2], secs);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::SetNX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->SetNX(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}
	int ArdbServer::SetRange(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int32 offset;
		if (!string_toint32(cmd[1], offset))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->SetRange(ctx.currentDB, cmd[0], offset, cmd[2]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}
	int ArdbServer::Strlen(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->Strlen(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SetBit(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int32 offset;
		if (!string_toint32(cmd[1], offset))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		if (cmd[2] != "1" && cmd[2] != "0")
		{
			fill_error_reply(ctx.reply,
					"ERR bit is not an integer or out of range");
			return 0;
		}
		uint8 bit = cmd[2] != "1";
		int ret = m_db->SetBit(ctx.currentDB, cmd[0], offset, bit);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::PSetEX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		uint32 mills;
		if (!string_touint32(cmd[1], mills))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_db->PSetEx(ctx.currentDB, cmd[0], cmd[2], mills);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::MSetNX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if (cmd.size() % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for MSETNX");
			return 0;
		}
		SliceArray keys;
		SliceArray vals;
		for (uint32 i = 0; i < cmd.size(); i += 2)
		{
			keys.push_back(cmd[i]);
			vals.push_back(cmd[i + 1]);
		}
		int count = m_db->MSetNX(ctx.currentDB, keys, vals);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::MSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if (cmd.size() % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for MSET");
			return 0;
		}
		SliceArray keys;
		SliceArray vals;
		for (uint32 i = 0; i < cmd.size(); i += 2)
		{
			keys.push_back(cmd[i]);
			vals.push_back(cmd[i + 1]);
		}
		m_db->MSet(ctx.currentDB, keys, vals);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::MGet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueArray res;
		m_db->MGet(ctx.currentDB, keys, res);
		fill_array_reply(ctx.reply, res);
		return 0;
	}

	int ArdbServer::IncrbyFloat(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		double increment, val;
		if (!string_todouble(cmd[1], increment))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not a float or out of range");
			return 0;
		}
		int ret = m_db->IncrbyFloat(ctx.currentDB, cmd[0], increment, val);
		if (ret == 0)
		{
			fill_double_reply(ctx.reply, val);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR value is not a float or out of range");
		}
		return 0;
	}

	int ArdbServer::Incrby(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64 increment, val;
		if (!string_toint64(cmd[1], increment))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->Incrby(ctx.currentDB, cmd[0], increment, val);
		if (ret == 0)
		{
			fill_int_reply(ctx.reply, val);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
		}
		return 0;
	}

	int ArdbServer::Incr(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64_t val;
		int ret = m_db->Incr(ctx.currentDB, cmd[0], val);
		if (ret == 0)
		{
			fill_int_reply(ctx.reply, val);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
		}
		return 0;
	}

	int ArdbServer::GetSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string v;
		int ret = m_db->GetSet(ctx.currentDB, cmd[0], cmd[1], v);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_str_reply(ctx.reply, v);
		}
		return 0;
	}

	int ArdbServer::GetRange(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int32 start, end;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], end))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		std::string v;
		m_db->GetRange(ctx.currentDB, cmd[0], start, end, v);
		fill_str_reply(ctx.reply, v);
		return 0;
	}

	int ArdbServer::GetBit(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int32 offset;
		if (!string_toint32(cmd[1], offset))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->GetBit(ctx.currentDB, cmd[0], offset);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::Decrby(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64 decrement, val;
		if (!string_toint64(cmd[1], decrement))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->Decrby(ctx.currentDB, cmd[0], decrement, val);
		if (ret == 0)
		{
			fill_int_reply(ctx.reply, val);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
		}
		return 0;
	}

	int ArdbServer::Decr(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64_t val;
		int ret = m_db->Decr(ctx.currentDB, cmd[0], val);
		if (ret == 0)
		{
			fill_int_reply(ctx.reply, val);
		} else
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
		}
		return 0;
	}

	int ArdbServer::Bitop(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 2; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		int ret = m_db->BitOP(ctx.currentDB, cmd[0], cmd[1], keys);
		if (ret < 0)
		{
			fill_error_reply(ctx.reply, "ERR syntax error");
		} else
		{
			fill_int_reply(ctx.reply, ret);
		}
		return 0;
	}

	int ArdbServer::Bitcount(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if (cmd.size() == 2)
		{
			fill_error_reply(ctx.reply, "ERR syntax error");
			return 0;
		}
		int count = 0;
		if (cmd.size() == 1)
		{
			count = m_db->BitCount(ctx.currentDB, cmd[0], 0, -1);
		} else
		{
			int32 start, end;
			if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], end))
			{
				fill_error_reply(ctx.reply,
						"ERR value is not an integer or out of range");
				return 0;
			}
			count = m_db->BitCount(ctx.currentDB, cmd[0], start, end);
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::Append(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		const std::string& key = cmd[0];
		const std::string& value = cmd[1];
		int ret = m_db->Append(ctx.currentDB, key, value);
		if (ret > 0)
		{
			fill_int_reply(ctx.reply, ret);
		} else
		{
			fill_error_reply(ctx.reply, "ERR failed to append key:%s",
					key.c_str());
		}
		return 0;
	}

	int ArdbServer::Ping(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "PONG");
		return 0;
	}
	int ArdbServer::Echo(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_str_reply(ctx.reply, cmd[0]);
		return 0;
	}
	int ArdbServer::Select(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ctx.currentDB = cmd[0];
		m_clients_holder.ChangeCurrentDB(ctx.conn, ctx.currentDB);
		fill_status_reply(ctx.reply, "OK");
		DEBUG_LOG("Select db is %s", cmd[0].c_str());
		return 0;
	}

	int ArdbServer::Quit(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		fill_status_reply(ctx.reply, "OK");
		return -1;
	}

	int ArdbServer::Shutdown(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_service->Stop();
		return -1;
	}

	int ArdbServer::Slaveof(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		const std::string& host = cmd[0];
		uint32 port = 0;
		if (!string_touint32(cmd[1], port))
		{
			if (!strcasecmp(cmd[0].c_str(), "no")
					&& !strcasecmp(cmd[1].c_str(), "one"))
			{
				fill_status_reply(ctx.reply, "OK");
				m_slave_client.Stop();
				return 0;
			}
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_slave_client.ConnectMaster(host, port);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::ReplConf(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		INFO_LOG("%s %s", cmd[0].c_str(), cmd[1].c_str());
		if (cmd.size() % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for ReplConf");
			return 0;
		}
		for (uint32 i = 0; i < cmd.size(); i += 2)
		{
			if (cmd[i] == "listening-port")
			{
				uint32 port = 0;
				string_touint32(cmd[i + 1], port);
				Address* addr =
						const_cast<Address*>(ctx.conn->GetRemoteAddress());
				if (InstanceOf<SocketHostAddress>(addr).OK)
				{
					SocketHostAddress* tmp = (SocketHostAddress*) addr;
					const SocketHostAddress& master_addr =
							m_slave_client.GetMasterAddress();
					if (master_addr.GetHost() == tmp->GetHost()
							&& master_addr.GetPort() == port)
					{
						m_repli_serv.MarkMasterSlave(ctx.conn);
					}
				}

			}
		}
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::Sync(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_repli_serv.ServSlaveClient(ctx.conn);
		return 0;
	}

	int ArdbServer::ARSync(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string& serverKey = cmd[0];
		uint64 seq;
		if (!string_touint64(cmd[1], seq))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_repli_serv.ServARSlaveClient(ctx.conn, serverKey, seq);
		return 0;
	}

	int ArdbServer::HMSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if ((cmd.size() - 1) % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for HMSet");
			return 0;
		}
		SliceArray fs;
		SliceArray vals;
		for (uint32 i = 1; i < cmd.size(); i += 2)
		{
			fs.push_back(cmd[i]);
			vals.push_back(cmd[i + 1]);
		}
		m_db->HMSet(ctx.currentDB, cmd[0], fs, vals);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}
	int ArdbServer::HSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		m_db->HSet(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
		fill_int_reply(ctx.reply, 1);
		return 0;
	}
	int ArdbServer::HSetNX(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->HSetNX(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}
	int ArdbServer::HVals(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		StringArray keys;
		m_db->HVals(ctx.currentDB, cmd[0], keys);
		fill_str_array_reply(ctx.reply, keys);
		return 0;
	}

	int ArdbServer::HMGet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ValueArray vals;
		SliceArray fs;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			fs.push_back(cmd[i]);
		}
		m_db->HMGet(ctx.currentDB, cmd[0], fs, vals);
		fill_array_reply(ctx.reply, vals);
		return 0;
	}

	int ArdbServer::HLen(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int len = m_db->HLen(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, len);
		return 0;
	}

	int ArdbServer::HKeys(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		StringArray keys;
		m_db->HKeys(ctx.currentDB, cmd[0], keys);
		fill_str_array_reply(ctx.reply, keys);
		return 0;
	}

	int ArdbServer::HIncrbyFloat(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		double increment, val = 0;
		if (!string_todouble(cmd[2], increment))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not a float or out of range");
			return 0;
		}
		m_db->HIncrbyFloat(ctx.currentDB, cmd[0], cmd[1], increment, val);
		fill_double_reply(ctx.reply, val);
		return 0;
	}

	int ArdbServer::HMIncrby(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if ((cmd.size() - 1) % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for HMIncrby");
			return 0;
		}
		SliceArray fs;
		Int64Array incs;
		for (uint32 i = 1; i < cmd.size(); i += 2)
		{
			fs.push_back(cmd[i]);
			int64 v = 0;
			if (!string_toint64(cmd[i + 1], v))
			{
				fill_error_reply(ctx.reply,
						"ERR value is not a integer or out of range");
				return 0;
			}
			incs.push_back(v);
		}
		Int64Array vs;
		m_db->HMIncrby(ctx.currentDB, cmd[0], fs, incs, vs);
		fill_int_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::HIncrby(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int64 increment, val = 0;
		if (!string_toint64(cmd[2], increment))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_db->HIncrby(ctx.currentDB, cmd[0], cmd[1], increment, val);
		fill_int_reply(ctx.reply, val);
		return 0;
	}

	int ArdbServer::HGetAll(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		StringArray fields;
		ValueArray results;
		m_db->HGetAll(ctx.currentDB, cmd[0], fields, results);
		ctx.reply.type = REDIS_REPLY_ARRAY;
		for (uint32 i = 0; i < fields.size(); i++)
		{
			RedisReply reply1, reply2;
			fill_str_reply(reply1, fields[i]);
			std::string str;
			fill_str_reply(reply2, results[i].ToString(str));
			ctx.reply.elements.push_back(reply1);
			ctx.reply.elements.push_back(reply2);
		}
		return 0;
	}

	int ArdbServer::HGet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string v;
		int ret = m_db->HGet(ctx.currentDB, cmd[0], cmd[1], &v);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_str_reply(ctx.reply, v);
		}
		return 0;
	}

	int ArdbServer::HExists(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->HExists(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::HDel(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray fields;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			fields.push_back(cmd[i]);
		}
		int ret = m_db->HDel(ctx.currentDB, cmd[0], fields);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

//========================Set CMDs==============================
	int ArdbServer::SAdd(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray values;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			values.push_back(cmd[i]);
		}
		int count = m_db->SAdd(ctx.currentDB, cmd[0], values);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::SCard(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->SCard(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret > 0 ? ret : 0);
		return 0;
	}

	int ArdbServer::SDiff(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		m_db->SDiff(ctx.currentDB, keys, vs);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::SDiffStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		int ret = m_db->SDiffStore(ctx.currentDB, cmd[0], keys);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SInter(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		m_db->SInter(ctx.currentDB, keys, vs);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::SInterStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		int ret = m_db->SInterStore(ctx.currentDB, cmd[0], keys);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SIsMember(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->SIsMember(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SMembers(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ValueArray vs;
		m_db->SMembers(ctx.currentDB, cmd[0], vs);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::SMove(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->SMove(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SPop(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string res;
		m_db->SPop(ctx.currentDB, cmd[0], res);
		fill_str_reply(ctx.reply, res);
		return 0;
	}

	int ArdbServer::SRandMember(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		ValueArray vs;
		int32 count = 1;
		if (cmd.size() > 1)
		{
			if (!string_toint32(cmd[1], count))
			{
				fill_error_reply(ctx.reply,
						"ERR value is not an integer or out of range");
				return 0;
			}
		}
		m_db->SRandMember(ctx.currentDB, cmd[0], vs, count);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::SRem(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		int ret = m_db->SRem(ctx.currentDB, cmd[0], keys);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SUnion(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		ValueSet vs;
		m_db->SUnion(ctx.currentDB, keys, vs);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::SUnionStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		int ret = m_db->SUnionStore(ctx.currentDB, cmd[0], keys);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::SUnionCount(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		uint32 count = 0;
		m_db->SUnionCount(ctx.currentDB, keys, count);
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::SInterCount(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		uint32 count = 0;
		m_db->SInterCount(ctx.currentDB, keys, count);
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::SDiffCount(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		for (uint32 i = 0; i < cmd.size(); i++)
		{
			keys.push_back(cmd[i]);
		}
		uint32 count = 0;
		m_db->SDiffCount(ctx.currentDB, keys, count);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

//===========================Sorted Sets cmds==============================
	int ArdbServer::ZAdd(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		if ((cmd.size() - 1) % 2 != 0)
		{
			fill_error_reply(ctx.reply,
					"ERR wrong number of arguments for ZAdd");
			return 0;
		}
		int count = 0;

		for (uint32 i = 1; i < cmd.size(); i += 2)
		{
			double score;
			if (!string_todouble(cmd[i], score))
			{
				fill_error_reply(ctx.reply,
						"ERR value is not a float or out of range");
				return 0;
			}
			count += m_db->ZAdd(ctx.currentDB, cmd[0], score, cmd[i + 1]);
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZCard(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->ZCard(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::ZCount(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->ZCount(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::ZIncrby(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		double increment, value;
		if (!string_todouble(cmd[1], increment))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not a float or out of range");
			return 0;
		}
		m_db->ZIncrby(ctx.currentDB, cmd[0], increment, cmd[2], value);
		return 0;
	}

	int ArdbServer::ZRange(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		bool withscores = false;
		if (cmd.size() == 4)
		{
			if (string_tolower(cmd[3]) != "WITHSCORES")
			{
				fill_error_reply(ctx.reply, "ERR syntax error");
				return 0;
			}
			withscores = true;
		}
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		QueryOptions options;
		options.withscores = withscores;
		ValueArray vs;
		m_db->ZRange(ctx.currentDB, cmd[0], start, stop, vs, options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	static bool process_query_options(ArgumentArray& cmd, uint32 idx,
			QueryOptions& options)
	{
		if (cmd.size() > idx)
		{
			std::string optionstr = string_tolower(cmd[3]);
			if (optionstr != "withscores" && optionstr != "limit")
			{
				return false;
			}
			if (optionstr == "withscores")
			{
				options.withscores = true;
				return process_query_options(cmd, idx + 1, options);
			} else
			{
				if (cmd.size() != idx + 3)
				{
					return false;
				}
				if (!string_toint32(cmd[idx + 1], options.limit_offset)
						|| !string_toint32(cmd[idx + 2], options.limit_count))
				{
					return false;
				}
				options.withlimit = true;
				return true;
			}
		} else
		{
			return false;
		}
	}

	int ArdbServer::ZRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		QueryOptions options;
		if (cmd.size() >= 4)
		{
			bool ret = process_query_options(cmd, 3, options);
			if (!ret)
			{
				fill_error_reply(ctx.reply, "ERR syntax error");
				return 0;
			}
		}

		ValueArray vs;
		m_db->ZRangeByScore(ctx.currentDB, cmd[0], cmd[1], cmd[2], vs, options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::ZRank(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->ZRank(ctx.currentDB, cmd[0], cmd[1]);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_int_reply(ctx.reply, ret);
		}
		return 0;
	}

	int ArdbServer::ZRem(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int count = 0;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			count += m_db->ZRem(ctx.currentDB, cmd[0], cmd[i]);
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZRemRangeByRank(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int count = m_db->ZRemRangeByRank(ctx.currentDB, cmd[0], start, stop);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZRemRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int count = m_db->ZRemRangeByScore(ctx.currentDB, cmd[0], cmd[1],
				cmd[2]);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZRevRange(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		bool withscores = false;
		if (cmd.size() == 4)
		{
			if (string_tolower(cmd[3]) != "WITHSCORES")
			{
				fill_error_reply(ctx.reply, "ERR syntax error");
				return 0;
			}
			withscores = true;
		}
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		QueryOptions options;
		options.withscores = withscores;
		ValueArray vs;
		m_db->ZRevRange(ctx.currentDB, cmd[0], start, stop, vs, options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::ZRevRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		QueryOptions options;
		if (cmd.size() >= 4)
		{
			bool ret = process_query_options(cmd, 3, options);
			if (!ret)
			{
				fill_error_reply(ctx.reply, "ERR syntax error");
				return 0;
			}
		}

		ValueArray vs;
		m_db->ZRevRangeByScore(ctx.currentDB, cmd[0], cmd[1], cmd[2], vs,
				options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::ZRevRank(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->ZRevRank(ctx.currentDB, cmd[0], cmd[1]);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_int_reply(ctx.reply, ret);
		}
		return 0;
	}

	static bool process_zstore_args(ArgumentArray& cmd, SliceArray& keys,
			WeightArray& ws, AggregateType& type)
	{
		int numkeys;
		if (!string_toint32(cmd[1], numkeys) || numkeys <= 0)
		{
			return false;
		}
		if (cmd.size() < (uint32) numkeys + 2)
		{
			return false;
		}
		for (int i = 2; i < numkeys + 2; i++)
		{
			keys.push_back(cmd[i]);
		}
		if (cmd.size() > (uint32) numkeys + 2)
		{
			uint32 idx = numkeys + 2;
			std::string opstr = string_tolower(cmd[numkeys + 2]);
			if (opstr == "weights")
			{
				if (cmd.size() < ((uint32) numkeys * 2) + 3)
				{
					return false;
				}
				uint32 weight;
				for (int i = idx + 1; i < (numkeys * 2) + 3; i++)
				{
					if (!string_touint32(cmd[i], weight))
					{
						return false;
					}
					ws.push_back(weight);
				}
				idx = numkeys * 2 + 3;
				if (cmd.size() <= idx)
				{
					return true;
				}
				opstr = string_tolower(cmd[idx]);
			}
			if (cmd.size() > (idx + 1) && opstr == "aggregate")
			{
				std::string typestr = string_tolower(cmd[idx + 1]);
				if (typestr == "sum")
				{
					type = AGGREGATE_SUM;
				} else if (typestr == "max")
				{
					type = AGGREGATE_MAX;
				} else if (typestr == "min")
				{
					type = AGGREGATE_MIN;
				} else
				{
					return false;
				}
			} else
			{
				return false;
			}
		}
		return true;
	}

	int ArdbServer::ZInterStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		WeightArray ws;
		AggregateType type = AGGREGATE_SUM;
		if (!process_zstore_args(cmd, keys, ws, type))
		{
			fill_error_reply(ctx.reply, "ERR syntax error");
			return 0;
		}
		int count = m_db->ZInterStore(ctx.currentDB, cmd[0], keys, ws, type);
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::ZUnionStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		WeightArray ws;
		AggregateType type = AGGREGATE_SUM;
		if (!process_zstore_args(cmd, keys, ws, type))
		{
			fill_error_reply(ctx.reply, "ERR syntax error");
			return 0;
		}
		int count = m_db->ZUnionStore(ctx.currentDB, cmd[0], keys, ws, type);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZScore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		double score;
		int ret = m_db->ZScore(ctx.currentDB, cmd[0], cmd[1], score);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_double_reply(ctx.reply, score);
		}
		return 0;
	}

//=========================Lists cmds================================
	int ArdbServer::LIndex(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int index;
		if (!string_toint32(cmd[1], index))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		std::string v;
		int ret = m_db->LIndex(ctx.currentDB, cmd[0], index, v);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_str_reply(ctx.reply, v);
		}
		return 0;
	}

	int ArdbServer::LInsert(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->LInsert(ctx.currentDB, cmd[0], cmd[1], cmd[2], cmd[3]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::LLen(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->LLen(ctx.currentDB, cmd[0]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::LPop(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string v;
		int ret = m_db->LPop(ctx.currentDB, cmd[0], v);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_str_reply(ctx.reply, v);
		}
		return 0;
	}
	int ArdbServer::LPush(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int count = 0;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			count = m_db->LPush(ctx.currentDB, cmd[0], cmd[i]);
		}
		if (count < 0)
		{
			count = 0;
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::LPushx(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->LPushx(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::LRange(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		ValueArray vs;
		m_db->LRange(ctx.currentDB, cmd[0], start, stop, vs);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}
	int ArdbServer::LRem(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int count;
		if (!string_toint32(cmd[1], count))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->LRem(ctx.currentDB, cmd[0], count, cmd[2]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}
	int ArdbServer::LSet(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int index;
		if (!string_toint32(cmd[1], index))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		int ret = m_db->LSet(ctx.currentDB, cmd[0], index, cmd[2]);
		if (ret < 0)
		{
			fill_error_reply(ctx.reply, "ERR index out of range");
		} else
		{
			fill_status_reply(ctx.reply, "OK");
		}
		return 0;
	}

	int ArdbServer::LTrim(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
		{
			fill_error_reply(ctx.reply,
					"ERR value is not an integer or out of range");
			return 0;
		}
		m_db->LTrim(ctx.currentDB, cmd[0], start, stop);
		fill_status_reply(ctx.reply, "OK");
		return 0;
	}

	int ArdbServer::RPop(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string v;
		int ret = m_db->RPop(ctx.currentDB, cmd[0], v);
		if (ret < 0)
		{
			ctx.reply.type = REDIS_REPLY_NIL;
		} else
		{
			fill_str_reply(ctx.reply, v);
		}
		return 0;
	}
	int ArdbServer::RPush(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int count = 0;
		for (uint32 i = 1; i < cmd.size(); i++)
		{
			count = m_db->RPush(ctx.currentDB, cmd[0], cmd[i]);
		}
		if (count < 0)
		{
			count = 0;
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::RPushx(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		int ret = m_db->RPushx(ctx.currentDB, cmd[0], cmd[1]);
		fill_int_reply(ctx.reply, ret);
		return 0;
	}

	int ArdbServer::RPopLPush(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		std::string v;
		m_db->RPopLPush(ctx.currentDB, cmd[0], cmd[1], v);
		fill_str_reply(ctx.reply, v);
		return 0;
	}

	ArdbServer::RedisCommandHandlerSetting* ArdbServer::FindRedisCommandHandlerSetting(
			std::string& cmd)
	{
		RedisCommandHandlerSettingTable::iterator found = m_handler_table.find(
				cmd);
		if (found != m_handler_table.end())
		{
			return &(found->second);
		}
		return NULL;
	}

	static bool is_sort_write_cmd(RedisCommandFrame& cmd)
	{
		if (cmd.GetCommand() == "sort")
		{
			ArgumentArray::iterator it = cmd.GetArguments().begin();
			while (it != cmd.GetArguments().end())
			{
				if (*it == "store")
				{
					return true;
				}
				it++;
			}
		}
		return false;
	}

	void ArdbServer::ProcessRedisCommand(ArdbConnContext& ctx,
			RedisCommandFrame& args)
	{
		m_current_ctx = &ctx;
		std::string& cmd = args.GetCommand();
		lower_string(cmd);
		RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(
				cmd);
		DEBUG_LOG("Process recved cmd:%s", cmd.c_str());
		int ret = 0;
		if (NULL != setting)
		{
			/**
			 * Return error if ardb is saving data
			 */
			if (m_repli_serv.IsSavingData())
			{
				if (setting->read_write_cmd == 1
						|| (setting->read_write_cmd == 2
								&& is_sort_write_cmd(args)))
				{
					fill_error_reply(ctx.reply, "ERR server is saving data");
					goto _exit;
				}
			}
			bool valid_cmd = true;
			if (setting->min_arity > 0)
			{
				valid_cmd = args.GetArguments().size()
						>= (uint32) setting->min_arity;
			}
			if (setting->max_arity >= 0 && valid_cmd)
			{
				valid_cmd = args.GetArguments().size()
						<= (uint32) setting->max_arity;
			}

			if (!valid_cmd)
			{
				fill_error_reply(ctx.reply,
						"ERR wrong number of arguments for '%s' command",
						cmd.c_str());
			} else
			{
				if (ctx.IsInTransaction()
						&& (cmd != "multi" && cmd != "exec" && cmd != "discard"
								&& cmd != "quit"))
				{
					ctx.transaction_cmds->push_back(args);
					fill_status_reply(ctx.reply, "QUEUED");
					ctx.conn->Write(ctx.reply);
					return;
				} else if (ctx.IsSubscribedConn()
						&& (cmd != "subscribe" && cmd != "psubscribe"
								&& cmd != "unsubscribe" && cmd != "punsubscribe"
								&& cmd != "quit"))
				{
					fill_error_reply(ctx.reply,
							"ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
				} else
				{
					ret = DoRedisCommand(ctx, setting, args);
				}
			}
		} else
		{
			ERROR_LOG("No handler found for:%s", cmd.c_str());
			fill_error_reply(ctx.reply, "ERR unknown command '%s'",
					cmd.c_str());
		}

		_exit: if (ctx.reply.type != 0)
		{
			ctx.conn->Write(ctx.reply);
			ctx.reply.Clear();
		}
		if (ret < 0)
		{
			ctx.conn->Close();
		}
	}

	void ArdbServer::HandleReply(ArdbConnContext* ctx)
	{
		if (ctx->closed)
		{
			ctx->TryDelete();
		} else
		{
			if (ctx->reply.type != 0)
			{
				ctx->conn->Write(ctx->reply);
			}
			ctx->reply.Clear();
		}
	}

	int ArdbServer::DoRedisCommand(ArdbConnContext& ctx,
			RedisCommandHandlerSetting* setting, RedisCommandFrame& args)
	{
		std::string& cmd = args.GetCommand();
		if (m_clients_holder.IsStatEnable())
		{
			m_clients_holder.TouchConn(ctx.conn, cmd);
		}
		if (m_cfg.batch_write_enable)
		{
			InsertBatchWriteDBID(ctx.currentDB);
		}
		uint64 start_time = get_current_epoch_micros();
		int ret = (this->*(setting->handler))(ctx, args.GetArguments());
		uint64 stop_time = get_current_epoch_micros();

		if (m_cfg.slowlog_log_slower_than
				&& (stop_time - start_time) > m_cfg.slowlog_log_slower_than)
		{
			m_slowlog_handler.PushSlowCommand(args, stop_time - start_time);
		}
		return ret;
	}

	typedef ChannelUpstreamHandler<RedisCommandFrame>* handler_creater();

	static void conn_pipeline_init(ChannelPipeline* pipeline, void* data)
	{
		ArdbServer* serv = (ArdbServer*) data;
		pipeline->AddLast("decoder", new RedisCommandDecoder);
		pipeline->AddLast("encoder", new RedisReplyEncoder);
		pipeline->AddLast("handler", new RedisRequestHandler(serv));
	}

	static void conn_pipeline_finallize(ChannelPipeline* pipeline, void* data)
	{
		ChannelHandler* handler = pipeline->Get("decoder");
		DELETE(handler);
		handler = pipeline->Get("encoder");
		DELETE(handler);
		handler = pipeline->Get("handler");
		DELETE(handler);
	}

	void RedisRequestHandler::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisCommandFrame>& e)
	{
		if (NULL == ardbctx)
		{
			ardbctx = new ArdbConnContext;
		}
		ardbctx->conn = ctx.GetChannel();
		server->ProcessRedisCommand(*ardbctx, *(e.GetMessage()));
	}
	void RedisRequestHandler::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		server->m_clients_holder.EraseConn(ctx.GetChannel());
		server->ClearWatchKeys(*ardbctx);
		server->ClearSubscribes(*ardbctx);
		ardbctx->closed = true;
		ardbctx->TryDelete();
	}

	static void daemonize(void)
	{
		int fd;

		if (fork() != 0)
		{
			exit(0); /* parent exits */
		}
		setsid(); /* create a new session */

		if ((fd = open("/dev/null", O_RDWR, 0)) != -1)
		{
			dup2(fd, STDIN_FILENO);
			dup2(fd, STDOUT_FILENO);
			dup2(fd, STDERR_FILENO);
			if (fd > STDERR_FILENO)
			{
				close(fd);
			}
		}
	}

	void ArdbServer::Run()
	{
		BatchWriteFlush();
	}

	void ArdbServer::BatchWriteFlush()
	{
		DBIDSet::iterator it = m_period_batch_dbids.begin();
		while (it != m_period_batch_dbids.end())
		{
			m_db->GetDB(*it)->CommitBatchWrite();
			it++;
		}
		m_period_batch_dbids.clear();
	}

	void ArdbServer::InsertBatchWriteDBID(const DBID& id)
	{
		bool isnew = m_period_batch_dbids.insert(id).second;
		if (isnew)
		{
			m_db->GetDB(id)->BeginBatchWrite();
		}
	}

	Timer& ArdbServer::GetTimer()
	{
		return m_service->GetTimer();
	}

	int ArdbServer::Start(const Properties& props)
	{
		m_cfg_props = props;
		if (ParseConfig(props, m_cfg) < 0)
		{
			ERROR_LOG("Failed to parse configurations.");
			return -1;
		}
		if (m_cfg.daemonize)
		{
			daemonize();
		}

		m_engine = new SelectedDBEngineFactory(props);
		m_db = new Ardb(m_engine, m_cfg.data_base_path);
		m_service = new ChannelService(m_cfg.max_clients + 32);

		ChannelOptions ops;
		ops.tcp_nodelay = true;
		if (m_cfg.tcp_keepalive > 0)
		{
			ops.keep_alive = m_cfg.tcp_keepalive;
		}
		if (m_cfg.listen_host.empty() && m_cfg.listen_unix_path.empty())
		{
			m_cfg.listen_host = "0.0.0.0";
			if (m_cfg.listen_port == 0)
			{
				m_cfg.listen_port = 6379;
			}
		}

		if (!m_cfg.listen_host.empty())
		{
			SocketHostAddress address(m_cfg.listen_host.c_str(),
					m_cfg.listen_port);
			ServerSocketChannel* server = m_service->NewServerSocketChannel();
			if (!server->Bind(&address))
			{
				ERROR_LOG(
						"Failed to bind on %s:%d", m_cfg.listen_host.c_str(), m_cfg.listen_port);
				goto sexit;
			}
			server->Configure(ops);
			server->SetChannelPipelineInitializor(conn_pipeline_init, this);
			server->SetChannelPipelineFinalizer(conn_pipeline_finallize, NULL);
		}
		if (!m_cfg.listen_unix_path.empty())
		{
			SocketUnixAddress address(m_cfg.listen_unix_path);
			ServerSocketChannel* server = m_service->NewServerSocketChannel();
			if (!server->Bind(&address))
			{
				ERROR_LOG(
						"Failed to bind on %s", m_cfg.listen_unix_path.c_str());
				goto sexit;
			}
			server->Configure(ops);
			server->SetChannelPipelineInitializor(conn_pipeline_init, this);
			server->SetChannelPipelineFinalizer(conn_pipeline_finallize, NULL);
			chmod(m_cfg.listen_unix_path.c_str(), m_cfg.unixsocketperm);
		}
		ArdbLogger::InitDefaultLogger(m_cfg.loglevel, m_cfg.logfile);

		if (m_cfg.batch_write_enable)
		{
			m_service->GetTimer().Schedule(this, m_cfg.batch_flush_period,
					m_cfg.batch_flush_period, SECONDS);
		}
		if (!m_cfg.single)
		{
			m_repli_serv.Init();
			m_repli_serv.Start();
			m_db->RegisterRawKeyListener(&m_repli_serv);
			if (!m_cfg.master_host.empty())
			{
				m_slave_client.ConnectMaster(m_cfg.master_host,
						m_cfg.master_port);
			}
		}
		m_service->SetThreadPoolSize(m_cfg.worker_count);
		INFO_LOG("Server started, Ardb version %s", ARDB_VERSION);
		INFO_LOG(
				"The server is now ready to accept connections on port %d", m_cfg.listen_port);
		m_service->Start();
		sexit: m_repli_serv.Stop();
		DELETE(m_db);
		DELETE(m_engine);
		DELETE(m_service);
		ArdbLogger::DestroyDefaultLogger();
		return 0;
	}
}


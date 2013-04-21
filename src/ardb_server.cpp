/*
 * ardb_server.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */
#include "ardb_server.hpp"
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include <stdarg.h>
#include <fnmatch.h>
//#define __USE_KYOTOCABINET__ 1
#ifdef __USE_KYOTOCABINET__
#include "engine/kyotocabinet_engine.hpp"
#else
#include "engine/leveldb_engine.hpp"
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

static inline void fill_status_reply(RedisReply& reply, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char buf[1024];
	vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
	va_end(ap);
	reply.type = REDIS_REPLY_STATUS;
	reply.str = buf;
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
		}
		else
		{
			fill_str_reply(r, vo.ToString());
		}
		reply.elements.push_back(r);
		it++;
	}
}

static inline void fill_str_array_reply(RedisReply& reply, StringArray& v)
{
	reply.type = REDIS_REPLY_ARRAY;
	StringArray::iterator it = v.begin();
	while (it != v.end())
	{
		RedisReply r;
		fill_str_reply(r, *it);
		reply.elements.push_back(r);
		it++;
	}
}



int ArdbServer::ParseConfig(const Properties& props, ArdbServerConfig& cfg)
{
	conf_get_int64(props, "port", cfg.listen_port);
	conf_get_int64(props, "unixsocketperm", cfg.unixsocketperm);
	conf_get_int64(props, "slowlog-log-slower-than", cfg.slowlog_log_slower_than);
	conf_get_int64(props, "slowlog-max-len", cfg.slowlog_max_len);
	conf_get_string(props, "bind", cfg.listen_host);
	conf_get_string(props, "unixsocket", cfg.listen_unix_path);
	conf_get_string(props, "dir", cfg.data_base_path);
	conf_get_string(props, "loglevel", cfg.loglevel);
	conf_get_string(props, "logfile", cfg.logfile);
	std::string daemonize;
	conf_get_string(props, "daemonize", daemonize);
	daemonize = string_tolower(daemonize);
	if (daemonize == "yes")
	{
		cfg.daemonize = true;
	}
	if(cfg.data_base_path.empty()){
		cfg.data_base_path = ".";
	}
	return 0;
}

ArdbServer::ArdbServer() :
		m_service(NULL), m_db(NULL), m_engine(NULL),m_slowlog_handler(m_cfg)
{
	struct RedisCommandHandlerSetting settingTable[] =
	{
	{ "ping", &ArdbServer::Ping, 0, 0 },
	{ "info", &ArdbServer::Info, 0, 1 },
	{ "slowlog", &ArdbServer::SlowLog, 1, 2 },
	{ "dbsize", &ArdbServer::DBSize, 0, 0 },
	{ "config", &ArdbServer::Config, 1, 3 },
	{ "client", &ArdbServer::Client, 1, 3 },
	{ "flushdb", &ArdbServer::FlushDB, 0, 0 },
	{ "flushall", &ArdbServer::FlushAll, 0, 0 },
	{ "time", &ArdbServer::Time, 0, 0 },
	{ "echo", &ArdbServer::Echo, 1, 1 },
	{ "quit", &ArdbServer::Quit, 0, 0 },
	{ "shutdown", &ArdbServer::Shutdown, 0, 1 },
	{ "slaveof", &ArdbServer::Slaveof, 2, 2 },
	{ "select", &ArdbServer::Select, 1, 1 },
	{ "append", &ArdbServer::Append, 2, 2 },
	{ "get", &ArdbServer::Get, 1, 1 },
	{ "set", &ArdbServer::Set, 2, 7 },
	{ "del", &ArdbServer::Del, 1, -1 },
	{ "exists", &ArdbServer::Exists, 1, 1 },
	{ "expire", &ArdbServer::Expire, 2, 2 },
	{ "expireat", &ArdbServer::Expireat, 2, 2 },
	{ "persist", &ArdbServer::Persist, 1, 1 },
	{ "type", &ArdbServer::Type, 1, 1 },
	{ "bitcount", &ArdbServer::Bitcount, 1, 3 },
	{ "bitop", &ArdbServer::Bitop, 3, -1 },
	{ "decr", &ArdbServer::Decr, 1, 1 },
	{ "decrby", &ArdbServer::Decrby, 2, 2 },
	{ "getbit", &ArdbServer::GetBit, 2, 2 },
	{ "getrange", &ArdbServer::GetRange, 3, 3 },
	{ "getset", &ArdbServer::GetSet, 2, 2 },
	{ "incr", &ArdbServer::Incr, 1, 1 },
	{ "incrby", &ArdbServer::Incrby, 2, 2 },
	{ "incrbyfloat", &ArdbServer::IncrbyFloat, 2, 2 },
	{ "mget", &ArdbServer::MGet, 1, -1 },
	{ "mset", &ArdbServer::MSet, 2, -1 },
	{ "msetnx", &ArdbServer::MSetNX, 2, -1 },
	{ "psetex", &ArdbServer::MSetNX, 3, 3 },
	{ "setbit", &ArdbServer::SetBit, 3, 3 },
	{ "setex", &ArdbServer::SetEX, 3, 3 },
	{ "setnx", &ArdbServer::SetNX, 2, 2 },
	{ "setrange", &ArdbServer::SetRange, 3, 3 },
	{ "strlen", &ArdbServer::Strlen, 1, 1 },
	{ "hdel", &ArdbServer::HDel, 2, -1 },
	{ "hexists", &ArdbServer::HExists, 2, 2 },
	{ "hget", &ArdbServer::HGet, 2, 2 },
	{ "hgetall", &ArdbServer::HGetAll, 1, 1 },
	{ "hincr", &ArdbServer::HIncrby, 3, 3 },
	{ "hincrbyfloat", &ArdbServer::HIncrbyFloat, 3, 3 },
	{ "hkeys", &ArdbServer::HKeys, 1, 1 },
	{ "hlen", &ArdbServer::HLen, 1, 1 },
	{ "hvals", &ArdbServer::HVals, 1, 1 },
	{ "hmget", &ArdbServer::HMGet, 2, -1 },
	{ "hset", &ArdbServer::HSet, 3, 3 },
	{ "hsetnx", &ArdbServer::HSetNX, 3, 3 },
	{ "hmset", &ArdbServer::HMSet, 3, -1 },
	{ "scard", &ArdbServer::SCard, 1, 1 },
	{ "sadd", &ArdbServer::SAdd, 2, -1 },
	{ "sdiff", &ArdbServer::SDiff, 2, -1 },
	{ "sdiffstore", &ArdbServer::SDiffStore, 3, -1 },
	{ "sinter", &ArdbServer::SInter, 2, -1 },
	{ "sinterstore", &ArdbServer::SInterStore, 3, -1 },
	{ "sismember", &ArdbServer::SIsMember, 2, 2 },
	{ "smembers", &ArdbServer::SMembers, 1, 1 },
	{ "smove", &ArdbServer::SMove, 3, 3 },
	{ "spop", &ArdbServer::SPop, 1, 1 },
	{ "sranmember", &ArdbServer::SRandMember, 1, 2 },
	{ "srem", &ArdbServer::SRem, 2, -1 },
	{ "sunion", &ArdbServer::SUnion, 2, -1 },
	{ "sunionstore", &ArdbServer::SUnionStore, 3, -1 },
	{ "zadd", &ArdbServer::ZAdd, 3, -1 },
	{ "zcard", &ArdbServer::ZCard, 1, 1 },
	{ "zcount", &ArdbServer::ZCount, 3, 3 },
	{ "zincrby", &ArdbServer::ZIncrby, 3, 3 },
	{ "zrange", &ArdbServer::ZRange, 3, 4 },
	{ "zrangebyscore", &ArdbServer::ZRangeByScore, 3, 7 },
	{ "zrank", &ArdbServer::ZRank, 2, 2 },
	{ "zrem", &ArdbServer::ZRem, 2, -1 },
	{ "zremrangebyrank", &ArdbServer::ZRemRangeByRank, 3, 3 },
	{ "zremrangebyscore", &ArdbServer::ZRemRangeByScore, 3, 3 },
	{ "zrevrange", &ArdbServer::ZRevRange, 3, 4 },
	{ "zrevrangebyscore", &ArdbServer::ZRevRangeByScore, 3, 7 },
	{ "zinterstore", &ArdbServer::ZInterStore, 3, -1 },
	{ "zunionstore", &ArdbServer::ZUnionStore, 3, -1 },
	{ "zrevrank", &ArdbServer::ZRevRank, 2, 2 },
	{ "zscore", &ArdbServer::ZScore, 2, 2 },
	{ "lindex", &ArdbServer::LIndex, 2, 2 },
	{ "linsert", &ArdbServer::LInsert, 4, 4 },
	{ "llen", &ArdbServer::LLen, 1, 1 },
	{ "lpop", &ArdbServer::LPop, 1, 1 },
	{ "lpush", &ArdbServer::LPush, 2, -1 },
	{ "lpushx", &ArdbServer::LPushx, 2, 2 },
	{ "lrange", &ArdbServer::LRange, 3, 3 },
	{ "lrem", &ArdbServer::LRem, 3, 3 },
	{ "lset", &ArdbServer::LSet, 3, 3 },
	{ "ltrim", &ArdbServer::LTrim, 3, 3 },
	{ "rpop", &ArdbServer::RPop, 1, 1 },
	{ "rpush", &ArdbServer::RPush, 2, -1 },
	{ "rpushx", &ArdbServer::RPushx, 2, 2 },
	{ "rpoplpush", &ArdbServer::RPopLPush, 2, 2 },

	{ "hclear", &ArdbServer::HClear, 1, 1 },
	{ "zclear", &ArdbServer::ZClear, 1, 1 },
	{ "sclear", &ArdbServer::SClear, 1, 1 },
	{ "lclear", &ArdbServer::LClear, 1, 1 },
	{ "tclear", &ArdbServer::TClear, 1, 1 },
	{ "move", &ArdbServer::Move, 2, 2 },
	{ "rename", &ArdbServer::Rename, 2, 2 },
	{ "renamenx", &ArdbServer::RenameNX, 2, 2 },
	{ "sort", &ArdbServer::Sort, 1, -1 },
	};

	uint32 arraylen = arraysize(settingTable);
	for (uint32 i = 0; i < arraylen; i++)
	{
		{
			m_handler_table[settingTable[i].name] = settingTable[i];
		}
	}
}
ArdbServer::~ArdbServer()
{

}

int ArdbServer::HClear(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	m_db->HClear(ctx.currentDB, cmd[0]);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}
int ArdbServer::SClear(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->SClear(ctx.currentDB, cmd[0]);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}
int ArdbServer::ZClear(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->ZClear(ctx.currentDB, cmd[0]);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}
int ArdbServer::LClear(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->LClear(ctx.currentDB, cmd[0]);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}
int ArdbServer::TClear(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->TClear(ctx.currentDB, cmd[0]);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}

int ArdbServer::Info(ArdbConnContext& ctx, ArgumentArray& cmd){
	std::string info;
	info.append("# Server\r\n");
	info.append("ardb_version:").append(ARDB_VERSION).append("\r\n");
	info.append("# Disk\r\n");
	int64 filesize = file_size(m_cfg.data_base_path);
	char tmp[256];
	sprintf(tmp, "%lld",filesize);
	info.append("db_used_space:").append(tmp).append("\r\n");
	fill_str_reply(ctx.reply, info);
	return 0;
}

int ArdbServer::DBSize(ArdbConnContext& ctx, ArgumentArray& cmd){
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
				        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
				return 0;
			}
			Channel* conn = m_clients_holder.GetConn(cmd[1]);
			if(NULL == conn)
			{
				fill_error_reply(ctx.reply,"ERR No such client");
				return 0;
			}
			fill_status_reply(ctx.reply, "OK");
			if(conn == ctx.conn)
			{
				return -1;
			}else{
				conn->Close();
				return 0;
			}
		}
		else if (subcmd == "setname")
		{
			if(cmd.size() != 2)
			{
				fill_error_reply(ctx.reply, "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
				return 0;
			}
			m_clients_holder.SetName(ctx.conn, cmd[1]);
			fill_status_reply(ctx.reply, "OK");
			return 0;
		}
		else if (subcmd == "getname")
		{
			if (cmd.size() != 1)
			{
				fill_error_reply(ctx.reply, "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
				return 0;
			}
			fill_str_reply(ctx.reply, m_clients_holder.GetName(ctx.conn));
			return 0;
		}
		else if (subcmd == "list")
		{
			if (cmd.size() != 1)
			{
				fill_error_reply(ctx.reply,
				        "ERR Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
				return 0;
			}
			m_clients_holder.List(ctx.reply);
			return 0;
		}
		else
		{
			fill_error_reply(ctx.reply,
			        "ERR CLIENT subcommand must be one of LIST, GETNAME, SETNAME, KILL");
		}
	return 0;
}

int ArdbServer::SlowLog(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	std::string subcmd = string_tolower(cmd[0]);
    if(subcmd == "len")
    {
    	fill_int_reply(ctx.reply, m_slowlog_handler.Size());
    }else if(subcmd == "reset"){
    	fill_status_reply(ctx.reply, "OK");
    }else if(subcmd == "get")
    {
        if(cmd.size() != 2){
        	fill_error_reply(ctx.reply, "ERR Wrong number of arguments for SLOWLOG GET");
        }
        uint32 len = 0;
        if(!string_touint32(cmd[1], len)){
        	fill_error_reply(ctx.reply, "ERR value is not an integer or out of range.");
        	return 0;
        }
        m_slowlog_handler.GetSlowlog(len, ctx.reply);
    }else{
    	fill_error_reply(ctx.reply, "ERR SLOWLOG subcommand must be one of GET, LEN, RESET");
    }
	return 0;
}

int ArdbServer::Config(ArdbConnContext& ctx, ArgumentArray& cmd){
	std::string arg0 = string_tolower(cmd[0]);
	if(arg0 != "get" && arg0 != "set" && arg0 != "resetstat"){
		fill_error_reply(ctx.reply, "ERR CONFIG subcommand must be one of GET, SET, RESETSTAT");
		return 0;
	}
	if(arg0 == "resetstat")
	{
		if(cmd.size() != 1){
			fill_error_reply(ctx.reply, "ERR Wrong number of arguments for CONFIG RESETSTAT");
			return 0;
		}
	}
	else if(arg0 == "get"){
		if(cmd.size() != 2){
			fill_error_reply(ctx.reply, "ERR Wrong number of arguments for CONFIG GET");
			return 0;
		}
		ctx.reply.type = REDIS_REPLY_ARRAY;
		Properties::iterator it = m_cfg_props.begin();
		while(it != m_cfg_props.end())
		{
			if(fnmatch(cmd[1].c_str(), it->first.c_str(), 0) == 0){
				ctx.reply.elements.push_back(RedisReply(it->first));
				ctx.reply.elements.push_back(RedisReply(it->second));
			}
			it++;
		}
	}else if(arg0 == "set"){
		if(cmd.size() != 3){
			fill_error_reply(ctx.reply, "RR Wrong number of arguments for CONFIG SET");
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
	vs.push_back(ValueObject((int64)micros/1000000));
	vs.push_back(ValueObject((int64)micros%1000000));
	fill_array_reply(ctx.reply, vs);
	return 0;
}

int ArdbServer::FlushDB(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->FlushDB(ctx.currentDB);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}

int ArdbServer::FlushAll(ArdbConnContext& ctx, ArgumentArray& cmd){
	m_db->FlushAll();
    fill_status_reply(ctx.reply, "OK");
	return 0;
}

int ArdbServer::Rename(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int ret = m_db->Rename(ctx.currentDB, cmd[0], cmd[1]);
	if(ret < 0){
		fill_error_reply(ctx.reply, "ERR no such key");
	}else{
		fill_status_reply(ctx.reply, "OK");
	}
	return 0;
}

int ArdbServer::Sort(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	ValueArray vs;
	std::string key = cmd[0];
	int ret = m_db->Sort(ctx.currentDB, key, cmd, vs);
	if(ret < 0){
		fill_error_reply(ctx.reply,"Invalid SORT command or invalid state for SORT.");
	}else{
		fill_array_reply(ctx.reply, vs);
	}
	return 0;
}

int ArdbServer::RenameNX(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int ret = m_db->RenameNX(ctx.currentDB, cmd[0], cmd[1]);
	fill_int_reply(ctx.reply, ret < 0 ? 0:1);
	return 0;
}

int ArdbServer::Move(ArdbConnContext& ctx, ArgumentArray& cmd){
	int ret = m_db->Move(ctx.currentDB, cmd[0], cmd[1]);
	fill_int_reply(ctx.reply, ret < 0 ? 0:1);
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
	}
	else
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
				}
				else
				{
					ex = iv;
				}
				i++;
			}
			else
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
			}
			else
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
	}
	else
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
	}
	else
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
		fill_error_reply(ctx.reply, "ERR wrong number of arguments for MSETNX");
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
		fill_error_reply(ctx.reply, "ERR wrong number of arguments for MSET");
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
		fill_error_reply(ctx.reply, "ERR value is not a float or out of range");
		return 0;
	}
	int ret = m_db->IncrbyFloat(ctx.currentDB, cmd[0], increment, val);
	if (ret == 0)
	{
		fill_double_reply(ctx.reply, val);
	}
	else
	{
		fill_error_reply(ctx.reply, "ERR value is not a float or out of range");
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
	}
	else
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
	}
	else
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
	}
	else
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
	}
	else
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
	}
	else
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
	}
	else
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
	}
	else
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
	}
	else
	{
		fill_error_reply(ctx.reply, "ERR failed to append key:%s", key.c_str());
	}
	return 0;
}

int ArdbServer::Ping(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	//fill_status_reply(ctx.reply, "PONG");
	ctx.reply.str = "PONG";
	ctx.reply.type = REDIS_REPLY_STATUS;
	return 0;
}
int ArdbServer::Echo(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	ctx.reply.str = cmd[0];
	ctx.reply.type = REDIS_REPLY_STRING;
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
	return 0;
}

int ArdbServer::HMSet(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	if ((cmd.size() - 1) % 2 != 0)
	{
		fill_error_reply(ctx.reply, "ERR wrong number of arguments for HMSet");
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
		fill_error_reply(ctx.reply, "ERR value is not a float or out of range");
		return 0;
	}
	m_db->HIncrbyFloat(ctx.currentDB, cmd[0], cmd[1], increment, val);
	fill_double_reply(ctx.reply, val);
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
		fill_str_reply(reply2, results[i].ToString());
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
	}
	else
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
	ValueSet vs;
	int ret = m_db->SUnionStore(ctx.currentDB, cmd[0], keys);
	fill_int_reply(ctx.reply, ret);
	return 0;
}


//===========================Sorted Sets cmds==============================
int ArdbServer::ZAdd(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	if ((cmd.size() - 1) % 2 != 0)
	{
		fill_error_reply(ctx.reply, "ERR wrong number of arguments for ZAdd");
		return 0;
	}
	m_db->Multi(ctx.currentDB);
	for(uint32 i = 1 ; i < cmd.size(); i+=2){
	    double score;
	    if (!string_todouble(cmd[i], score))
	    {
			fill_error_reply(ctx.reply,
					"ERR value is not a float or out of range");
			m_db->Discard(ctx.currentDB);
			return 0;
		}
	    m_db->ZAdd(ctx.currentDB, cmd[0], score, cmd[i+1]);
	}
	m_db->Exec(ctx.currentDB);
	return 0;
}

int ArdbServer::ZCard(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int ret = m_db->ZCard(ctx.currentDB, cmd[0]);
	fill_int_reply(ctx.reply, ret);
	return 0;
}

int ArdbServer::ZCount(ArdbConnContext& ctx, ArgumentArray& cmd){
    int ret = m_db->ZCount(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
    fill_int_reply(ctx.reply, ret);
    	return 0;
}

int ArdbServer::ZIncrby(ArdbConnContext& ctx, ArgumentArray& cmd){
	double increment, value;
	m_db->ZIncrby(ctx.currentDB, cmd[0], increment, cmd[2], value);
    return 0;
}

int ArdbServer::ZRange(ArdbConnContext& ctx, ArgumentArray& cmd){
	bool withscores = false;
	if(cmd.size() == 4){
		if(string_tolower(cmd[3]) !=  "WITHSCORES"){
			fill_error_reply(ctx.reply,
								"ERR syntax error");
			return 0;
		}
		withscores = true;
	}
	int start, stop;
	if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
	{
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
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
			if(optionstr == "withscores"){
				options.withscores = true;
				return process_query_options(cmd, idx + 1, options);
			}else{
				if(cmd.size() != idx + 3){
					return false;
				}
				if (!string_toint32(cmd[idx+1], options.limit_offset)
						|| !string_toint32(cmd[idx+2], options.limit_count))
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
			if(!ret){
				fill_error_reply(ctx.reply,
												"ERR syntax error");
				return 0;
			}
		}

		ValueArray vs;
		m_db->ZRangeByScore(ctx.currentDB, cmd[0], cmd[1], cmd[2], vs, options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::ZRank(ArdbConnContext& ctx, ArgumentArray& cmd){
		int ret = m_db->ZRank(ctx.currentDB, cmd[0], cmd[1]);
		if(ret < 0){
			ctx.reply.type = REDIS_REPLY_NIL;
		}else{
			fill_int_reply(ctx.reply, ret);
		}
		return 0;
	}

	int ArdbServer::ZRem(ArdbConnContext& ctx, ArgumentArray& cmd){
		int count = 0;
		for(uint32 i = 1; i < cmd.size(); i++){
			count += m_db->ZRem(ctx.currentDB, cmd[0], cmd[i]);
		}
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZRemRangeByRank(ArdbConnContext& ctx, ArgumentArray& cmd){
		int start, stop;
		if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
			{
				fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
				return 0;
			}
		int count = m_db->ZRemRangeByRank(ctx.currentDB, cmd[0], start, stop);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

	int ArdbServer::ZRemRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd){
		int count = m_db->ZRemRangeByScore(ctx.currentDB, cmd[0], cmd[1], cmd[2]);
				fill_int_reply(ctx.reply, count);
				return 0;
	}

	int ArdbServer::ZRevRange(ArdbConnContext& ctx, ArgumentArray& cmd){
		bool withscores = false;
			if(cmd.size() == 4){
				if(string_tolower(cmd[3]) !=  "WITHSCORES"){
					fill_error_reply(ctx.reply,
										"ERR syntax error");
					return 0;
				}
				withscores = true;
			}
			int start, stop;
			if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
			{
				fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
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
			if(!ret){
				fill_error_reply(ctx.reply,"ERR syntax error");
				return 0;
			}
		}

		ValueArray vs;
		m_db->ZRevRangeByScore(ctx.currentDB, cmd[0], cmd[1], cmd[2], vs, options);
		fill_array_reply(ctx.reply, vs);
		return 0;
	}

	int ArdbServer::ZRevRank(ArdbConnContext& ctx, ArgumentArray& cmd){
		int ret = m_db->ZRevRank(ctx.currentDB, cmd[0], cmd[1]);
		if(ret < 0){
			ctx.reply.type = REDIS_REPLY_NIL;
		}else{
			fill_int_reply(ctx.reply, ret);
		}
		return 0;
	}

	static bool process_zstore_args(ArgumentArray& cmd, SliceArray& keys, WeightArray& ws, AggregateType& type){
		int numkeys;
		if(!string_toint32(cmd[1], numkeys) || numkeys <= 0){
			return false;
		}
		if(cmd.size() < (uint32)numkeys + 2){
			return false;
		}
		for(int i = 2; i < numkeys + 2; i++){
			keys.push_back(cmd[i]);
		}
		if(cmd.size() > (uint32)numkeys + 2){
            uint32 idx = numkeys + 2;
            std::string opstr = string_tolower(cmd[numkeys + 2]);
            if(opstr == "weights"){
            	    if(cmd.size() < ((uint32)numkeys*2) + 3){
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
				idx = numkeys*2 + 3;
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
		if(!process_zstore_args(cmd, keys, ws, type))
		{
			fill_error_reply(ctx.reply,"ERR syntax error");
			return 0;
		}
		int count = m_db->ZInterStore(ctx.currentDB,cmd[0], keys, ws, type);
		fill_int_reply(ctx.reply, count);
		return 0;
	}
	int ArdbServer::ZUnionStore(ArdbConnContext& ctx, ArgumentArray& cmd)
	{
		SliceArray keys;
		WeightArray ws;
		AggregateType type = AGGREGATE_SUM;
		if(!process_zstore_args(cmd, keys, ws, type))
		{
			fill_error_reply(ctx.reply,"ERR syntax error");
			return 0;
		}
		int count = m_db->ZUnionStore(ctx.currentDB,cmd[0], keys, ws, type);
		fill_int_reply(ctx.reply, count);
		return 0;
	}

int ArdbServer::ZScore(ArdbConnContext& ctx, ArgumentArray& cmd){
	double score ;
	int ret = m_db->ZScore(ctx.currentDB, cmd[0], cmd[1], score);
	if(ret < 0){
		ctx.reply.type = REDIS_REPLY_NIL;
	}else{
		fill_double_reply(ctx.reply, score);
	}
	return 0;
}

//=========================Lists cmds================================
int ArdbServer::LIndex(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int index;
	if(!string_toint32(cmd[1], index)){
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
		return 0;
	}
	std::string v;
	int ret = m_db->LIndex(ctx.currentDB, cmd[0], index, v);
	if(ret < 0){
		ctx.reply.type = REDIS_REPLY_NIL;
	}else{
		fill_str_reply(ctx.reply, v);
	}
	return 0;
}

int ArdbServer::LInsert(ArdbConnContext& ctx, ArgumentArray& cmd){
	int ret = m_db->LInsert(ctx.currentDB, cmd[0], cmd[1], cmd[2], cmd[3]);
	fill_int_reply(ctx.reply, ret);
	return 0;
}

int ArdbServer::LLen(ArdbConnContext& ctx, ArgumentArray& cmd){
	int ret = m_db->LLen(ctx.currentDB, cmd[0]);
	fill_int_reply(ctx.reply, ret);
	return 0;
}

int ArdbServer::LPop(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	std::string v;
	int ret = m_db->LPop(ctx.currentDB, cmd[0], v);
	if(ret < 0){
		ctx.reply.type = REDIS_REPLY_NIL;
	}else{
		fill_str_reply(ctx.reply, v);
	}
	return 0;
}
int ArdbServer::LPush(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int count = 0;
	for(uint32 i = 1; i < cmd.size(); i++){
		count = m_db->LPush(ctx.currentDB, cmd[0], cmd[i]);
	}
	if(count < 0){
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
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
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
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
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
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
		return 0;
	}
	int ret = m_db->LSet(ctx.currentDB, cmd[0],index, cmd[2]);
	if(ret < 0){
		fill_error_reply(ctx.reply, "ERR index out of range");
	}else{
		fill_status_reply(ctx.reply, "OK");
	}
	return 0;
}

int ArdbServer::LTrim(ArdbConnContext& ctx, ArgumentArray& cmd){
	int start, stop;
	if (!string_toint32(cmd[1], start) || !string_toint32(cmd[2], stop))
	{
		fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
		return 0;
	}
	m_db->LTrim(ctx.currentDB, cmd[0],start, stop);
	fill_status_reply(ctx.reply, "OK");
	return 0;
}

int ArdbServer::RPop(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	std::string v;
	int ret = m_db->RPop(ctx.currentDB, cmd[0], v);
	if(ret < 0){
		ctx.reply.type = REDIS_REPLY_NIL;
	}else{
		fill_str_reply(ctx.reply, v);
	}
	return 0;
}
int ArdbServer::RPush(ArdbConnContext& ctx, ArgumentArray& cmd)
{
	int count = 0;
	for(uint32 i = 1; i < cmd.size(); i++){
		count = m_db->RPush(ctx.currentDB, cmd[0], cmd[i]);
	}
	if(count < 0){
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

int ArdbServer::RPopLPush(ArdbConnContext& ctx, ArgumentArray& cmd){
	std::string v;
	m_db->RPopLPush(ctx.currentDB, cmd[0], cmd[1], v);
	fill_str_reply(ctx.reply, v);
	return 0;
}

ArdbServer::RedisCommandHandlerSetting* ArdbServer::FindRedisCommandHandlerSetting(std::string& cmd){
	RedisCommandHandlerSettingTable::iterator found = m_handler_table.find(cmd);
	if(found != m_handler_table.end()){
		return &(found->second);
	}
	return NULL;
}

void ArdbServer::ProcessRedisCommand(ArdbConnContext& ctx,
		RedisCommandFrame& args)
	{
		ctx.reply.Clear();
		std::string& cmd = args.GetCommand();
		int ret = 0;
		lower_string(cmd);
		RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(cmd);
		if (NULL != setting)
		{
			RedisCommandHandler handler = setting->handler;
			bool valid_cmd = true;
			if (setting->min_arity > 0)
			{
				valid_cmd = args.GetArguments().size() >= (uint32)setting->min_arity;
			}
			if (setting->max_arity >= 0 && valid_cmd)
			{
				valid_cmd = args.GetArguments().size() <= (uint32)setting->max_arity;
			}

			if (!valid_cmd)
			{
				fill_error_reply(ctx.reply,
				        "ERR wrong number of arguments for '%s' command",
				        cmd.c_str());
			}
			else
			{
				m_clients_holder.TouchConn(ctx.conn, cmd);
				uint64 start_time = get_current_epoch_micros();
				ret = (this->*handler)(ctx, args.GetArguments());
				uint64 stop_time = get_current_epoch_micros();
				if ( m_cfg.slowlog_log_slower_than &&
						(stop_time - start_time) > m_cfg.slowlog_log_slower_than)
				{
					m_slowlog_handler.PushSlowCommand(args, stop_time - start_time);
					INFO_LOG(
					        "Cost %lldus to exec %s", (stop_time-start_time), cmd.c_str());
				}
			}
		}
		else
		{
			ERROR_LOG("No handler found for:%s", cmd.c_str());
			fill_error_reply(ctx.reply, "ERR unknown command '%s'",
			        cmd.c_str());
		}

		if (ctx.reply.type != 0)
		{
			ctx.conn->Write(ctx.reply);
		}
		if (ret < 0)
		{
			ctx.conn->Close();
		}
}

static void ardb_pipeline_init(ChannelPipeline* pipeline, void* data)
{
	ChannelUpstreamHandler<RedisCommandFrame>* handler =
			(ChannelUpstreamHandler<RedisCommandFrame>*) data;
	pipeline->AddLast("decoder", new RedisFrameDecoder);
	pipeline->AddLast("encoder", new RedisReplyEncoder);
	pipeline->AddLast("handler", handler);
}

static void ardb_pipeline_finallize(ChannelPipeline* pipeline, void* data)
{
	ChannelHandler* handler = pipeline->Get("decoder");
	DELETE(handler);
	handler = pipeline->Get("encoder");
	DELETE(handler);
}

static void daemonize(void) {
    int fd;

    if (fork() != 0)
    {
    	exit(0); /* parent exits */
    }
    setsid(); /* create a new session */

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO){
        	close(fd);
        }
    }
}

int ArdbServer::Start(const Properties& props)
{
	m_cfg_props = props;
	ParseConfig(props, m_cfg);
	if(m_cfg.daemonize){
		daemonize();
	}

#ifdef __USE_KYOTOCABINET__
	m_engine = new KCDBEngineFactory(props);
#else
	m_engine = new LevelDBEngineFactory(props);
#endif
	m_db = new Ardb(m_engine);
	m_service = new ChannelService(m_cfg.max_clients + 32);


	ChannelOptions ops;
	ops.tcp_nodelay = true;
	if (m_cfg.listen_host.empty() && m_cfg.listen_unix_path.empty())
	{
		m_cfg.listen_host = "0.0.0.0";
		if (m_cfg.listen_port == 0)
		{
			m_cfg.listen_port = 6379;
		}
	}
	struct RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
	{
		ArdbServer* server;
		ArdbConnContext ardbctx;
		void MessageReceived(ChannelHandlerContext& ctx,
				MessageEvent<RedisCommandFrame>& e)
		{
			ardbctx.conn = ctx.GetChannel();
			server->ProcessRedisCommand(ardbctx, *(e.GetMessage()));
		}
		void ChannelClosed(ChannelHandlerContext& ctx,
							ChannelStateEvent& e)
		{
			server->m_clients_holder.EraseConn(ctx.GetChannel());
		}
		RedisRequestHandler(ArdbServer* s) :
				server(s)
		{
		}
	}handler(this);

	if (!m_cfg.listen_host.empty())
	{
		SocketHostAddress address(m_cfg.listen_host.c_str(), m_cfg.listen_port);
		ServerSocketChannel* server = m_service->NewServerSocketChannel();
		if (!server->Bind(&address))
		{
			ERROR_LOG(
					"Failed to bind on %s:%d", m_cfg.listen_host.c_str(), m_cfg.listen_port);
			goto sexit;
		}
		server->Configure(ops);
		server->SetChannelPipelineInitializor(ardb_pipeline_init, &handler);
		server->SetChannelPipelineFinalizer(ardb_pipeline_finallize, NULL);
	}
	if (!m_cfg.listen_unix_path.empty())
	{
		SocketUnixAddress address(m_cfg.listen_unix_path);
		ServerSocketChannel* server = m_service->NewServerSocketChannel();
		if (!server->Bind(&address))
		{
			ERROR_LOG( "Failed to bind on %s", m_cfg.listen_unix_path.c_str());
			goto sexit;
		}
		server->Configure(ops);
		server->SetChannelPipelineInitializor(ardb_pipeline_init, &handler);
		server->SetChannelPipelineFinalizer(ardb_pipeline_finallize, NULL);
		chmod(m_cfg.listen_unix_path.c_str(), m_cfg.unixsocketperm);
	}
	ArdbLogger::InitDefaultLogger(m_cfg.loglevel, m_cfg.logfile);
    INFO_LOG("Server started, Ardb version %s", ARDB_VERSION);
    INFO_LOG("The server is now ready to accept connections on port %d", m_cfg.listen_port);
	m_service->Start();
sexit:
	DELETE(m_engine);
	DELETE(m_db);
	DELETE(m_service);
	ArdbLogger::DestroyDefaultLogger();
	return 0;
}
}


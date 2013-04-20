/*
 * slowlog.cpp
 *
 *  Created on: 2013-4-20
 *      Author: wqy
 */
#include "ardb_server.hpp"

namespace ardb
{
	static uint64 kSlowlogIDSeed = 0;
	void SlowLogHandler::PushSlowCommand(const RedisCommandFrame& cmd,
	        uint64 micros)
	{
		while (m_cfg.slowlog_max_len > 0
		        && m_cmds.size() >= m_cfg.slowlog_max_len)
		{
			m_cmds.pop_front();
		}
		SlowLog log;
		log.id = kSlowlogIDSeed++;
		log.costs = micros;
		log.ts = get_current_epoch_micros();
		log.cmd = cmd;
		m_cmds.push_back(log);
	}

	void SlowLogHandler::GetSlowlog(uint32 len, ArdbReply& reply)
	{
		reply.type = REDIS_REPLY_ARRAY;
		for (uint32 i = 0; i < len && i < m_cmds.size(); i++)
		{
			SlowLog& cmd = m_cmds[i];
			ArdbReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(ArdbReply(cmd.id));
			r.elements.push_back(ArdbReply(cmd.ts));
			r.elements.push_back(ArdbReply(cmd.costs));
			ArdbReply cmdreply;
			cmdreply.type = REDIS_REPLY_ARRAY;
			cmdreply.elements.push_back(ArdbReply(cmd.cmd.GetCommand()));
			for (uint32 j = 0; j < cmd.cmd.GetArguments().size(); j++)
			{
				cmdreply.elements.push_back(
				        ArdbReply(*(cmd.cmd.GetArgument(j))));
			}
			r.elements.push_back(cmdreply);
			reply.elements.push_back(r);
		}
	}
}


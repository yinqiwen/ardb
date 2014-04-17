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

namespace ardb
{
	static uint64 kSlowlogIDSeed = 0;
	void SlowLogHandler::PushSlowCommand(const RedisCommandFrame& cmd,
	        uint64 micros)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		while (m_cfg.slowlog_max_len > 0
		        && m_cmds.size() >= (uint32)m_cfg.slowlog_max_len)
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

	void SlowLogHandler::GetSlowlog(uint32 len, RedisReply& reply)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		reply.type = REDIS_REPLY_ARRAY;
		for (uint32 i = 0; i < len && i < m_cmds.size(); i++)
		{
			SlowLog& cmd = m_cmds[i];
			RedisReply r;
			r.type = REDIS_REPLY_ARRAY;
			r.elements.push_back(RedisReply(cmd.id));
			r.elements.push_back(RedisReply(cmd.ts));
			r.elements.push_back(RedisReply(cmd.costs));
			RedisReply cmdreply;
			cmdreply.type = REDIS_REPLY_ARRAY;
			cmdreply.elements.push_back(RedisReply(cmd.cmd.GetCommand()));
			for (uint32 j = 0; j < cmd.cmd.GetArguments().size(); j++)
			{
				cmdreply.elements.push_back(
				        RedisReply(*(cmd.cmd.GetArgument(j))));
			}
			r.elements.push_back(cmdreply);
			reply.elements.push_back(r);
		}
	}
}


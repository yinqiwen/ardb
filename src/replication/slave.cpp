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

/*
 * slave_client.cpp
 *
 *  Created on: 2013年8月20日
 *      Author: wqy
 */
#include "slave.hpp"
#include "ardb_server.hpp"
#include <sstream>


namespace ardb
{
	Slave::Slave(ArdbServer* serv) :
			m_serv(serv), m_client(NULL), m_rest_chunk_len(0), m_slave_state(
			SLAVE_STATE_CLOSED), m_cron_inited(false), m_ping_recved(false), m_reply_decoder(
					true), m_server_type(0), m_server_key("?"), m_sync_offset((uint64)-1), m_actx(
			NULL), m_rdb(NULL)
	{
	}

	void Slave::SwitcåhToReplyCodec()
	{
		ChannelUpstreamHandler<RedisReply>* handler = this;
		m_client->GetPipeline().Remove("handler");
		m_client->GetPipeline().Remove("decoder");
		m_client->GetPipeline().Remove("encoder");
		m_reply_decoder.Clear();
		m_client->GetPipeline().AddLast("decoder", &m_reply_decoder);
		m_client->GetPipeline().AddLast("handler", handler);
	}
	void Slave::SwitchToCommandCodec()
	{
		m_client->GetPipeline().Remove("handler");
		m_client->GetPipeline().Remove("decoder");
		m_client->GetPipeline().Remove("encoder");
		m_decoder.Clear();
		m_client->GetPipeline().AddLast("decoder", &m_decoder);
		m_client->GetPipeline().AddLast("encoder", &m_encoder);
		ChannelUpstreamHandler<RedisCommandFrame>* handler = this;
		m_client->GetPipeline().AddLast("handler", handler);
	}

	RedisDumpFile* Slave::GetNewRedisDumpFile()
	{
		DELETE(m_rdb);
		std::string dump_file_path = m_serv->m_cfg.home + "/repl/dump.rdb";
		NEW(m_rdb, RedisDumpFile(m_serv->m_db, dump_file_path));
		INFO_LOG("[REPL]Create redis dump file:%s", dump_file_path.c_str());
		return m_rdb;
	}

	void Slave::ChannelConnected(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		LoadSyncState();
		Buffer info;
		info.Printf("info Server\r\n");
		ctx.GetChannel()->Write(info);
		m_slave_state = SLAVE_STATE_WAITING_INFO_REPLY;
		m_ping_recved = true;
	}

	void Slave::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisCommandFrame>& e)
	{
		DEBUG_LOG("Recv master cmd %s", e.GetMessage()->GetCommand().c_str());
		RedisCommandFrame* cmd = e.GetMessage();
		if (!strcasecmp(cmd->GetCommand().c_str(), "ping"))
		{
			m_ping_recved = true;
			return;
		} else if (!strcasecmp(cmd->GetCommand().c_str(), "arsynced"))
		{
			m_server_key = *(cmd->GetArgument(0));
			m_slave_state = SLAVE_STATE_SYNCED;
			uint64 seq;
			string_touint64(*(cmd->GetArgument(1)), seq);
			m_sync_offset = seq;
			return;
		}
		if (NULL == m_actx)
		{
			NEW(m_actx, ArdbConnContext);
		}
		m_actx->is_slave_conn = true;
		m_actx->conn = ctx.GetChannel();
		m_serv->ProcessRedisCommand(*m_actx, *cmd);
	}
	static void LoadRDBRoutine(void* cb)
	{
		Channel* client = (Channel*) cb;
		client->GetService().Continue();
	}
	void Slave::MessageReceived(ChannelHandlerContext& ctx,
			MessageEvent<RedisReply>& e)
	{
		RedisReply* reply = e.GetMessage();
		switch (m_slave_state)
		{
			case SLAVE_STATE_WAITING_INFO_REPLY:
			{
				if (reply->type == REDIS_REPLY_ERROR)
				{
					ERROR_LOG("Recv info reply error:%s", reply->str.c_str());
					Close();
				}
				if (reply->str.find("redis_version") != std::string::npos)
				{
					INFO_LOG("[REPL]Remote master is a Redis instance.");
					m_server_type = REDIS_DB_SERVER_TYPE;
				} else if (reply->str.find("ardb_version") != std::string::npos)
				{
					INFO_LOG("[REPL]Remote master is an Ardb instance.");
					m_server_type = ARDB_DB_SERVER_TYPE;
				}
				Buffer replconf;
				replconf.Printf("replconf listening-port %u\r\n",
						m_serv->GetServerConfig().listen_port);
				ctx.GetChannel()->Write(replconf);
				m_slave_state = SLAVE_STATE_WAITING_REPLCONF_REPLY;
				break;
			}
			case SLAVE_STATE_WAITING_REPLCONF_REPLY:
			{
				if (reply->type == REDIS_REPLY_ERROR)
				{
					ERROR_LOG("Recv replconf reply error:%s",
							reply->str.c_str());
				}
				if (m_server_type == ARDB_DB_SERVER_TYPE)
				{
					Buffer sync;
					if (m_sync_dbs.empty())
					{
						sync.Printf("psync %s %lld\r\n", m_server_key.c_str(),
								m_sync_offset);
					} else
					{
						std::stringstream stream;
						DBIDSet::iterator it = m_sync_dbs.begin();
						while (it != m_sync_dbs.end())
						{
							stream << *it << " ";
							it++;
						}
						sync.Printf("arsync %s %lld %s\r\n",
								m_server_key.c_str(), m_sync_offset,
								stream.str().c_str());
					}
					ctx.GetChannel()->Write(sync);
					m_slave_state = SLAVE_STATE_WAITING_ARSYNC_REPLY;
				} else
				{
					Buffer sync;
					sync.Printf("sync\r\n");
					ctx.GetChannel()->Write(sync);
					m_slave_state = SLAVE_STATE_WAITING_PSYNC_REPLY;
				}
				break;
			}
			case SLAVE_STATE_WAITING_PSYNC_REPLY:
			case SLAVE_STATE_SYNING_DATA:
			{
				if (reply->type != REDIS_REPLY_STRING)
				{
					ERROR_LOG("Recv error type for sync:%d", reply->type);
					Close();
					return;
				}
				ASSERT(m_server_type == REDIS_DB_SERVER_TYPE);
				if (m_slave_state == SLAVE_STATE_WAITING_PSYNC_REPLY)
				{
					GetNewRedisDumpFile();
				}
				if (!reply->str.empty())
				{
					m_rdb->Write(reply->str.c_str(), reply->str.size());
				}
				if (reply->IsLastChunk()
						|| reply->integer == (int64) reply->str.size())
				{
					m_rdb->Flush();
					SwitchToCommandCodec();
					m_slave_state = SLAVE_STATE_SYNCED;
					m_rdb->Load(LoadRDBRoutine, m_client);
					DELETE(m_rdb);
				}
				m_slave_state = SLAVE_STATE_SYNING_DATA;
				break;
			}
			default:
			{
				ERROR_LOG("Slave client is in invalid state:%d", m_slave_state);
				break;
			}
		}
	}

	void Slave::ChannelClosed(ChannelHandlerContext& ctx,
			ChannelStateEvent& e)
	{
		m_client = NULL;
		m_slave_state = 0;
		DELETE(m_actx);
		//reconnect master after 1000ms
		struct ReconnectTask: public Runnable
		{
				Slave& sc;
				ReconnectTask(Slave& ssc) :
						sc(ssc)
				{
				}
				void Run()
				{
					if (NULL == sc.m_client
							&& !sc.m_master_addr.GetHost().empty())
					{
						sc.ConnectMaster(sc.m_master_addr.GetHost(),
								sc.m_master_addr.GetPort());
					}
				}
		};
		m_serv->GetTimer().ScheduleHeapTask(new ReconnectTask(*this), 1000, -1,
				MILLIS);
	}

	void Slave::Timeout()
	{
		WARN_LOG("Master connection timeout.");
		Close();
	}

	void Slave::LoadSyncState()
	{
		std::string path = m_serv->m_cfg.repl_data_dir + "/repl.sync.state";
		Buffer content;
		if (file_read_full(path, content) == 0)
		{
			std::string str = content.AsString();
			std::vector<std::string> ss = split_string(str, " ");
			if (ss.size() >= 2)
			{
				m_server_key = ss[0];
				string_touint64(ss[1], m_sync_offset);
			}
			DEBUG_LOG("Load repl state %s:%u", m_server_key.c_str(),
					m_sync_offset);
		}
	}

	void Slave::PersistSyncState()
	{
		if (m_server_key == "-")
		{
			return;
		}
		char syncState[1024];
		sprintf(syncState, "%s %"PRIu64, m_server_key.c_str(), m_sync_offset);
		std::string path = m_serv->m_cfg.repl_data_dir + "/repl.sync.state";
		std::string content = syncState;
		file_write_content(path, content);
	}

	void Slave::Run()
	{
		if (m_slave_state == SLAVE_STATE_SYNCED)
		{
			//do nothing
			if (!m_ping_recved)
			{
				Timeout();
			}
			m_ping_recved = false;
		}
	}

	int Slave::ConnectMaster(const std::string& host, uint32 port)
	{
		if (!m_cron_inited)
		{
			m_cron_inited = true;
			m_serv->GetTimer().Schedule(this, m_serv->m_cfg.repl_timeout,
					m_serv->m_cfg.repl_timeout, SECONDS);

			struct PersistTask: public Runnable
			{
					Slave* c;
					PersistTask(Slave* cc) :
							c(cc)
					{
					}
					void Run()
					{
						c->PersistSyncState();
					}
			};
			m_serv->GetTimer().ScheduleHeapTask(new PersistTask(this),
					m_serv->m_cfg.repl_syncstate_persist_period,
					m_serv->m_cfg.repl_syncstate_persist_period, SECONDS);
		}
		SocketHostAddress addr(host, port);
		if (m_master_addr == addr && NULL != m_client)
		{
			return 0;
		}
		m_master_addr = addr;
		Close();
		m_client = m_serv->m_service->NewClientSocketChannel();
		SwitchToReplyCodec();
		m_slave_state = SLAVE_STATE_CONNECTING;
		m_client->Connect(&m_master_addr);
		return 0;
	}

	void Slave::Stop()
	{
		SocketHostAddress empty;
		m_master_addr = empty;
		m_slave_state = 0;
		m_sync_offset = 0;
		Close();
	}
}


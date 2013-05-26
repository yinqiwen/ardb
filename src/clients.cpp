/*
 * clients.cpp
 *
 *  Created on: 2013-4-20
 *      Author: wqy
 */
#include "ardb_server.hpp"
#include "util/socket_address.hpp"
#include "util/lru.hpp"
#include <sstream>

namespace ardb
{
	struct IdleConn
	{
			uint32 conn_id;
			uint64 ts;

	};

	struct IdleConnManager: public Runnable
	{
			int32 timer_id;
			LRUCache<uint32, IdleConn> lru;
			ChannelService* serv;
			uint32 maxIdleTime;
			IdleConnManager() :
					timer_id(-1), serv(NULL), maxIdleTime(10000000)
			{
			}
			void Run()
			{
				IdleConn conn;
				uint64 now = get_current_epoch_millis();
				while (lru.PeekFront(conn) && (now - conn.ts >= maxIdleTime))
				{

					Channel* ch = serv->GetChannel(conn.conn_id);
					if (NULL != ch)
					{
						DEBUG_LOG("Closed timeout connection:%d.", ch->GetID());
						ch->Close();
					}
					lru.PopFront();
				}
			}
	};

	void ArdbServer::TouchIdleConn(Channel* ch)
	{
		static ThreadLocal<IdleConnManager> kLocalIdleConns;
		uint64 now = get_current_epoch_millis();
		IdleConn conn;
		conn.conn_id = ch->GetID();
		conn.ts = now;

		IdleConnManager& m = kLocalIdleConns.GetValue();
		if (m.timer_id == -1)
		{
			m.serv = &(ch->GetService());
			m.maxIdleTime = m_cfg.timeout * 1000;
			m.lru.SetMaxCacheSize(m_cfg.max_clients);
			m.timer_id = ch->GetService().GetTimer().Schedule(&m, 1, 5,
			        SECONDS);
		}
		IdleConn tmp;
		m.lru.Insert(conn.conn_id, conn, tmp);
	}

	void ClientConnHolder::ChangeCurrentDB(Channel* conn,
	        const DBID& dbid)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		m_conn_table[conn->GetID()].currentDB = dbid;
	}

	void ClientConnHolder::TouchConn(Channel* conn,
	        const std::string& currentCmd)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		ArdbConncetionTable::iterator found = m_conn_table.find(conn->GetID());
		if (found != m_conn_table.end())
		{
			found->second.lastCmd = currentCmd;
			found->second.lastTs = time(NULL);
		}
		else
		{
			ArdbConncetion c;
			c.conn = conn;
			c.birth = time(NULL);
			c.lastTs = time(NULL);
			c.lastCmd = currentCmd;
			const Address* remote = conn->GetRemoteAddress();
			if (InstanceOf<SocketUnixAddress>(remote).OK)
			{
				const SocketUnixAddress* addr =
				        (const SocketUnixAddress*) remote;
				c.addr = addr->GetPath();
			}
			else if (InstanceOf<SocketHostAddress>(remote).OK)
			{
				const SocketHostAddress* addr =
				        (const SocketHostAddress*) remote;
				char tmp[256];
				sprintf(tmp, "%s:%u", addr->GetHost().c_str(), addr->GetPort());
				c.addr = tmp;
			}
			m_conn_table.insert(
			        ArdbConncetionTable::value_type(conn->GetID(), c));
		}
	}

	Channel* ClientConnHolder::GetConn(const std::string& addr)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		ArdbConncetionTable::iterator it = m_conn_table.begin();
		while (it != m_conn_table.end())
		{
			if (it->second.addr == addr)
			{
				return it->second.conn;
			}
			it++;
		}
		return NULL;
	}
	void ClientConnHolder::SetName(Channel* conn, const std::string& name)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		m_conn_table[conn->GetID()].name = name;
	}
	const std::string& ClientConnHolder::GetName(Channel* conn)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		return m_conn_table[conn->GetID()].name;
	}
	void ClientConnHolder::List(RedisReply& reply)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		reply.type = REDIS_REPLY_STRING;
		std::stringstream ss(std::stringstream::in | std::stringstream::out);
		ArdbConncetionTable::iterator it = m_conn_table.begin();
		time_t now = time(NULL);
		while (it != m_conn_table.end())
		{
			ArdbConncetion& c = it->second;
			ss << "addr=" << c.addr << " fd=" << c.conn->GetReadFD();
			ss << " name=" << c.name << " age=" << now - c.birth;
			ss << " idle=" << now - c.lastTs << " db=" << c.currentDB;
			ss << " cmd=" << c.lastCmd << "\r\n";
			it++;
		}
		reply.str = ss.str();
	}
}


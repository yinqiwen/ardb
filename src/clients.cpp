/*
 * clients.cpp
 *
 *  Created on: 2013-4-20
 *      Author: wqy
 */
#include "ardb_server.hpp"
#include "util/socket_address.hpp"
#include <sstream>

namespace ardb
{
	void ClientConnHolder::ChangeCurrentDB(Channel* conn,
	        const std::string& dbid)
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


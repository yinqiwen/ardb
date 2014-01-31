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
                LRUCache<uint32, IdleConn>::CacheEntry conn;
                uint64 now = get_current_epoch_millis();
                while (lru.PeekFront(conn) && (now - conn.second.ts >= maxIdleTime))
                {
                    Channel* ch = serv->GetChannel(conn.second.conn_id);
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
            m.timer_id = ch->GetService().GetTimer().Schedule(&m, 1, 5, SECONDS);
        }
        LRUCache<uint32, IdleConn>::CacheEntry tmp;
        m.lru.Insert(conn.conn_id, conn, tmp);
    }

    void ClientConnHolder::ChangeCurrentDB(Channel* conn, const DBID& dbid)
    {
        LockGuard<ThreadMutex> guard(m_mutex);
        m_conn_table[conn->GetID()].currentDB = dbid;
    }

    void ClientConnHolder::TouchConn(Channel* conn, const std::string& currentCmd)
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
                const SocketUnixAddress* addr = (const SocketUnixAddress*) remote;
                c.addr = addr->GetPath();
            }
            else if (InstanceOf<SocketHostAddress>(remote).OK)
            {
                const SocketHostAddress* addr = (const SocketHostAddress*) remote;
                char tmp[256];
                sprintf(tmp, "%s:%u", addr->GetHost().c_str(), addr->GetPort());
                c.addr = tmp;
            }
            m_conn_table.insert(ArdbConncetionTable::value_type(conn->GetID(), c));
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


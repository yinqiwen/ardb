/*
 * zookeeper_client.cpp
 *
 *  Created on: 2013年9月22日
 *      Author: wqy
 */
#include "zookeeper_client.hpp"

namespace ardb
{
	void ZookeeperClient::WatchCallback(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
	{
		DEBUG_LOG("Recv watch callback with type:%d state:%d, path:%s.", type, state, path);
		ZookeeperClient* zk = (ZookeeperClient*) watcherCtx;
		if (NULL != zk->m_callback)
		{
			if(state == ZOO_EXPIRED_SESSION_STATE)
			{
				//zk->
			}
			zk->m_callback->OnWatch(type, state, path);
		}
	}

	void ZookeeperClient::ZKIOCallback(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask)
	{
		int events = 0;
		if (mask & AE_READABLE)
		{
			events |= ZOOKEEPER_READ;
		}
		if (mask & AE_WRITABLE)
		{
			events |= ZOOKEEPER_WRITE;
		}
		ZookeeperClient* zk = (ZookeeperClient*) clientData;
		if (0 != events && -1 != fd)
		{
			int rc = zookeeper_process(zk->m_zk, events);
			if (rc != ZOK && !is_valid_fd(fd))
			{
				aeDeleteFileEvent(eventLoop, fd, AE_READABLE | AE_WRITABLE);
				if(zk->m_zk_fd == fd)
				{
					zk->m_zk_fd = -1;
				}
			}
		}
		int interest;
		struct timeval tv;
		int rc = zookeeper_interest(zk->m_zk, &fd, &interest, &tv);
		if (rc == ZOK && fd != -1)
		{
			int events = 0;
			if (interest & ZOOKEEPER_READ)
			{
				events |= AE_READABLE;
			}
			if (interest & ZOOKEEPER_WRITE)
			{
				events |= AE_WRITABLE;
			}
			aeCreateFileEvent(eventLoop, fd, events, ZookeeperClient::ZKIOCallback, clientData);
			zk->m_zk_fd = fd;
		}
		if (rc != ZOK)
		{
			ERROR_LOG("Failed to get interst for reason：%s at state:%d", zerror(rc), zoo_state(zk->m_zk));
		}
	}

	ZookeeperClient::ZookeeperClient(ChannelService& serv) :
			m_serv(serv), m_zk(NULL), m_callback(NULL),m_zk_fd(-1),m_timer_id(-1)
	{

	}

	void ZookeeperClient::Run()
	{
		if(-1 == m_zk_fd)
		{
			ZKIOCallback(m_serv.GetRawEventLoop(), -1, this, 0);
		}
	}

	int ZookeeperClient::Connect(const std::string& servers, const ZKOptions& options)
	{
		if(-1 == m_timer_id)
		{
			m_timer_id = m_serv.GetTimer().Schedule(this, 1, 1, SECONDS);
		}
		if (NULL != m_zk)
		{
			zookeeper_close(m_zk);
		}
		m_zk = zookeeper_init(servers.c_str(), ZookeeperClient::WatchCallback, options.recv_timeout, &options.clientid, this, 0);
		if (NULL == m_zk)
		{
			ERROR_LOG("Failed to init zokkeeper.");
			return -1;
		}
		m_callback = options.cb;
		ZKIOCallback(m_serv.GetRawEventLoop(), -1, this, 0);
		return 0;
	}

	void ZookeeperClient::ExistsCompletionCB(int rc, const struct Stat *stat, const void *data)
	{
		std::pair<std::string, ZookeeperClient*>* p = (std::pair<std::string, ZookeeperClient*>*) data;
		ZookeeperClient* zk = p->second;
		if (NULL != zk->m_callback)
		{
			zk->m_callback->OnStat(p->first, stat, rc);
		}
		DELETE(p);
	}
	int ZookeeperClient::Exists(const std::string& path, bool watch)
	{
		if (NULL == m_zk)
		{
			return -1;
		}
		std::pair<std::string, ZookeeperClient*>* cb = new std::pair<std::string, ZookeeperClient*>;
		cb->first = path;
		cb->second = this;
		return zoo_aexists(m_zk, path.c_str(), watch ? 1 : 0, ZookeeperClient::ExistsCompletionCB, cb);
	}

	void ZookeeperClient::CreateCB(int rc, const char *value, const void *data)
	{
		std::pair<std::string, ZookeeperClient*>* p = (std::pair<std::string, ZookeeperClient*>*) data;
		ZookeeperClient* zk = p->second;
		if (NULL != zk->m_callback)
		{
			zk->m_callback->OnCreated(NULL == value ? p->first : value, rc);
		}
	}

	int ZookeeperClient::Create(const std::string& path, const std::string& value, const ZKACLArray& acls, int flags)
	{
		if (NULL == m_zk)
		{
			return -1;
		}
		std::pair<std::string, ZookeeperClient*>* cb = new std::pair<std::string, ZookeeperClient*>;
		cb->first = path;
		cb->second = this;
		struct ACL_vector* acl_entries = &ZOO_OPEN_ACL_UNSAFE;
		int ret = zoo_acreate(m_zk, path.c_str(), value.data(), value.size(), acl_entries, flags, ZookeeperClient::CreateCB, cb);
		return ret;
	}

	void ZookeeperClient::AuthCB(int rc, const void *data)
	{
		std::vector<void*>* cb = (std::vector<void*>*) data;
		std::string* scheme = (std::string*) ((*cb)[0]);
		std::string* cert = (std::string*) ((*cb)[1]);
		ZookeeperClient* zk = (ZookeeperClient*) ((*cb)[2]);
		if (NULL != zk->m_callback)
		{
			zk->m_callback->OnAuth(*scheme, *cert, rc);
		}
		DELETE(scheme);
		DELETE(cert);
		DELETE(cb);
	}

	int ZookeeperClient::Auth(const std::string& scheme, const std::string& cert)
	{
		if (NULL == m_zk)
		{
			return -1;
		}
		std::vector<void*>* cb = new std::vector<void*>;
		cb->push_back(new std::string(scheme));
		cb->push_back(new std::string(cert));
		cb->push_back(this);
		return zoo_add_auth(m_zk, scheme.c_str(), cert.data(), cert.size(), ZookeeperClient::AuthCB, cb);
	}

	int ZookeeperClient::GetClientID(clientid_t& id)
	{
		if (NULL == m_zk)
		{
			return -1;
		}
		id = *(zoo_client_id(m_zk));
		return 0;
	}

	ZookeeperClient::~ZookeeperClient()
	{
		if(-1 != m_timer_id)
		{
			m_serv.GetTimer().Cancel(m_timer_id);
		}

		if (NULL != m_zk)
		{
			zookeeper_close(m_zk);
		}
	}
}


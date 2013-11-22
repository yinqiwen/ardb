/*
 * zookeeper_client.cpp
 *
 *  Created on: 2013-9-22
 *      Author: wqy
 */
#include "zookeeper_client.hpp"
#include <libgen.h>
#include <deque>

namespace ardb
{
    void ZookeeperClient::WatchCallback(zhandle_t *zh, int type, int state,
		    const char *path, void *watcherCtx)
    {
	DEBUG_LOG("Recv watch callback with type:%d state:%d, path:%s.", type,
			state, path);
	ZookeeperClient* zk = (ZookeeperClient*) watcherCtx;
	if (NULL != zk->m_callback)
	{
	    if (state == ZOO_EXPIRED_SESSION_STATE)
	    {
		//zk->
	    }
	    zk->m_callback->OnWatch(type, state, path);
	}
    }

    void ZookeeperClient::ZKIOCallback(struct aeEventLoop *eventLoop, int fd,
		    void *clientData, int mask)
    {
	if (fd > 0 && !is_valid_fd(fd))
	{
	    aeDeleteFileEvent(eventLoop, fd, AE_READABLE | AE_WRITABLE);
	    return;
	}

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
	zhandle_t* zhandler = zk->m_zk;
	if (0 != events && -1 != fd)
	{
	    int rc = zookeeper_process(zhandler, events);
	    if (rc != ZOK && !is_valid_fd(fd))
	    {
		aeDeleteFileEvent(eventLoop, fd, AE_READABLE | AE_WRITABLE);
		if (zk->m_zk_fd == fd)
		{
		    zk->m_zk_fd = -1;
		}
	    }
	}
	if (zhandler != zk->m_zk)
	{
	    //already closed
	    return;
	}
	zk->CheckConn();
    }

    ZookeeperClient::ZookeeperClient(ChannelService& serv) :
		    m_serv(serv), m_zk(NULL), m_callback(NULL), m_zk_fd(-1), m_timer_task_id(
				    -1)
    {

    }

    void ZookeeperClient::CheckConn()
    {
	int interest;
	struct timeval tv;
	int fd;
	int rc = zookeeper_interest(m_zk, &fd, &interest, &tv);
	if (rc == ZOK)
	{
	    if (fd != -1)
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
		aeDeleteFileEvent(m_serv.GetRawEventLoop(), fd,
				AE_READABLE | AE_WRITABLE);
		aeCreateFileEvent(m_serv.GetRawEventLoop(), fd, events,
				ZookeeperClient::ZKIOCallback, this);
		m_zk_fd = fd;
	    }
	    int64 wait = tv.tv_sec * 1000 + tv.tv_usec / 1000;
	    if (wait == 0)
	    {
		wait = 1000;
	    }
	    if (m_timer_task_id == -1)
	    {
		m_timer_task_id = m_serv.GetTimer().Schedule(this, wait, -1,
				MILLIS);
	    }
	}
	else
	{
	    if (rc != ZOK)
	    {
		if (m_timer_task_id == -1)
		{
		    m_timer_task_id = m_serv.GetTimer().Schedule(this, 1000, -1,
				    MILLIS);
		}
	    }
	}

    }

    void ZookeeperClient::Run()
    {
	m_timer_task_id = -1;
	CheckConn();
    }

    int ZookeeperClient::Connect(const std::string& servers,
		    const ZKOptions& options)
    {
	if (NULL != m_zk)
	{
	    zookeeper_close(m_zk);
	    m_zk = NULL;
	}
	m_zk = zookeeper_init(servers.c_str(), ZookeeperClient::WatchCallback,
			options.recv_timeout, &options.clientid, this, 0);
	if (NULL == m_zk)
	{
	    ERROR_LOG("Failed to init zokkeeper.");
	    return -1;
	}
	m_callback = options.cb;
	CheckConn();
	return 0;
    }

    void ZookeeperClient::ExistsCompletionCB(int rc, const struct Stat *stat,
		    const void *data)
    {
	std::pair<std::string, ZookeeperClient*>* p = (std::pair<std::string,
			ZookeeperClient*>*) data;
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
	std::pair<std::string, ZookeeperClient*>* cb = new std::pair<
			std::string, ZookeeperClient*>;
	cb->first = path;
	cb->second = this;
	return zoo_aexists(m_zk, path.c_str(), watch ? 1 : 0,
			ZookeeperClient::ExistsCompletionCB, cb);
    }

    void ZookeeperClient::CreateCB(int rc, const char *value, const void *data)
    {
	std::pair<std::string, ZookeeperClient*>* p = (std::pair<std::string,
			ZookeeperClient*>*) data;
	ZookeeperClient* zk = p->second;
	if (NULL != zk->m_callback)
	{
	    zk->m_callback->OnCreated(NULL == value ? p->first : value, rc);
	}
    }

    struct MultiCreatCBData
    {
	    ZookeeperClient* zk;
	    std::string orgin_path;
	    std::string value;
	    int flags;
	    std::deque<std::string> pending_dirs;
    };
    void ZookeeperClient::RecursiveCreateCB(int rc, const char *value,
		    const void *data)
    {
	MultiCreatCBData* cb = (MultiCreatCBData*) data;
	if (rc == ZOK || rc == ZNODEEXISTS)
	{
	    std::string retpath = cb->orgin_path;
	    if (cb->pending_dirs.empty())
	    {
		if (NULL != value)
		{
		    retpath = value;
		}
		cb->zk->m_callback->OnCreated(retpath, rc);
		DELETE(cb);
	    }
	    else
	    {
		std::string& first_dir = cb->pending_dirs.back();
		cb->pending_dirs.pop_back();
		bool need_value = cb->pending_dirs.empty();
		int flags = need_value ? cb->flags : 0;
		zoo_acreate(cb->zk->m_zk, first_dir.c_str(),
				need_value ? cb->value.data() : NULL,
				need_value ? cb->value.size() : 0,
				&ZOO_OPEN_ACL_UNSAFE, flags,
				ZookeeperClient::RecursiveCreateCB, cb);
	    }
	}
	else
	{
	    cb->zk->m_callback->OnCreated(cb->orgin_path, rc);
	    DELETE(cb);
	}
    }

    int ZookeeperClient::Create(const std::string& path,
		    const std::string& value, const ZKACLArray& acls, int flags,
		    bool recursive)
    {
	if (NULL == m_zk)
	{
	    return -1;
	}
	if (recursive)
	{
	    std::vector<zoo_op_t> ops;
	    MultiCreatCBData* cbdata = new MultiCreatCBData;
	    cbdata->zk = this;
	    cbdata->flags = flags;
	    cbdata->orgin_path = path;
	    cbdata->value = value;
	    std::string base_dir = path;
	    cbdata->pending_dirs.push_back(base_dir);
	    char* dir = dirname((char*) base_dir.c_str());
	    while (NULL != dir && dir[0] != '.' && strcasecmp(dir, "/"))
	    {
		cbdata->pending_dirs.push_back(dir);
		dir = dirname(dir);
	    }
	    std::string& first_dir = cbdata->pending_dirs.back();
	    cbdata->pending_dirs.pop_back();
	    bool need_value = cbdata->pending_dirs.empty();
	    return zoo_acreate(m_zk, first_dir.c_str(),
			    need_value ? value.data() : NULL,
			    need_value ? value.size() : 0, &ZOO_OPEN_ACL_UNSAFE,
			    need_value ? flags : 0,
			    ZookeeperClient::RecursiveCreateCB, cbdata);
	}
	std::pair<std::string, ZookeeperClient*>* cb = new std::pair<
			std::string, ZookeeperClient*>;
	cb->first = path;
	cb->second = this;
	struct ACL_vector* acl_entries = &ZOO_OPEN_ACL_UNSAFE;
	int ret = zoo_acreate(m_zk, path.c_str(), value.data(), value.size(),
			acl_entries, flags, ZookeeperClient::CreateCB, cb);
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

    int ZookeeperClient::Auth(const std::string& scheme,
		    const std::string& cert)
    {
	if (NULL == m_zk)
	{
	    return -1;
	}
	std::vector<void*>* cb = new std::vector<void*>;
	cb->push_back(new std::string(scheme));
	cb->push_back(new std::string(cert));
	cb->push_back(this);
	return zoo_add_auth(m_zk, scheme.c_str(), cert.data(), cert.size(),
			ZookeeperClient::AuthCB, cb);
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

    int ZookeeperClient::Lock(const std::string& path)
    {
	//TODO
	return -1;
    }
    int ZookeeperClient::UnLock(const std::string& path)
    {
	//TODO
	return -1;
    }

    void ZookeeperClient::Close()
    {
	if (NULL != m_zk)
	{
	    if (-1 != m_zk_fd)
	    {
		aeDeleteFileEvent(m_serv.GetRawEventLoop(), m_zk_fd,
				AE_READABLE | AE_WRITABLE);
		m_zk_fd = -1;
	    }
	    zookeeper_close(m_zk);
	    m_zk = NULL;
	}
    }

    ZookeeperClient::~ZookeeperClient()
    {
	if (NULL != m_zk)
	{
	    zookeeper_close(m_zk);
	}
    }
}


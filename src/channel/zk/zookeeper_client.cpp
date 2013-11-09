/*
 * zokkeeper_client.cpp
 *
 *  Created on: 2013Äê9ÔÂ22ÈÕ
 *      Author: wqy
 */
#include "zookeeper_client.hpp"

namespace ardb
{
    static void WatchCallback(zhandle_t *zh, int type, int state,
            const char *path, void *watcherCtx)
    {

    }

    ZookeeperClient::ZookeeperClient(ChannelService& serv) :
            m_serv(serv), m_zh(NULL), m_zh_channel(NULL)
    {

    }

    int ZookeeperClient::Connect(const std::string& servers)
    {
        if(NULL != m_zh)
        {
            WARN_LOG("Destroy previous zookeeper connection.");
            zookeeper_close(m_zh);
            if(NULL != m_zh_channel)
            {
                m_zh_channel->Close();
            }
        }
        m_zh = zookeeper_init(servers.c_str(), WatchCallback, 10000,
                &m_zh_clientid, this, 0);
        if(NULL == m_zh_channel)
        {
            m_zh_channel = m_serv.NewClientSocketChannel();
        }
        return 0;
    }

}


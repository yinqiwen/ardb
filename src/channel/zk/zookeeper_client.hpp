/*
 * zookeeper_client.hpp
 *
 *  Created on: 2013Äê9ÔÂ22ÈÕ
 *      Author: wqy
 */

#ifndef ZOOKEEPER_CLIENT_HPP_
#define ZOOKEEPER_CLIENT_HPP_
#include "channel/all_includes.hpp"
#include "zookeeper.h"

namespace ardb
{
    struct ZKConnectionCallback
    {


            virtual ~ZKConnectionCallback()
            {
            }
    };

    class ZookeeperClient
    {
        private:
            ChannelService& m_serv;
            zhandle_t* m_zh;
            clientid_t m_zh_clientid;
            Channel* m_zh_channel;
        public:
            ZookeeperClient(ChannelService& serv);
            int Connect(const std::string& servers);

    };
}

#endif /* ZOOKEEPER_CLIENT_HPP_ */

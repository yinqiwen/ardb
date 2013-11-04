/*
 * zk_agent.hpp
 *
 *  Created on: 2013-9-23
 *      Author: wqy
 */

#ifndef ZK_AGENT_HPP_
#define ZK_AGENT_HPP_

#include "channel/zookeeper/zookeeper_client.hpp"

namespace ardb
{
	class ArdbServer;
	class ZKAgent:public ZKAsyncCallback
	{
		private:
			ArdbServer* m_server;
			ZookeeperClient* m_zk_client;
			clientid_t m_zk_clientid;
			std::string m_zk_node;
			int m_state;
			void OnWatch(int type, int state, const char *path);
			void OnStat(const std::string& path, const struct Stat* stat, int rc);
			void OnCreated(const std::string& path, int rc);
			void OnAuth(const std::string& scheme,const std::string& cert, int rc);
			void OnConnected();
			void OnAuthFailed();
			void OnSessionExpired();
			void Reconnect();
		public:
			ZKAgent();
			int Init(ArdbServer* serv);
			void Close();
			~ZKAgent();
	};
}

#endif /* ZK_AGENT_HPP_ */

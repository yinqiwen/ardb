/*
 * agent.cpp
 *
 *  Created on: 2013-9-23
 *      Author: wqy
 */
#include "agent.hpp"
#include "ardb_server.hpp"

#define ZK_STATE_CREATE_ROOT 1
#define ZK_STATE_CREATE_SERVER_ROOT 2
#define ZK_STATE_CREATE_NODE 3
#define ZK_STATE_CONNECTED 4

namespace ardb
{
	ZKAgent::ZKAgent() :
			m_server(NULL), m_zk_client(NULL), m_state(0)
	{
		memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
	}

	int ZKAgent::Init(ArdbServer* serv)
	{
		m_server = serv;
		ArdbServerConfig& cfg = serv->m_cfg;
		if (cfg.zookeeper_servers.empty())
		{
			WARN_LOG("No zookeeper servers specified, zookeeper agent would not start.");
			return 0;
		}
		ZooLogLevel z_log_loevel;
		if (DEBUG_ENABLED())
		{
			z_log_loevel = ZOO_LOG_LEVEL_DEBUG;
		} else if (INFO_ENABLED())
		{
			z_log_loevel = ZOO_LOG_LEVEL_INFO;
		} else if (WARN_ENABLED())
		{
			z_log_loevel = ZOO_LOG_LEVEL_WARN;
		} else if (ERROR_ENABLED())
		{
			z_log_loevel = ZOO_LOG_LEVEL_ERROR;
		} else
		{
			z_log_loevel = ZOO_LOG_LEVEL_DEBUG;
		}
		zoo_set_debug_level(z_log_loevel);
		zoo_set_log_stream(ArdbLogger::GetLogStream());
		std::string zkidfile = cfg.home + "/.zkcid";
		FILE* idfile = fopen(zkidfile.c_str(), "r");
		if (idfile != NULL)
		{
			if (fread(&m_zk_clientid, sizeof(m_zk_clientid), 1, idfile) != 1)
			{
				memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
			}
			fclose(idfile);
		}
		Buffer tmp;
		std::string zknodefile = cfg.home + "/.zknode";
		file_read_full(zknodefile, tmp);
		m_zk_node = tmp.AsString();

		DEBUG_LOG("zookeeper node:%s, clientid= %"PRId64, m_zk_node.c_str(), m_zk_clientid.client_id);
		Reconnect();
		return 0;
	}

	void ZKAgent::OnWatch(int type, int state, const char *path)
	{
		if (type == ZOO_SESSION_EVENT)
		{
			if (state == ZOO_CONNECTED_STATE)
			{
				OnConnected();
			} else if (state == ZOO_AUTH_FAILED_STATE)
			{
				OnAuthFailed();
			} else if (state == ZOO_EXPIRED_SESSION_STATE)
			{
				OnSessionExpired();
			} else if (state == ZOO_CONNECTING_STATE)
			{

			}
		}
	}

	void ZKAgent::OnAuth(const std::string& scheme, const std::string& cert, int rc)
	{

	}

	void ZKAgent::OnStat(const std::string& path, const struct Stat* stat, int rc)
	{

	}
	void ZKAgent::OnCreated(const std::string& path, int rc)
	{
		DEBUG_LOG("Create %s on zk with rc:%d", path.c_str(), rc);
		if (rc == ZOK || rc == ZNODEEXISTS)
		{
			switch (m_state)
			{
				case ZK_STATE_CREATE_ROOT:
				{
					m_state = ZK_STATE_CREATE_SERVER_ROOT;
					ZKACLArray acls;
					m_zk_client->Create("/ardb/servers", "", acls, 0);
					break;
				}
				case ZK_STATE_CREATE_SERVER_ROOT:
				{
					m_state = ZK_STATE_CREATE_NODE;
					std::string path = "/ardb/servers/" + m_server->m_repl_backlog.GetServerKey();
					int flag = ZOO_EPHEMERAL | ZOO_SEQUENCE;
					if (!m_zk_node.empty())
					{
						path = m_zk_node;
						flag = ZOO_EPHEMERAL;
					}
					ZKACLArray acls;
					std::string localip;
					get_local_host_ipv4(localip);
					if (localip.empty())
					{
						localip = "127.0.0.1";
					}
					char data[100];
					std::string role = m_server->m_cfg.master_host.empty() ? "master" : "slave";
					sprintf(data, "%s:%s:%"PRId64, role.c_str(), localip.c_str(), m_server->m_cfg.listen_port);
					m_zk_client->Create(path.c_str(), data, acls, flag);
					break;
				}
				case ZK_STATE_CREATE_NODE:
				{
					m_state = ZK_STATE_CONNECTED;
					m_zk_node = path;
					ArdbServerConfig& cfg = m_server->m_cfg;
					std::string zknodefile = cfg.home + "/.zknode";
					file_write_content(zknodefile, path);
					break;
				}
				default:
				{
					ERROR_LOG("Invalid state:%d to process oncreate event.", m_state);
					break;
				}
			}
		} else
		{
			ERROR_LOG("Failed to create path:%s at state:%d for reason:%s", path.c_str(), m_state, zerror(rc));
			Reconnect();
		}
	}

	void ZKAgent::OnConnected()
	{
		ArdbServerConfig& cfg = m_server->m_cfg;
		std::string zkidfile = cfg.home + "/.zkcid";
		FILE* idfile = fopen(zkidfile.c_str(), "w");
		m_zk_client->GetClientID(m_zk_clientid);
		if (NULL != idfile)
		{
			DEBUG_LOG("Save zookeeper clientid= %"PRId64, m_zk_clientid.client_id);
			int rc = fwrite(&m_zk_clientid, sizeof(m_zk_clientid), 1, idfile);
			if (rc != 1)
			{
				int err = errno;
				WARN_LOG("Failed to persist zookeeper clientid for reason:%s", strerror(err));
			}
			fclose(idfile);
		}
		m_state = ZK_STATE_CREATE_ROOT;
		ZKACLArray acls;
		m_zk_client->Create("/ardb", "", acls, 0);
	}

	void ZKAgent::OnAuthFailed()
	{

	}
	void ZKAgent::OnSessionExpired()
	{
		DEBUG_LOG("ZK session expired.");
		memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
		m_zk_node.clear();
		ArdbServerConfig& cfg = m_server->m_cfg;
		std::string zkidfile = cfg.home + "/.zkcid";
		std::string zknodefile = cfg.home + "/.zknode";
		unlink(zkidfile.c_str());
		unlink(zknodefile.c_str());
		Reconnect();
	}

	void ZKAgent::Reconnect()
	{
		ArdbServerConfig& cfg = m_server->m_cfg;
		ZookeeperClient* newzk = NULL;
		NEW(newzk, ZookeeperClient(*(m_server->m_service)));
		ZKOptions options;
		options.cb = this;
		options.clientid = m_zk_clientid;
		newzk->Connect(cfg.zookeeper_servers, options);
		Close();
		m_zk_client = newzk;
	}

	void ZKAgent::Close()
	{
		m_state = 0;
		DELETE(m_zk_client);
	}

	ZKAgent::~ZKAgent()
	{
		Close();
	}
}


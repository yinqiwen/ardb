/*
 * agent.cpp
 *
 *  Created on: 2013-9-23
 *      Author: wqy
 */
#include "agent.hpp"
#include "ardb_server.hpp"

#define ZK_STATE_CREATE_NODE 1

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
        }
        else if (INFO_ENABLED())
        {
            z_log_loevel = ZOO_LOG_LEVEL_INFO;
        }
        else if (WARN_ENABLED())
        {
            z_log_loevel = ZOO_LOG_LEVEL_WARN;
        }
        else if (ERROR_ENABLED())
        {
            z_log_loevel = ZOO_LOG_LEVEL_ERROR;
        }
        else
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
            }
            else if (state == ZOO_AUTH_FAILED_STATE)
            {
                OnAuthFailed();
            }
            else if (state == ZOO_EXPIRED_SESSION_STATE)
            {
                OnSessionExpired();
            }
            else if (state == ZOO_CONNECTING_STATE)
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
                case ZK_STATE_CREATE_NODE:
                {
                    m_state = ZK_STATE_CONNECTED;
                    break;
                }
                default:
                {
                    ERROR_LOG("Invalid state:%d", m_state);
                    break;
                }
            }
        }
        else
        {
            ERROR_LOG("Failed to create path:%s at state:%d for reason:%s", path.c_str(), m_state, zerror(rc));
            //OnSessionExpired();
            memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
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
            DEBUG_LOG("Save zookeeper clientid= %" PRId64, m_zk_clientid.client_id);
            int rc = fwrite(&m_zk_clientid, sizeof(m_zk_clientid), 1, idfile);
            if (rc != 1)
            {
                int err = errno;
                WARN_LOG("Failed to persist zookeeper clientid for reason:%s", strerror(err));
            }
            fclose(idfile);
        }
        m_state = ZK_STATE_CREATE_NODE;
        DEBUG_LOG("Create zknodes");
        ZKACLArray acls;
        std::string localip;
        get_local_host_ipv4(localip);
        if (localip.empty())
        {
            localip = "127.0.0.1";
        }
        std::string role = m_server->m_cfg.master_host.empty() ? "master" : "slave";
        char znode[512];
        sprintf(znode, "/ardb/servers/%s/%s:%"PRId64, m_server->m_repl_backlog.GetServerKey(), localip.c_str(), m_server->m_cfg.listen_port);
        m_zk_client->Create(znode, role, acls, ZOO_EPHEMERAL);
    }

    void ZKAgent::OnAuthFailed()
    {

    }
    void ZKAgent::OnSessionExpired()
    {
        DEBUG_LOG("ZK session expired.");
        memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
        ArdbServerConfig& cfg = m_server->m_cfg;
        std::string zkidfile = cfg.home + "/.zkcid";
        unlink(zkidfile.c_str());
        Reconnect();
    }

    void ZKAgent::Reconnect()
    {
        ArdbServerConfig& cfg = m_server->m_cfg;
        if (NULL == m_zk_client)
        {
            NEW(m_zk_client, ZookeeperClient(*(m_server->m_service)));
        }
        m_zk_client->Close();
        ZKOptions options;
        options.cb = this;
        options.clientid = m_zk_clientid;
        m_zk_client->Connect(cfg.zookeeper_servers, options);
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


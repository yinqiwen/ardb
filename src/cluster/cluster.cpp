/*
 * cluster.cpp
 *
 *  Created on: 2016Äê4ÔÂ28ÈÕ
 *      Author: wangqiying
 */
#include "cluster.hpp"
#include "cJSON.h"

OP_NAMESPACE_BEGIN
    static const char* state2String(int state)
    {
        if (state == 0)
            return "CLOSED_STATE";
        if (state == ZOO_CONNECTING_STATE)
            return "CONNECTING_STATE";
        if (state == ZOO_ASSOCIATING_STATE)
            return "ASSOCIATING_STATE";
        if (state == ZOO_CONNECTED_STATE)
            return "CONNECTED_STATE";
        if (state == ZOO_EXPIRED_SESSION_STATE)
            return "EXPIRED_SESSION_STATE";
        if (state == ZOO_AUTH_FAILED_STATE)
            return "AUTH_FAILED_STATE";

        return "INVALID_STATE";
    }

    static const char* type2String(int state)
    {
        if (state == ZOO_CREATED_EVENT)
            return "CREATED_EVENT";
        if (state == ZOO_DELETED_EVENT)
            return "DELETED_EVENT";
        if (state == ZOO_CHANGED_EVENT)
            return "CHANGED_EVENT";
        if (state == ZOO_CHILD_EVENT)
            return "CHILD_EVENT";
        if (state == ZOO_SESSION_EVENT)
            return "SESSION_EVENT";
        if (state == ZOO_NOTWATCHING_EVENT)
            return "NOTWATCHING_EVENT";

        return "UNKNOWN_EVENT_TYPE";
    }
    int Cluster::Init()
    {

        return 0;
    }
    static clientid_t myzkid;

    void Cluster::ZKInitWatcher(zhandle_t *zzh, int type, int state, const char *path, void* context)
    {
        INFO_LOG("Watcher %s state = %s for path %s", type2String(type), state2String(state), (path && strlen(path) > 0) ? path : "");
        if (type == ZOO_SESSION_EVENT)
        {
            if (state == ZOO_CONNECTED_STATE)
            {
                const clientid_t *id = zoo_client_id(zzh);
                if (myzkid.client_id == 0 || myzkid.client_id != id->client_id)
                {
                    myzkid = *id;
                    INFO_LOG("Got a new session id: 0x%llx", (long long )myzkid.client_id);
                    if (!g_db->GetConf().zk_clientid_file.empty())
                    {
                        std::string content;
                        content.assign((const char*) (&myzkid), sizeof(myzkid));
                        file_write_content(g_db->GetConf().zk_clientid_file, content);
                    }
                }
            }
            else if (state == ZOO_AUTH_FAILED_STATE)
            {
                WARN_LOG("Zookeeper server authentication failure. Shutting down...");
                zookeeper_close(zzh);
//                shutdownThisThing = 1;
//                zh = 0;
            }
            else if (state == ZOO_EXPIRED_SESSION_STATE)
            {
                WARN_LOG("Session expired. Shutting down...");
                zookeeper_close(zzh);
//                shutdownThisThing = 1;
//                zh = 0;
            }
        }
    }

    Cluster::Cluster() :
            m_zk(NULL), m_state(0)
    {
        m_zk_acl = ZOO_OPEN_ACL_UNSAFE;
    }

    void Cluster::BuildNodeTopoFromZk(const PartitionArray& partitions, const NodeArray& nodes)
    {
    }

    void Cluster::UpdateClusterTopo()
    {
        std::string partitions_path = "/" + g_db->GetConf().cluster_name + "/partitions";
        std::string nodes_path = "/" + g_db->GetConf().cluster_name + "/nodes";
    }

    int Cluster::CreateZookeeperPath()
    {
        std::string zk_path = "/" + g_db->GetConf().cluster_name + "/servers/10.10.10.10:16379";
        int err = zoo_create(m_zk, zk_path.c_str(), "", 0, &m_zk_acl, 0, "", 0);

        return -1;
    }
    void Cluster::Routine()
    {
        if (NULL == m_zk)
        {
            m_zk = zookeeper_init(g_db->GetConf().zookeeper_servers.c_str(), ZKInitWatcher, 30000, &myzkid, this, 0);
        }
    }
OP_NAMESPACE_END


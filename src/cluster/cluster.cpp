/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include "cluster.hpp"
#include "cJSON.h"
#include <unistd.h>

OP_NAMESPACE_BEGIN

    enum AgentState
    {
        ZK_STATE_DISCONNECT = 0, ZK_STATE_CONNECTING, ZK_STATE_CONNECTED, ZK_STATE_CREATE_PATH, ZK_STATE_FETCH_TOPO, ZK_STATE_READY,
    };

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
        memset(&m_zk_clientid, 0, sizeof(m_zk_clientid));
        return 0;
    }

    void Cluster::OnInitWatcher(zhandle_t *zzh, int type, int state, const char *path)
    {
        INFO_LOG("Watcher %s state = %s for path %s", type2String(type), state2String(state), (path && strlen(path) > 0) ? path : "");
        if (type == ZOO_SESSION_EVENT)
        {
            if (state == ZOO_CONNECTED_STATE)
            {
                const clientid_t *id = zoo_client_id(zzh);
                if (m_zk_clientid.client_id == 0 || m_zk_clientid.client_id != id->client_id)
                {
                    m_zk_clientid = *id;
                    INFO_LOG("Got a new session id: 0x%llx", (long long )m_zk_clientid.client_id);
                    if (!g_db->GetConf().zk_clientid_file.empty())
                    {
                        std::string content;
                        content.assign((const char*) (&m_zk_clientid), sizeof(m_zk_clientid));
                        file_write_content(g_db->GetConf().zk_clientid_file, content);
                    }
                }
                m_state = ZK_STATE_CONNECTED;
            }
            else if (state == ZOO_AUTH_FAILED_STATE)
            {
                WARN_LOG("Zookeeper server authentication failure. Shutting down...");
                zookeeper_close(zzh);
                m_zk = NULL;
                m_state = ZK_STATE_DISCONNECT;
            }
            else if (state == ZOO_EXPIRED_SESSION_STATE)
            {
                WARN_LOG("Session expired. Shutting down...");
                zookeeper_close(zzh);
                m_zk = NULL;
                m_state = ZK_STATE_DISCONNECT;
            }
        }
    }

    void Cluster::ZKInitWatcherCallback(zhandle_t *zzh, int type, int state, const char *path, void* context)
    {
        Cluster* c = (Cluster*) context;
        c->OnInitWatcher(zzh, type, state, path);
    }

    Cluster::Cluster() :
            m_zk(NULL), m_state(0)
    {
        m_zk_acl = ZOO_OPEN_ACL_UNSAFE;
    }

    void Cluster::BuildNodeTopoFromZk(const PartitionArray& partitions, const NodeArray& nodes)
    {
    }

    void Cluster::ZKGetWatchCallback(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
    {

    }

    int Cluster::FetchClusterTopo()
    {
        std::string partitions_path = "/" + g_db->GetConf().cluster_name + "/partitions";
        std::string nodes_path = "/" + g_db->GetConf().cluster_name + "/nodes";
        char bufer[2028];
        int err = zoo_wget(m_zk, nodes_path.c_str(), ZKGetWatchCallback, this, bufer, 0, NULL);
        err = zoo_get(m_zk, partitions_path.c_str(), 0, bufer, 0, NULL);
        return -1;
    }

    int Cluster::CreateZookeeperPath()
    {
        char hostname[1024]; //_SC_HOST_NAME_MAX = 255 default,
        int hostname_len = gethostname(hostname, sizeof(hostname));
        std::string host_name(hostname, hostname_len);
        cJSON* content = cJSON_CreateObject();
        cJSON_AddItemToObject(content, "uptime", cJSON_CreateNumber(time(NULL)));
        cJSON_AddItemToObject(content, "addr", cJSON_CreateString(host_name.c_str()));
        char* jsonstr = cJSON_Print(content);
        cJSON_Delete(content);
        std::string zk_path = "/" + g_db->GetConf().cluster_name + "/servers/" + host_name + ":" + stringfromll(g_db->GetConf().PrimaryPort());
        int err = zoo_create(m_zk, zk_path.c_str(), jsonstr, strlen(jsonstr), &m_zk_acl, ZOO_EPHEMERAL, "", 0);
        free(jsonstr);
        if (0 == err)
        {
            m_state = ZK_STATE_FETCH_TOPO;
        }
        else
        {
            WARN_LOG("Failed to create path or reason:%s", zerror(err));
        }
        return err;
    }
    void Cluster::Routine()
    {
        while (1)
        {
            switch (m_state)
            {
                case ZK_STATE_DISCONNECT:
                {
                    m_state = ZK_STATE_CONNECTING;
                    m_zk = zookeeper_init(g_db->GetConf().zookeeper_servers.c_str(), ZKInitWatcherCallback, 30000, &m_zk_clientid, this, 0);
                    continue;
                }
                case ZK_STATE_CONNECTING:
                {
                    return;
                }
                case ZK_STATE_CONNECTED:
                {
                    m_state = ZK_STATE_CREATE_PATH;

                    break;
                }
                case ZK_STATE_CREATE_PATH:
                {
                    if (0 != CreateZookeeperPath())
                    {
                        return;
                    }
                    else
                    {
                        continue;
                    }
                }
                case ZK_STATE_FETCH_TOPO:
                {
                    if (0 != FetchClusterTopo())
                    {
                        return;
                    }
                    else
                    {
                        m_state = ZK_STATE_READY;
                        continue;
                    }
                }
                case ZK_STATE_READY:
                {
                    return;
                }
                default:
                {
                    ERROR_LOG("Invalid zk agent state:%u", m_state);
                    return;
                }
            }
        }

    }
OP_NAMESPACE_END


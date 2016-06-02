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

#ifndef CLUSTER_HPP_
#define CLUSTER_HPP_

#include "common/common.hpp"
#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "context.hpp"
#include "zookeeper.h"
#include <map>
#include <vector>
#include "../repl/snapshot.hpp"

OP_NAMESPACE_BEGIN

    struct Node
    {
            uint32 id;
            std::string host;
            uint16 port;
            uint8 state;
            Node() :
                    id(0), port(0), state(0)
            {
            }
    };
    struct Partition
    {
            uint32 id;
            std::vector<uint32> slots;
            uint32 master;
            std::vector<uint32> slaves;
            Node* _master_node;
            std::vector<Node*> _slave_nodes;
            Partition() :
                    id(0), master(0), _master_node(NULL)
            {
            }
    };

    class Cluster
    {
        private:
            zhandle_t* m_zk;
            clientid_t m_zk_clientid;
            uint8 m_state;
            struct ACL_vector m_zk_acl;
            typedef std::vector<Partition> PartitionArray;
            typedef std::vector<Node> NodeArray;
            typedef TreeMap<uint32, Node>::Type NodeTable;
            typedef TreeMap<uint32, Partition*>::Type PartitionTable;
            NodeTable m_all_nodes;
            PartitionTable m_slot2partitions;
            void Routine();
            void BuildNodeTopoFromZk(const PartitionArray& partitions, const NodeArray& nodes);
            int FetchClusterTopo();
            int FetchClusterConfig();
            int CreateZookeeperPath();
            void OnInitWatcher(zhandle_t *zzh, int type, int state, const char *path);
            static void ZKInitWatcherCallback(zhandle_t *zzh, int type, int state, const char *path, void* context);
            static void ZKGetWatchCallback(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);
        public:
            Cluster();
            int Init();
            void GetNodeByCommand();
    };
OP_NAMESPACE_END

#endif /* SRC_CLUSTER_AGENT_CPP_ */

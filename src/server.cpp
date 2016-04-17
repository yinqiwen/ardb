/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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
#include "server.hpp"
#include "thread/thread_local.hpp"
#include "db/db.hpp"
#include <sys/types.h>
#include <sys/stat.h>

OP_NAMESPACE_BEGIN
    struct ServerHandlerData
    {
            ListenPoint listen;
            QPSTrack qps;
            ServerHandlerData()
            {
            }
    };
    typedef std::vector<ServerHandlerData> ServerHandlerDataArray;
    static ThreadLocal<RedisReplyPool> g_reply_pool;
    static QPSTrack g_total_qps;

    class ServerLifecycleHandler: public ChannelServiceLifeCycle, public Runnable
    {
            void Run()
            {
                g_db->ScanClients();
            }
            void OnStart(ChannelService* serv, uint32 idx)
            {
                serv->GetTimer().Schedule(this, 100, 100, MILLIS);
            }
            void OnStop(ChannelService* serv, uint32 idx)
            {

            }
            void OnRoutine(ChannelService* serv, uint32 idx)
            {

            }
    };

    class RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            ServerHandlerData* data;
            ClientContext m_client_ctx;
            Context m_ctx;
            bool m_delete_after_processing;
            RedisReplyPool& pool;

            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
            {
                m_client_ctx.last_interaction_ustime = get_current_epoch_micros();
                m_client_ctx.client = ctx.GetChannel();
                RedisCommandFrame* cmd = e.GetMessage();
                ChannelService& serv = m_client_ctx.client->GetService();
                uint32 channel_id = ctx.GetChannel()->GetID();
                m_client_ctx.processing = true;
                pool.Clear();
                m_ctx.SetReply(&pool.Allocate());
                RedisReply& reply = m_ctx.GetReply();
                int ret = g_db->Call(m_ctx, *cmd);
                data->qps.IncMsgCount(1);
                g_total_qps.IncMsgCount(1);
                if (m_delete_after_processing)
                {
                    delete this;
                    return;
                }
                if (ret >= 0)
                {
                    if (reply.type != 0)
                    {
                        m_client_ctx.client->Write(reply);
                    }
                }
                else
                {
                    m_client_ctx.client->Close();
                }
                m_client_ctx.processing = false;
                m_client_ctx.last_interaction_ustime = get_current_epoch_micros();
                m_ctx.ClearState();
            }
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
            {
                g_db->FreeClient(m_ctx);
            }
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
            {
                m_client_ctx.uptime = get_current_epoch_micros();
                m_client_ctx.last_interaction_ustime = get_current_epoch_micros();
                m_client_ctx.client = ctx.GetChannel();
                m_client_ctx.clientid.id = ctx.GetChannel()->GetID();
                m_client_ctx.clientid.service = &(ctx.GetChannel()->GetService());
                m_client_ctx.client->Attach(&m_ctx, NULL);
                if (!g_db->GetConf().requirepass.empty())
                {
                    m_ctx.authenticated = false;
                }

                //client ip white list
                //ReadLockGuard<SpinRWLock> guard(const_cast<SpinRWLock>(g_db->GetConf().lock));
                if (!g_db->GetConf().trusted_ip.empty())
                {
                    const Address* remote = ctx.GetChannel()->GetRemoteAddress();
                    if (InstanceOf<SocketHostAddress>(remote).OK)
                    {
                        const SocketHostAddress* addr = (const SocketHostAddress*) remote;
                        const std::string& ip = addr->GetHost();
                        if (ip != "127.0.0.1") //allways trust 127.0.0.1
                        {
                            StringTreeSet::const_iterator sit = g_db->GetConf().trusted_ip.begin();
                            while (sit != g_db->GetConf().trusted_ip.end())
                            {
                                if (stringmatchlen(sit->c_str(), sit->size(), ip.c_str(), ip.size(), 0) == 1)
                                {
                                    return;
                                }
                                sit++;
                            }
                            ctx.GetChannel()->Close();
                        }
                    }
                }
                else
                {
                    g_db->AddClient(m_ctx);
                }
            }
        public:
            RedisRequestHandler(ServerHandlerData* init_data) :
                    data(init_data), m_delete_after_processing(false), pool(g_reply_pool.GetValue())
            {
                m_ctx.client = &m_client_ctx;
                pool.SetMaxSize(g_db->GetConf().reply_pool_size);
            }
            bool IsProcessing()
            {
                return m_client_ctx.processing;
            }
            void EnableSelfDeleteAfterProcessing()
            {
                m_delete_after_processing = true;
            }
    };
    static void pipelineInit(ChannelPipeline* pipeline, void* data)
    {
        ServerHandlerData* init_data = (ServerHandlerData*) data;
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisReplyEncoder);
        pipeline->AddLast("handler", new RedisRequestHandler(init_data));
    }
    static void pipelineDestroy(ChannelPipeline* pipeline, void* data)
    {
        ChannelHandler* handler = pipeline->Get("decoder");
        DELETE(handler);
        handler = pipeline->Get("encoder");
        DELETE(handler);
        RedisRequestHandler* rhandler = (RedisRequestHandler*) pipeline->Get("handler");
        if (NULL != rhandler && rhandler->IsProcessing())
        {
            rhandler->EnableSelfDeleteAfterProcessing();
        }
        else
        {
            DELETE(rhandler);
        }
    }

    Server::Server() :
            m_service(NULL), m_uptime(0)
    {

    }
    int Server::Start()
    {
        uint32 worker_count = 0;
        for (uint32 i = 0; i < g_db->GetConf().servers.size(); i++)
        {
            worker_count += g_db->GetConf().servers[i].GetThreadPoolSize();
        }
        m_service = new ChannelService(g_db->GetConf().max_open_files);
        m_service->SetThreadPoolSize(worker_count);
        ServerLifecycleHandler lifecycle;
        m_service->RegisterLifecycleCallback(&lifecycle);

        ChannelOptions ops;
        ops.tcp_nodelay = true;
        ops.reuse_address = true;
        if (g_db->GetConf().tcp_keepalive > 0)
        {
            ops.keep_alive = g_db->GetConf().tcp_keepalive;
        }
        g_total_qps.Name = "total_msg";
        Statistics::GetSingleton().AddTrack(&g_total_qps);

        ServerHandlerDataArray handler_datas(g_db->GetConf().servers.size());
        for (uint32 i = 0; i < g_db->GetConf().servers.size(); i++)
        {
            const std::string& address = g_db->GetConf().servers[i].address;
            ServerSocketChannel* server = NULL;
            if (address.find(":") == std::string::npos)
            {
                SocketUnixAddress unix_address(address);
                server = m_service->NewServerSocketChannel();
                if (!server->Bind(&unix_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
                chmod(address.c_str(), g_db->GetConf().servers[i].unixsocketperm);
            }
            else
            {
                std::vector<std::string> ss = split_string(address, ":");
                uint32 port;
                if (ss.size() != 2 || !string_touint32(ss[1], port))
                {
                    ERROR_LOG("Invalid server socket address %s", address.c_str());
                    goto sexit;
                }
                SocketHostAddress socket_address(ss[0], port);
                server = m_service->NewServerSocketChannel();
                if (!server->Bind(&socket_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
            }
            server->Configure(ops);
            handler_datas[i].listen = g_db->GetConf().servers[i];
            handler_datas[i].qps.Name = g_db->GetConf().servers[i].address;
            Statistics::GetSingleton().AddTrack(&handler_datas[i].qps);
            server->SetChannelPipelineInitializor(pipelineInit, &handler_datas[i]);
            server->SetChannelPipelineFinalizer(pipelineDestroy, NULL);
            uint32 min = 0;
            for (uint32 j = 0; j < i; j++)
            {
                min += min + g_db->GetConf().servers[j].GetThreadPoolSize();
            }
            server->BindThreadPool(min, min + g_db->GetConf().servers[i].GetThreadPoolSize());
            INFO_LOG("Ardb will accept connections on %s", address.c_str());
        }


        StartCrons();

        INFO_LOG("Ardb started with version %s", ARDB_VERSION);
        m_service->Start();

        sexit:
        DELETE(m_service);
        return 0;
    }
OP_NAMESPACE_END


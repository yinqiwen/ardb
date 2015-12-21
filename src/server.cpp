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

OP_NAMESPACE_BEGIN

    class RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
    {
        private:
            Ardb* m_db;
            ClientContext m_client_ctx;
            Context m_ctx;
            bool m_delete_after_processing;

            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
            {
                m_client_ctx.client = ctx.GetChannel();
                RedisCommandFrame* cmd = e.GetMessage();
                ChannelService& serv = m_client_ctx.client->GetService();
                uint32 channel_id = ctx.GetChannel()->GetID();
                m_client_ctx.processing = true;
                m_ctx.reply.pool->Clear();
                //m_ctx.current_cmd = NULL;
                int ret = m_db->Call(m_ctx, *cmd);
                if (ret >= 0 && m_ctx.reply.type != 0)
                {
                    m_client_ctx.client->Write(m_ctx.reply);
                }
                if (m_delete_after_processing)
                {
                    delete this;
                    return;
                }
                if (ret < 0 && serv.GetChannel(channel_id) != NULL)
                {
                    m_client_ctx.client->Close();
                }
                else
                {
                    m_client_ctx.processing = false;
                    if (m_client_ctx.close_after_processed)
                    {
                        m_client_ctx.client->Close();
                    }
                }
            }
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
            {
                //m_db->FreeClientContext(m_ctx);
            }
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
            {
                m_client_ctx.uptime = get_current_epoch_micros();
                m_client_ctx.last_interaction_ustime = get_current_epoch_micros();
                m_ctx.client = ctx.GetChannel();
                RedisReplyPool& reply_pool = m_db->GetRedisReplyPool();
                reply_pool.SetMaxSize((uint32) (m_db->GetConfig().reply_pool_size));
                m_ctx.reply.SetPool(&reply_pool);
                if (!m_db->GetConfig().requirepass.empty())
                {
                    m_ctx.authenticated = false;
                }
                uint32 parent_id = ctx.GetChannel()->GetParentID();
                ServerSocketChannel* server_socket = (ServerSocketChannel*) m_db->GetChannelService().GetChannel(parent_id);
                m_ctx.server_address = server_socket->GetStringAddress();
                //m_db->GetStatistics().IncAcceptedClient(m_ctx.server_address, 1);
                m_db->AddClientContext(m_ctx);

                //client ip white list
                ReadLockGuard<SpinRWLock> guard(m_db->m_cfg_lock);
                if (!m_db->GetConfig().trusted_ip.empty())
                {
                    const Address* remote = ctx.GetChannel()->GetRemoteAddress();
                    if (InstanceOf<SocketHostAddress>(remote).OK)
                    {
                        const SocketHostAddress* addr = (const SocketHostAddress*) remote;
                        const std::string& ip = addr->GetHost();
                        if (ip != "127.0.0.1") //allways trust 127.0.0.1
                        {
                            StringSet::iterator sit = m_db->GetConfig().trusted_ip.begin();
                            while (sit != m_db->GetConfig().trusted_ip.end())
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
            }
        public:
            RedisRequestHandler(Ardb* s) :
                    m_db(s), m_delete_after_processing(false)
            {
                m_ctx.client = &m_client_ctx;
            }
    };
    static void pipelineInit(ChannelPipeline* pipeline, void* data)
    {
        Ardb* db = (Ardb*) data;
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisReplyEncoder);
        pipeline->AddLast("handler", new RedisRequestHandler(db));
    }
    static void pipelineDestroy(ChannelPipeline* pipeline, void* data)
    {
        ChannelHandler* handler = pipeline->Get("decoder");
        DELETE(handler);
        handler = pipeline->Get("encoder");
        DELETE(handler);
        RedisRequestHandler* rhandler = (RedisRequestHandler*) pipeline->Get("handler");
        if (NULL != rhandler && rhandler->m_client_ctx.processing)
        {
            rhandler->m_delete_after_processing = true;
        }
        else
        {
            DELETE(rhandler);
        }
    }

    Server::Server(ArdbConfig& conf, Ardb* db) :
            m_service(NULL), m_config(conf), m_db(db), m_uptime(0)
    {

    }
    int Server::Start()
    {
        uint32 worker_count = 0;
        for (uint32 i = 0; i < m_config.thread_pool_sizes.size(); i++)
        {
            if (m_config.thread_pool_sizes[i] == 0)
            {
                m_config.thread_pool_sizes[i] = 1;
            }
            else if (m_config.thread_pool_sizes[i] < 0)
            {
                m_config.thread_pool_sizes[i] = available_processors();
            }
            worker_count += m_config.thread_pool_sizes[i];
        }
        m_service = new ChannelService(m_config.max_clients + 32 + GetKeyValueEngine().MaxOpenFiles());
        m_service->SetThreadPoolSize(worker_count);

        ChannelOptions ops;
        ops.tcp_nodelay = true;
        ops.reuse_address = true;
        if (m_config.tcp_keepalive > 0)
        {
            ops.keep_alive = m_config.tcp_keepalive;
        }
        for (uint32 i = 0; i < m_config.listen_addresses.size(); i++)
        {
            const std::string& address = m_config.listen_addresses[i];
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
                chmod(address.c_str(), m_config.unixsocketperm);
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
                if (m_cfg.primary_port == 0)
                {
                    m_cfg.primary_port = port;
                }
            }
            server->Configure(ops);
            server->SetChannelPipelineInitializor(pipelineInit, m_db);
            server->SetChannelPipelineFinalizer(pipelineDestroy, NULL);
            uint32 min = 0;
            for (uint32 j = 0; j < i; j++)
            {
                min += min + m_cfg.thread_pool_sizes[j];
            }
            server->BindThreadPool(min, min + m_cfg.thread_pool_sizes[i]);
        }

        INFO_LOG("Server started, Ardb version %s", ARDB_VERSION);
        INFO_LOG("The server is now ready to accept connections on  %s", string_join_container(m_cfg.listen_addresses, ",").c_str());

        m_service->Start();

        sexit:
        DELETE(m_service);
    }
OP_NAMESPACE_END


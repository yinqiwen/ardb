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
#include "network.hpp"
#include "ardb.hpp"

OP_NAMESPACE_BEGIN
    void RedisRequestHandler::PipelineInit(ChannelPipeline* pipeline, void* data)
    {
        Ardb* serv = (Ardb*) data;
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisReplyEncoder);
        pipeline->AddLast("handler", new RedisRequestHandler(serv));
    }
    void RedisRequestHandler::PipelineDestroy(ChannelPipeline* pipeline, void* data)
    {
        ChannelHandler* handler = pipeline->Get("decoder");
        DELETE(handler);
        handler = pipeline->Get("encoder");
        DELETE(handler);
        RedisRequestHandler* rhandler = (RedisRequestHandler*) pipeline->Get("handler");
        if (NULL != rhandler && rhandler->m_ctx.processing)
        {
            rhandler->m_delete_after_processing = true;
        }
        else
        {
            DELETE(handler);
        }
    }

    void RedisRequestHandler::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
    {
        m_ctx.client = ctx.GetChannel();
        RedisCommandFrame* cmd = e.GetMessage();
        ChannelService& serv = m_ctx.client->GetService();
        uint32 channel_id = ctx.GetChannel()->GetID();
        m_ctx.processing = true;
        m_ctx.reply.pool->Clear();
        m_ctx.current_cmd = NULL;
        int ret = m_db->Call(m_ctx, *cmd, 0);
        if (ret >= 0 && m_ctx.reply.type != 0)
        {
            m_ctx.client->Write(m_ctx.reply);
        }
        if (m_delete_after_processing)
        {
            delete this;
            return;
        }
        if (ret < 0 && serv.GetChannel(channel_id) != NULL)
        {
            m_ctx.client->Close();
        }
        else
        {
            m_ctx.processing = false;
            if (m_ctx.close_after_processed)
            {
                //delete this;
                m_ctx.client->Close();
            }
        }
    }
    void RedisRequestHandler::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        m_db->FreeClientContext(m_ctx);
        m_db->GetStatistics().IncAcceptedClient(m_ctx.server_address, -1);
    }
    void RedisRequestHandler::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        m_ctx.born_time = get_current_epoch_micros();
        m_ctx.last_interaction_ustime = get_current_epoch_micros();
        m_ctx.client = ctx.GetChannel();
        m_ctx.identity = CONTEXT_NORMAL_CONNECTION;
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
        m_db->GetStatistics().IncAcceptedClient(m_ctx.server_address, 1);
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
OP_NAMESPACE_END


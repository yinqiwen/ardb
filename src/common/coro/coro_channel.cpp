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
#include "coro_channel.hpp"

OP_NAMESPACE_BEGIN

    CoroChannel::CoroChannel(Channel* ch) :
            m_io_serv(NULL), m_ch(ch), m_coro(NULL), m_waiting_coro(NULL), m_connect_success(false), m_readed_content(NULL), m_timeout(false), m_timeout_taskid(
                    -1)
    {
        m_coro = Scheduler::CurrentScheduler().GetCurrentCoroutine();
        m_io_serv = &(ch->GetService());
        //g_coro_scheduler.GetValue().StartCoro(0, coro_channel_func, this);
    }
    void CoroChannel::Init()
    {
        m_ch->ClearPipeline();
        m_ch->GetPipeline().AddLast("handler", (ChannelUpstreamHandler<Buffer>*) this);
    }
    void CoroChannel::WakeupCoro()
    {
        if (NULL != m_waiting_coro)
        {
            Scheduler& scheduler = Scheduler::CurrentScheduler();
            m_waiting_coro = NULL;
            scheduler.Wakeup(m_coro);
        }
    }
    int CoroChannel::WaitCoro()
    {
        if (NULL != m_waiting_coro)
        {
            ERROR_LOG("Duplicate wait on same coroutine.");
            return -1;
        }
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        m_waiting_coro = m_coro;
        scheduler.Wait(m_coro);
        return 0;
    }
    void CoroChannel::Run()
    {
        m_timeout = true;
        WakeupCoro();
    }
    bool CoroChannel::IsTimeout()
    {
        return m_timeout;
    }
    void CoroChannel::CreateTimeoutTask(int timeout)
    {
        CancelTimeoutTask();
        m_timeout = false;
        if (timeout > 0)
        {
            m_timeout_taskid = m_io_serv->GetTimer().Schedule(this, timeout, -1, MILLIS);
        }
    }
    void CoroChannel::CancelTimeoutTask()
    {
        m_timeout = false;
        if (NULL != m_io_serv && m_timeout_taskid > 0)
        {
            m_io_serv->GetTimer().Cancel((uint32) m_timeout_taskid);
            m_timeout_taskid = -1;
        }
    }
    void CoroChannel::SetConnectResult(bool v)
    {
        m_connect_success = v;
        WakeupCoro();
    }
    void CoroChannel::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<Buffer>& e)
    {
        Buffer* buf = e.GetMessage();
        SetReadedContent(buf);
        WakeupCoro();
    }
    void CoroChannel::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        SetConnectResult(false);
    }
    void CoroChannel::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        SetConnectResult(true);
    }
    void CoroChannel::Close()
    {
        if (NULL != m_ch)
        {
            while(NULL != m_ch->GetPipeline().RemoveFirst());
            m_ch->Close();
            m_ch = NULL;
        }
    }
    CoroChannel::~CoroChannel()
    {
        CancelTimeoutTask();
        Close();
    }
    void CoroChannel::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        WakeupCoro();
    }
    bool CoroChannel::SyncConnect(Address* addr, int timeout)
    {
        if (NULL == m_ch)
        {
            return false;
        }
        if (!m_ch->Connect(addr))
        {
            return false;
        }
        CreateTimeoutTask(timeout);
        if (0 != WaitCoro())
        {
            return -1;
        }
        if (IsTimeout())
        {
            return false;
        }
        return m_connect_success;
    }
    int CoroChannel::SyncRead(Buffer& buffer, int timeout)
    {
        CreateTimeoutTask(timeout);
        if (0 != WaitCoro())
        {
            return -1;
        }
        if (NULL != m_readed_content)
        {
            buffer.Write(m_readed_content, m_readed_content->ReadableBytes());
        }
        SetReadedContent(NULL);
        return 0;
    }

    int CoroChannel::SyncWrite(Buffer& buffer, int timeout)
    {
        uint64 start = timeout > 0 ? get_current_epoch_millis() : 0;
        m_ch->GetOutputBuffer().Write(&buffer, buffer.ReadableBytes());
        m_ch->EnableWriting();
        CreateTimeoutTask(timeout);
        while (m_ch->GetOutputBuffer().ReadableBytes() > 0)
        {
            if (timeout > 0)
            {
                if (get_current_epoch_millis() - start >= timeout)
                {
                    return -1;
                }
            }
            if (0 != WaitCoro())
            {
                return -1;
            }
        }
        return 0;
    }

    CoroRedisClient::CoroRedisClient(Channel* ch) :
            CoroChannel(ch), m_expected_multi_reply_count(0)
    {

    }

    void CoroRedisClient::Init()
    {
        m_ch->ClearPipeline();

        m_ch->GetPipeline().AddLast("decoder", &m_decoder);
        m_ch->GetPipeline().AddLast("encoder", &m_encoder);
        m_ch->GetPipeline().AddLast("handler", (ChannelUpstreamHandler<RedisReply>*) this);
    }

    CoroRedisClient::~CoroRedisClient()
    {
        Clear();
    }

    void CoroRedisClient::Clear()
    {
        if (m_expected_multi_reply_count > 1)
        {
            for (size_t i = 0; i < m_multi_replies.size(); i++)
            {
                if (m_multi_replies[i] != &m_error_reply)
                {
                    DELETE(m_multi_replies[i]);
                }
            }
        }
        m_multi_replies.clear();
        m_expected_multi_reply_count = 1;
    }

    void CoroRedisClient::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisReply>& e)
    {
        SetReply(e.GetMessage());

    }
    void CoroRedisClient::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        SetConnectResult(false);
    }
    void CoroRedisClient::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        SetConnectResult(true);
    }

    void CoroRedisClient::FillErrorReply()
    {
        while (m_multi_replies.size() < m_expected_multi_reply_count)
        {
            m_multi_replies.push_back(&m_error_reply);
        }
    }

    void CoroRedisClient::SetReply(RedisReply* reply)
    {
        if (1 == m_expected_multi_reply_count)
        {
            m_multi_replies.resize(1);
            m_multi_replies[0] = reply;
            WakeupCoro();
        }
        else
        {
            if (NULL != reply)
            {
                RedisReply* clone = new RedisReply;
                clone_redis_reply(*reply, *clone);
                m_multi_replies.push_back(clone);
            }
            else
            {
                m_multi_replies.push_back(NULL);
            }
            if (m_multi_replies.size() == m_expected_multi_reply_count)
            {
                WakeupCoro();
            }
        }
    }

    RedisReplyArray* CoroRedisClient::SyncMultiCall(RedisCommandFrameArray& cmds, int timeout)
    {
        Clear();
        m_expected_multi_reply_count = cmds.size();
        for (size_t i = 0; i < cmds.size(); i++)
        {
            m_ch->Write(cmds[i]);
        }
        CreateTimeoutTask(timeout);
        if (0 != WaitCoro())
        {
            return NULL;
        }
        if (!m_connect_success)
        {
            m_error_reply.SetErrorReason("client connection closed.");
            FillErrorReply();
            return &m_multi_replies;
        }
        if (IsTimeout())
        {
            m_error_reply.SetErrorReason("server timeout.");
            FillErrorReply();
            return &m_multi_replies;
        }
        CancelTimeoutTask();
        return &m_multi_replies;
    }

    RedisReply* CoroRedisClient::SyncCall(RedisCommandFrame& cmd, int timeout)
    {
        Clear();
        m_expected_multi_reply_count = 1;
        m_ch->Write(cmd);
        CreateTimeoutTask(timeout);
        if (0 != WaitCoro())
        {
            return NULL;
        }
        if (!m_connect_success)
        {
            m_error_reply.SetErrorReason("client connection closed.");
            return &m_error_reply;
        }
        if (IsTimeout())
        {
            m_error_reply.SetErrorReason("server timeout");
            return &m_error_reply;
        }
        RedisReply* r = NULL;
        if (m_multi_replies.size() > 0)
        {
            r = m_multi_replies[0];
        }
        CancelTimeoutTask();
        return r;
    }
OP_NAMESPACE_END


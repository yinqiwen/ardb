/*
 * coro_channel.cpp
 *
 *  Created on: 2016年4月30日
 *      Author: wangqiying
 */
#include "coro_channel.hpp"

OP_NAMESPACE_BEGIN

    CoroChannel::CoroChannel(Channel* ch) :
            m_ch(ch), m_coro(NULL), m_connect_success(false), m_readed_content(NULL), m_timeout(false)
    {
        m_coro = Scheduler::CurrentScheduler().GetCurrentCoroutine();
        //g_coro_scheduler.GetValue().StartCoro(0, coro_channel_func, this);
    }
    void CoroChannel::Init()
    {
        m_ch->ClearPipeline();
        m_ch->GetPipeline().AddLast("handler", (ChannelUpstreamHandler<Buffer>*) this);
    }
    void CoroChannel::Run()
    {
        m_timeout = true;
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        scheduler.Wakeup(m_coro);
    }
    bool CoroChannel::IsTimeout()
    {
        return m_timeout;
    }
    void CoroChannel::CreateTimeoutTask(int timeout)
    {
        m_timeout = false;
        if (NULL != m_ch && timeout > 0)
        {
            m_ch->GetService().GetTimer().Schedule(this, timeout, -1, MILLIS);
        }
    }
    void CoroChannel::SetConnectResult(bool v)
    {
        m_connect_success = v;
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        scheduler.Wakeup(m_coro);
    }
    void CoroChannel::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<Buffer>& e)
    {
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        Buffer* buf = e.GetMessage();
        SetReadedContent(buf);
        scheduler.Wakeup(m_coro);
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
        if (NULL == m_ch)
        {
            m_ch->Close();
            m_ch = NULL;
        }
    }
    CoroChannel::~CoroChannel()
    {
        Close();
    }
    void CoroChannel::ChannelWritable(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        //coro->SetConnectResult(true);
        scheduler.Wakeup(m_coro);
    }
    bool CoroChannel::SyncConnect(Address* addr, int timeout)
    {
        if (NULL == m_ch)
        {
            return false;
        }
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        if (!m_ch->Connect(addr))
        {
            return false;
        }
        CreateTimeoutTask(timeout);
        scheduler.Wait(scheduler.GetCurrentCoroutine());
        if (IsTimeout())
        {
            return false;
        }
        return m_connect_success;
    }
    int CoroChannel::SyncRead(Buffer& buffer, int timeout)
    {
        CreateTimeoutTask(timeout);
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        scheduler.Wait(scheduler.GetCurrentCoroutine());
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
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        while (m_ch->GetOutputBuffer().ReadableBytes() > 0)
        {
            if (timeout > 0)
            {
                if (get_current_epoch_millis() - start >= timeout)
                {
                    return -1;
                }
            }
            scheduler.Wait(scheduler.GetCurrentCoroutine());
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
            Scheduler& scheduler = Scheduler::CurrentScheduler();
            scheduler.Wakeup(m_coro);
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
                Scheduler& scheduler = Scheduler::CurrentScheduler();
                scheduler.Wakeup(m_coro);
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
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        scheduler.Wait(scheduler.GetCurrentCoroutine());
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
        return &m_multi_replies;
    }

    RedisReply* CoroRedisClient::SyncCall(RedisCommandFrame& cmd, int timeout)
    {
        Clear();
        m_expected_multi_reply_count = 1;
        m_ch->Write(cmd);
        CreateTimeoutTask(timeout);
        Scheduler& scheduler = Scheduler::CurrentScheduler();
        scheduler.Wait(scheduler.GetCurrentCoroutine());
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
        return r;
    }
OP_NAMESPACE_END


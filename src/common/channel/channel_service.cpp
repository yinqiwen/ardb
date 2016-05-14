/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "channel/all_includes.hpp"
#include "util/helpers.hpp"
#include "util/datagram_packet.hpp"
#include "buffer/buffer_helper.hpp"
#include <list>

using namespace ardb;

ChannelService::ChannelService(uint32 setsize) :
        m_setsize(setsize), m_eventLoop(NULL), m_timer(NULL), m_signal_channel(
        NULL), m_self_soft_signal_channel(NULL), m_running(false), m_thread_pool_size(1), m_tid(0), m_lifecycle_callback(NULL), m_pool_index(0), m_parent(NULL)
{
    m_eventLoop = aeCreateEventLoop(m_setsize);
    m_self_soft_signal_channel = NewSoftSignalChannel();
    if (NULL != m_self_soft_signal_channel)
    {
        m_self_soft_signal_channel->Register(CHANNEL_REMOVE, this);
        m_self_soft_signal_channel->Register(USER_DEFINED, this);

        m_self_soft_signal_channel->Register(WAKEUP, this);
        m_self_soft_signal_channel->Register(CHANNEL_ASNC_IO, this);
    }
}

//void ChannelService::RegisterUserEventCallback(UserEventCallback* cb, void* data)
//{
//    m_user_cb = cb;
//    m_user_cb_data = data;
//}
void ChannelService::RegisterLifecycleCallback(ChannelServiceLifeCycle* callback)
{
    m_lifecycle_callback = callback;
}

void ChannelService::FireUserEvent(uint32 ev)
{
    if (NULL != m_self_soft_signal_channel)
    {
        m_self_soft_signal_channel->FireSoftSignal(USER_DEFINED, ev);
    }
    ChannelServicePool::iterator it = m_sub_pool.begin();
    while (it != m_sub_pool.end())
    {
        ChannelService* serv = *it;
        if (NULL != serv->m_self_soft_signal_channel)
        {
            serv->m_self_soft_signal_channel->FireSoftSignal(USER_DEFINED, ev);
        }
        it++;
    }
}

void ChannelService::SetThreadPoolSize(uint32 size)
{
    m_thread_pool_size = size;
}

uint32 ChannelService::GetThreadPoolSize()
{
    return m_thread_pool_size;
}

ChannelService& ChannelService::GetNextChannelService()
{
    static uint32 idx = 0;
    if (m_sub_pool.empty())
    {
        return *this;
    }
    if (idx >= m_sub_pool.size())
    {
        idx = 0;
    }
    return *(m_sub_pool[idx++]);
}

ChannelService& ChannelService::GetIdlestChannelService(uint32 min, uint32 max)
{
    if (m_sub_pool.empty())
    {
        return *this;
    }
    if (max >= m_sub_pool.size())
    {
        max = m_sub_pool.size();
    }
    if (min >= m_sub_pool.size())
    {
        min = m_sub_pool.size() - 1;
    }
    if (min == max)
    {
        max = min + 1;
    }
    uint32 size = 0;
    uint32 idx = 0;
    for (uint32 i = min; i < max; i++)
    {
        if (size == 0 || m_sub_pool[i]->m_channel_table.size() < size)
        {
            size = m_sub_pool[i]->m_channel_table.size();
            idx = i;
        }
    }
    return *(m_sub_pool[idx]);
}

void ChannelService::OnSoftSignal(uint32 soft_signo, uint32 appendinfo)
{
    switch (soft_signo)
    {
        case CHANNEL_REMOVE:
        {
            //uint32 chanel_id = appendinfo;
            VerifyRemoveQueue();
            break;
        }
        case WAKEUP:
        {
            Runnable* task = NULL;
            while (m_pending_tasks.Pop(task))
            {
                if (NULL != task)
                {
                    task->Run();
                }
            }
            break;
        }
        case USER_DEFINED:
        {
//            if (NULL != m_user_cb)
//            {
//                m_user_cb(this, appendinfo, m_user_cb_data);
//            }
            break;
        }
        case CHANNEL_ASNC_IO:
        {
            ChannelAsyncIOContext ctx;
            while (m_async_io_queue.Pop(ctx))
            {
                if (NULL != ctx.cb)
                {
                    Channel* ch = GetChannel(ctx.channel_id);
                    ctx.cb(ch, ctx.data);
                }
            }
            break;
        }
        default:
        {
            break;
        }
    }
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline, ChannelStateEvent& e)
{
    return false;
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline, MessageEvent<Buffer>& e)
{
    Buffer* buffer = e.GetMessage();
    RETURN_FALSE_IF_NULL(buffer);
    uint32 len = buffer->ReadableBytes();
    int32 ret = e.GetChannel()->WriteNow(buffer);
    return ret >= 0 ? (len == (uint32) ret) : false;
}

bool ChannelService::EventSunk(ChannelPipeline* pipeline, MessageEvent<DatagramPacket>& e)
{
    DatagramPacket* packet = e.GetMessage();
    Channel* ch = e.GetChannel();
    DatagramChannel* sc = (DatagramChannel*) ch;
    return sc->Send(packet->GetInetAddress(), &(packet->GetBuffer())) > 0;
}

Channel* ChannelService::GetChannel(uint32_t channelID)
{
    if (m_channel_table.count(channelID) > 0)
    {
        return m_channel_table[channelID];
    }
    return NULL;
}

bool ChannelService::DetachChannel(Channel* ch, bool remove)
{
    RETURN_FALSE_IF_NULL(ch);
    ch->DetachFD();
    if (remove)
    {
        m_channel_table.erase(ch->GetID());
    }
    return true;
}

Channel* ChannelService::AttachChannel(Channel* ch, bool transfer_service_only)
{
    RETURN_NULL_IF_NULL(ch);
    if (transfer_service_only)
    {
        ch->m_service = this;
        ch->AttachFD();
        m_channel_table[ch->GetID()] = ch;
        return ch;
    }
    if (m_channel_table.count(ch->GetID()) > 0)
    {
        return NULL;
    }
    ch->DetachFD();

    if (ch->GetReadFD() != ch->GetWriteFD())
    {
        ERROR_LOG("Failed to attach channel since source channel has diff read fd & write fd.");
        return NULL;
    }
    Channel* newch = CloneChannel(ch);
    if (NULL != newch)
    {
        newch->AttachFD(ch->GetReadFD());
        newch->Configure(ch->m_options);
    }
    return newch;
}

Channel* ChannelService::CloneChannel(Channel* ch)
{
    RETURN_NULL_IF_NULL(ch);
    uint32 channelID = ch->GetID();
    uint32 type = channelID & 0xF;
    Channel* newch = NULL;
    switch (type)
    {
        case TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK:
        {
            newch = NewClientSocketChannel();
            break;
        }
        case TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK:
        {
            newch = NewServerSocketChannel();
            break;
        }
        case UDP_SOCKET_CHANNEL_ID_BIT_MASK:
        {
            newch = NewDatagramSocketChannel();
            break;
        }
        default:
        {
            ERROR_LOG("Not support clone non TCP/UDP socket channel.");
            return NULL;
        }
    }
    if (NULL != newch)
    {

        if (NULL != ch->m_pipeline_initializor)
        {
            newch->SetChannelPipelineInitializor(ch->m_pipeline_initializor, ch->m_pipeline_initailizor_user_data);
        }
        if (NULL != ch->m_pipeline_finallizer)
        {
            newch->SetChannelPipelineFinalizer(ch->m_pipeline_finallizer, ch->m_pipeline_finallizer_user_data);
        }
    }

    return newch;
}

void ChannelService::StartSubPool()
{
    if (m_thread_pool_size > 1)
    {
        struct LaunchThread: public Thread
        {
                ChannelService* serv;
                LaunchThread(ChannelService* s) :
                        serv(s)
                {
                }
                void Run()
                {
                    serv->Start();
                }
        };

        for (uint32 i = 0; i < m_thread_pool_size; i++)
        {
            ChannelService* s = new ChannelService(m_setsize);
            s->SetParent(this);
            s->m_pool_index = i + 1;
            s->RegisterLifecycleCallback(m_lifecycle_callback);
//            s->RegisterUserEventCallback(m_user_cb, m_user_cb_data);
            m_sub_pool.push_back(s);
            LaunchThread* launch = new LaunchThread(s);
            launch->Start();
            m_sub_pool_ts.push_back(launch);
        }
    }
}

void ChannelService::Start()
{
    if (!m_running)
    {
        StartSubPool();
        GetTimer().Schedule(this, 1000, 1000);
        m_running = true;
        m_tid = Thread::CurrentThreadID();
        if (NULL != m_lifecycle_callback)
        {
            m_lifecycle_callback->OnStart(this, m_pool_index);
        }
        aeMain(m_eventLoop);
    }
}

void ChannelService::Continue()
{
    aeProcessEvents(m_eventLoop, AE_FILE_EVENTS | AE_DONT_WAIT | AE_TIME_EVENTS);
}

void ChannelService::OnStopCB(Channel*, void* data)
{
    ChannelService* serv = (ChannelService*) data;
    ChannelServicePool::iterator it = serv->m_sub_pool.begin();
    while (it != serv->m_sub_pool.end())
    {
        ChannelService* sub = *it;
        sub->AsyncIO(0, OnStopCB, sub);
        it++;
    }
    aeStop(serv->m_eventLoop);
    serv->Wakeup();
}

void ChannelService::Stop()
{
    if (m_running)
    {
        m_running = false;
        if (NULL != m_lifecycle_callback)
        {
            m_lifecycle_callback->OnStop(this, m_pool_index);
        }

        if (!IsInLoopThread())
        {
            AsyncIO(0, OnStopCB, this);
        }
        else
        {
            OnStopCB(NULL, this);
        }

//        ThreadVector::iterator tit = m_sub_pool_ts.begin();
//        while (tit != m_sub_pool_ts.end())
//        {
//            (*tit)->Join();
//            delete *tit;
//            tit++;
//        }
    }
}

Timer& ChannelService::GetTimer()
{
    if (NULL == m_timer)
    {
        m_timer = NewTimerChannel();
    }
    return *m_timer;
}

SignalChannel& ChannelService::GetSignalChannel()
{
    if (NULL == m_signal_channel)
    {
        m_signal_channel = NewSignalChannel();
    }
    return *m_signal_channel;
}

TimerChannel* ChannelService::NewTimerChannel()
{
    TimerChannel* ch = NULL;
    NEW(ch, TimerChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + TIMER_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        //ch->GetPipeline().Attach(ch, this);
    }
    return ch;
}

SignalChannel* ChannelService::NewSignalChannel()
{
    SignalChannel* ch = NULL;
    NEW(ch, SignalChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + SIGNAL_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        //ch->GetPipeline().Attach(ch, this);
    }
    ch->Open();
    return ch;
}

SoftSignalChannel* ChannelService::NewSoftSignalChannel()
{
    SoftSignalChannel* ch = NULL;
    NEW(ch, SoftSignalChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + SOFT_SIGNAL_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        //ch->GetPipeline().Attach(ch, this);
    }
    ch->Open();
    return ch;
}

ClientSocketChannel* ChannelService::NewClientSocketChannel()
{
    ClientSocketChannel* ch = NULL;
    NEW(ch, ClientSocketChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + TCP_CLIENT_SOCKET_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        ch->GetPipeline().Attach(ch);
    }
    return ch;
}
ServerSocketChannel* ChannelService::NewServerSocketChannel()
{
    ServerSocketChannel* ch = NULL;
    NEW(ch, ServerSocketChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + TCP_SERVER_SOCKET_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        ch->GetPipeline().Attach(ch);
        ch->BindThreadPool(0, m_thread_pool_size);
    }
    return ch;
}

PipeChannel* ChannelService::NewPipeChannel(int readFd, int writeFD)
{
    PipeChannel* ch = NULL;
    NEW(ch, PipeChannel(*this, readFd, writeFD));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + FIFO_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        ch->GetPipeline().Attach(ch);
    }
    return ch;
}
DatagramChannel* ChannelService::NewDatagramSocketChannel()
{
    DatagramChannel* ch = NULL;
    NEW(ch, DatagramChannel(*this));
    if (NULL != ch)
    {
        Channel* c = ch;
        c->m_id = (((c->m_id) << 4) + UDP_SOCKET_CHANNEL_ID_BIT_MASK);
        m_channel_table[c->m_id] = c;
        ch->GetPipeline().Attach(ch);
    }
    return ch;
}

void ChannelService::RemoveChannel(Channel* ch)
{
    if (NULL != ch)
    {
        if (NULL != m_self_soft_signal_channel)
        {
            m_remove_queue.push_back(ch->GetID());
            m_self_soft_signal_channel->FireSoftSignal(CHANNEL_REMOVE, ch->GetID());
        }
    }
}

void ChannelService::VerifyRemoveQueue()
{
    RemoveChannelQueue::iterator it = m_remove_queue.begin();
    while (it != m_remove_queue.end())
    {
        Channel* ch = GetChannel(*it);
        if (NULL != ch)
        {
            DeleteChannel(ch);
        }
        it++;
    }
    m_remove_queue.clear();
}

void ChannelService::DeleteChannel(Channel* ch)
{
    RETURN_IF_NULL(ch);
    ChannelTable::iterator found = m_channel_table.find(ch->GetID());
    if (found != m_channel_table.end())
    {
        m_channel_table.erase(found);
    }
    DELETE(ch);
}

void ChannelService::Run()
{
    VerifyRemoveQueue();
    Routine();
}

void ChannelService::AsyncIO(const ChannelAsyncIOContext& ctx)
{
    m_async_io_queue.Push(ctx);
    if (NULL != m_self_soft_signal_channel)
    {
        m_self_soft_signal_channel->FireSoftSignal(CHANNEL_ASNC_IO, 1);
    }
}

void ChannelService::AttachAcceptedChannel(SocketChannel *ch)
{
    if (IsInLoopThread())
    {
        ch->OnAccepted();
    }
    else
    {
        ch->m_detached = true;
        ch->GetService().DetachChannel(ch, true);
        struct ChannelTask: public Runnable
        {
                SocketChannel * sch;
                ChannelService* serv;
                ChannelTask(SocketChannel * c, ChannelService* s) :
                        sch(c), serv(s)
                {
                }
                void Run()
                {
                    sch->m_service = serv;
                    sch->m_service->m_channel_table[sch->GetID()] = sch;
                    sch->OnAccepted();
                    delete this;
                }
        };
        ChannelTask* t = new ChannelTask(ch, this);
        //m_pending_tasks.write(t, false);
        //m_pending_tasks.flush();
        m_pending_tasks.Push(t);
        Wakeup();
    }
}

void ChannelService::Routine()
{
    if (NULL != m_lifecycle_callback)
    {
        m_lifecycle_callback->OnRoutine(this, m_pool_index);
    }
}

void ChannelService::Wakeup()
{
    if (NULL != m_self_soft_signal_channel)
    {
        m_self_soft_signal_channel->FireSoftSignal(WAKEUP, 1);
    }
}

bool ChannelService::IsInLoopThread() const
{
    return m_tid == Thread::CurrentThreadID();
}

void ChannelService::CloseAllChannelFD(std::set<Channel*>& exceptions)
{
    ChannelTable::iterator it = m_channel_table.begin();
    while (it != m_channel_table.end())
    {
        Channel* ch = it->second;
        if (exceptions.find(ch) == exceptions.end())
        {
            ::close(ch->GetReadFD());
            ::close(ch->GetWriteFD());
        }
        it++;
    }
}

void ChannelService::CloseAllChannels(bool fireCloseEvent)
{
    ChannelTable tmp = m_channel_table;
    ChannelTable::iterator it = tmp.begin();
    while (it != tmp.end())
    {
        Channel* ch = it->second;
        if (fireCloseEvent)
        {
            ch->Close();
        }
        DeleteChannel(ch);
        it++;
    }
    m_channel_table.clear();
}

void ChannelService::AsyncIO(uint32 id, ChannelAsyncIOCallback* cb, void* data)
{
    ChannelAsyncIOContext ctx;
    ctx.channel_id = id;
    ctx.cb = cb;
    ctx.data = data;
    AsyncIO(ctx);
}

ChannelService::~ChannelService()
{
    CloseAllChannels(false);
    aeDeleteEventLoop(m_eventLoop);
    ThreadVector::iterator tit = m_sub_pool_ts.begin();
    while (tit != m_sub_pool_ts.end())
    {
        (*tit)->Join();
        delete *tit;
        tit++;
    }
}

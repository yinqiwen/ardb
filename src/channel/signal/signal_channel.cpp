/*
 * signal_channel.cpp
 *
 *  Created on: 2011-4-23
 *      Author: wqy
 */
#include "channel/all_includes.hpp"
#include "util/helpers.hpp"
#include <signal.h>

using namespace rddb;

static char kReadSigInfoBuf[sizeof(int) + sizeof(siginfo_t)];
static char kWriteSigInfoBuf[sizeof(int) + sizeof(siginfo_t)];

SignalChannel* SignalChannel::m_singleton_instance = NULL;



void SignalChannel::SignalCB(int signo, siginfo_t* info, void* ctx)
{
    if (NULL != m_singleton_instance
            && m_singleton_instance->m_self_write_pipe_fd > 0)
    {
        memcpy(kWriteSigInfoBuf, &signo, sizeof(int));
        memcpy(kWriteSigInfoBuf + sizeof(int), info, sizeof(siginfo_t));
        uint32 writed = 0;
        uint32 total = (sizeof(int) + sizeof(siginfo_t));
        while (writed < total)
        {
            int ret = ::write(m_singleton_instance->m_self_write_pipe_fd,
                    kWriteSigInfoBuf + writed, total - writed);
            if (ret >= 0)
            {
                writed += ret;
            }
            else
            {
                return;
            }
        }
    }
}

SignalChannel::SignalChannel(ChannelService& factory) :
        Channel(NULL, factory), m_self_read_pipe_fd(-1), m_self_write_pipe_fd(
                -1), m_readed_siginfo_len(0)
{
}

bool SignalChannel::DoOpen()
{
    int pipefd[2];
    int ret = pipe(pipefd);
    if (ret == -1)
    {
        ERROR_LOG("Failed to create pipe for signal channel.");
        return false;
    }
    m_self_read_pipe_fd = pipefd[0];
    m_self_write_pipe_fd = pipefd[1];
    rddb::make_fd_nonblocking(m_self_read_pipe_fd);
    rddb::make_fd_nonblocking(m_self_write_pipe_fd);
    aeCreateFileEvent(m_service.GetRawEventLoop(), m_self_read_pipe_fd,
            AE_READABLE, Channel::IOEventCallback, this);
    m_singleton_instance = this;
    return true;
}

int SignalChannel::GetWriteFD()
{
    return m_self_write_pipe_fd;
}
int SignalChannel::GetReadFD()
{
    return m_self_read_pipe_fd;
}

bool SignalChannel::DoClose()
{
    if (-1 != m_self_read_pipe_fd && -1 != m_self_write_pipe_fd)
    {
        ::close(m_self_read_pipe_fd);
        ::close(m_self_write_pipe_fd);
        m_self_read_pipe_fd = -1;
        m_self_write_pipe_fd = -1;
    }
    return true;
}

void SignalChannel::OnRead()
{
    uint32 sig_ifo_len = sizeof(int) + sizeof(siginfo_t);
    if (m_readed_siginfo_len < sig_ifo_len)
    {
        int readed = ::read(m_self_read_pipe_fd,
                kReadSigInfoBuf + m_readed_siginfo_len,
                (sig_ifo_len - m_readed_siginfo_len));
        if (readed > 0)
        {
            m_readed_siginfo_len += readed;
            if (m_readed_siginfo_len == sig_ifo_len)
            {
                m_readed_siginfo_len = 0;
                int signo;
                siginfo_t info;
                memcpy(&signo, kReadSigInfoBuf, sizeof(int));
                memcpy(&info, kReadSigInfoBuf + sizeof(int), sizeof(siginfo_t));
                FireSignalReceived(signo, info);
            }
        }
    }
}

void SignalChannel::FireSignalReceived(int signo, siginfo_t& info)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it != m_hander_map.end())
    {
        std::vector<SignalHandler*>& vec = it->second;
        std::vector<SignalHandler*>::iterator vecit = vec.begin();
        while (vecit != vec.end())
        {
            SignalHandler* handler = *vecit;
            if (NULL != handler)
            {
                handler->OnSignal(signo, info);
            }
            vecit++;
        }
    }
}

void SignalChannel::Register(uint32 signo, SignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it == m_hander_map.end() || it->second.empty())
    {
        struct sigaction action;
        action.sa_sigaction = SignalChannel::SignalCB;
        sigemptyset(&action.sa_mask);
        action.sa_flags = SA_SIGINFO;
        sigaction(signo, &action, NULL);
        m_hander_map[signo].push_back(handler);
    }
    else
    {
        it->second.push_back(handler);
    }
}
void SignalChannel::Unregister(SignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.begin();
    while (it != m_hander_map.end())
    {
        std::vector<SignalHandler*>& vec = it->second;
        std::vector<SignalHandler*>::iterator vecit = vec.begin();
        while (vecit != vec.end())
        {
            if (handler == (*vecit))
            {
                vec.erase(vecit);
                break;
            }
            vecit++;
        }
        it++;
    }
}

void SignalChannel::Clear()
{
    m_hander_map.clear();
}

SignalChannel::~SignalChannel()
{
    Clear();
    m_singleton_instance = NULL;
}

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
#include <signal.h>

using namespace ardb;

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
    ardb::make_fd_nonblocking(m_self_read_pipe_fd);
    ardb::make_fd_nonblocking(m_self_write_pipe_fd);
    aeCreateFileEvent(GetService().GetRawEventLoop(), m_self_read_pipe_fd,
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

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
#include <errno.h>
#include <string.h>
using namespace ardb;

int EventFD::OpenNonBlock()
{
#if (HAVE_EVENTFD)
    int ret = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (ret == -1)
    {
        int err = errno;
        ERROR_LOG("Failed to create eventfd for signal channel:%s", strerror(err));
        return -1;
    }
    write_fd = ret;
    read_fd = ret;
#else
    int pipefd[2];
    int ret = pipe(pipefd);
    if (ret == -1)
    {
        ERROR_LOG("Failed to create pipe for signal channel.");
        return -1;
    }
    read_fd = pipefd[0];
    write_fd = pipefd[1];
    ardb::make_fd_nonblocking(write_fd);
    ardb::make_fd_nonblocking(read_fd);
#endif
    return 0;
}

int EventFD::Close()
{
    if (-1 != read_fd)
    {
        ::close(read_fd);
        read_fd = -1;
    }
    if (-1 != write_fd)
    {
        ::close(write_fd);
        write_fd = -1;
    }
    m_readed_siginfo_len = 0;
    return 0;
}

int EventFD::WriteEvent(uint64 ev)
{
    if (write_fd > -1)
    {
#if (HAVE_EVENTFD)
        if (ev == 0xffffffffffffffffULL)
        {
            //Not allowed value.
            ERROR_LOG(
                    "EventFD:%llu is not allowed.", ev);
            return -1;
        }
#endif
        memcpy(_kWriteSigInfoBuf, &ev, sizeof(uint64));
        uint32 writed = 0;
        uint32 total = sizeof(uint64);
        while (writed < total)
        {
            int ret = ::write(write_fd, _kWriteSigInfoBuf + writed,
                    total - writed);
            if (ret >= 0)
            {
                writed += ret;
            }
            else
            {
                return -1;
            }
        }
    }
    return -1;
}

int EventFD::ReadEvent(uint64& ev)
{
    uint32 sig_ifo_len = sizeof(uint64);
    if (m_readed_siginfo_len < sig_ifo_len)
    {
        int readed = ::read(read_fd, _kReadSigInfoBuf + m_readed_siginfo_len,
                (sig_ifo_len - m_readed_siginfo_len));
        if (readed > 0)
        {
            m_readed_siginfo_len += readed;
            if (m_readed_siginfo_len == sig_ifo_len)
            {
                m_readed_siginfo_len = 0;
                memcpy(&ev, _kReadSigInfoBuf, sizeof(uint64));
                return 0;
            }
        }
    }
    return -1;
}

SoftSignalChannel::SoftSignalChannel(ChannelService& factory) :
        Channel(NULL, factory)
{
}

void SoftSignalChannel::AttachEventFD(EventFD* fd)
{
    m_event_fd.read_fd = fd->read_fd;
    m_event_fd.write_fd = fd->write_fd;
    DoOpen();
}

int SoftSignalChannel::GetWriteFD()
{
    return m_event_fd.write_fd;
}
int SoftSignalChannel::GetReadFD()
{
    return m_event_fd.read_fd;
}

bool SoftSignalChannel::DoOpen()
{
    if (!m_event_fd.IsOpen())
    {
        m_event_fd.OpenNonBlock();
    }
    aeCreateFileEvent(GetService().GetRawEventLoop(), m_event_fd.read_fd,
            AE_READABLE, Channel::IOEventCallback, this);
    return true;
}

bool SoftSignalChannel::DoClose()
{
    if (m_event_fd.IsOpen())
    {
        m_event_fd.Close();
    }
    return true;
}

void SoftSignalChannel::OnRead()
{
    uint64 v;
    if (0 == m_event_fd.ReadEvent(v))
    {
        uint32 signo = v & 0xFFFFFFFF;
        uint32 info = ((v >> 32) & 0xFFFFFFFF);
        FireSignalReceived(signo, info);
    }
}

void SoftSignalChannel::FireSignalReceived(uint32 signo, uint32 append_info)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it != m_hander_map.end())
    {
        std::vector<SoftSignalHandler*>& vec = it->second;
        std::vector<SoftSignalHandler*>::iterator vecit = vec.begin();
        while (vecit != vec.end())
        {
            SoftSignalHandler* handler = *vecit;
            if (NULL != handler)
            {
                handler->OnSoftSignal(signo, append_info);
            }
            vecit++;
        }
    }
}

int SoftSignalChannel::FireSoftSignal(uint32 signo, uint32 info)
{
    if(!m_event_fd.IsOpen())
    {
        m_event_fd.OpenNonBlock();
    }
    uint64 v = info;
    v = (v << 32) + signo;
    return m_event_fd.WriteEvent(v);
}

void SoftSignalChannel::Register(uint32 signo, SoftSignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.find(signo);
    if (it == m_hander_map.end() || it->second.empty())
    {
        m_hander_map[signo].push_back(handler);
    }
    else
    {
        it->second.push_back(handler);
    }
}
void SoftSignalChannel::Unregister(SoftSignalHandler* handler)
{
    SignalHandlerMap::iterator it = m_hander_map.begin();
    while (it != m_hander_map.end())
    {
        std::vector<SoftSignalHandler*>& vec = it->second;
        std::vector<SoftSignalHandler*>::iterator vecit = vec.begin();
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

void SoftSignalChannel::Clear()
{
    m_hander_map.clear();
}

SoftSignalChannel::~SoftSignalChannel()
{
    Clear();
}

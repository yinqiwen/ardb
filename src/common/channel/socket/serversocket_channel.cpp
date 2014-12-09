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
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>

using namespace ardb;

ServerSocketChannel::ServerSocketChannel(ChannelService& factory) :
        SocketChannel(factory), m_connected_socks(0), m_pool_min(0), m_pool_max(0)
{
}

bool ServerSocketChannel::DoConfigure(const ChannelOptions& options)
{
    if (!SocketChannel::DoConfigure(options))
    {
        return false;
    }
    return true;
}

bool ServerSocketChannel::DoConnect(Address* remote)
{
    ERROR_LOG("DoConnect() is not supported in server socket channel.");
    return false;
}

bool ServerSocketChannel::DoBind(Address* local)
{
    if (m_fd > 0)
    {
        return true;
    }
    int on = 1;
    SocketInetAddress addr;
    if (InstanceOf<SocketHostAddress>(local).OK)
    {
        SocketHostAddress* host_addr = (SocketHostAddress*) local;
        addr = get_inet_address(host_addr->GetHost(), host_addr->GetPort());
        char tmp[1024];
        sprintf(tmp, "%s:%u", host_addr->GetHost().c_str(), host_addr->GetPort());
        m_adress_str = tmp;
    }
    else if (InstanceOf<SocketInetAddress>(local).OK)
    {
        SocketInetAddress* inet_addr = (SocketInetAddress*) local;
        addr = (*inet_addr);
    }
    else if (InstanceOf<SocketUnixAddress>(local).OK)
    {
        SocketUnixAddress* unix_addr = (SocketUnixAddress*) local;
        int ret = unlink(unix_addr->GetPath().c_str()); // in case it already exists
        if (ret == -1)
        {
            int e = errno;
            ERROR_LOG("unlink %s failed:%s", unix_addr->GetPath().c_str(), strerror(e));
        }
        addr = get_inet_address(*unix_addr);
        m_adress_str = unix_addr->GetPath();
    }
    else
    {
        return false;
    }
    int family = addr.GetRawSockAddr().sa_family;
    int fd = ::socket(family, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return false;
    }

    if (make_fd_nonblocking(fd) < 0)
    {
        ::close(fd);
        return false;
    }
    if (!addr.IsUnix())
    {
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1)
        {
            ::close(fd);
            return false;
        }
    }
    else
    {
        struct sockaddr_un* pun = (struct sockaddr_un*) &(addr.GetRawSockAddr());
        DEBUG_LOG("Bind on %s", pun->sun_path);
    }

    int flag = 1;
    if (m_options.reuse_address)
    {
        int ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
        if (ret != 0)
        {
            WARN_LOG("Failed to set SO_REUSEADDR for socket.");
        }
    }
    //setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void*) &on, sizeof(on));
    if (::bind(fd, (struct sockaddr*) &(addr.GetRawSockAddr()), addr.GetRawSockAddrSize()) == -1)
    {
        int e = errno;
        ERROR_LOG("Failed to bind address for reason:%s", strerror(e));
        ::close(fd);
        return false;
    }
    if (::listen(fd, 511) == -1)
    { /* the magic 511 constant is from nginx */
        int e = errno;
        ERROR_LOG("Failed to listen for reason:%s", strerror(e));
        ::close(fd);
        return false;
    }
    if (aeCreateFileEvent(GetService().GetRawEventLoop(), fd, AE_READABLE, Channel::IOEventCallback, this) == AE_ERR)
    {
        ::close(fd);
        return false;
    }
    m_fd = fd;
    return true;
}

uint32 ServerSocketChannel::ConnectedSockets()
{
    return m_connected_socks;
}

void ServerSocketChannel::OnChildClose(Channel* ch)
{
    m_connected_socks--;
}

//static void DelayAttach(ServerSocketChannel* ss)
//{
//    if (ss->IsDetached())
//    {
//        DEBUG_LOG("Attach read FD again.");
//        ss->AttachFD();
//    }
//}

void ServerSocketChannel::OnRead()
{
    int fd;
    char addrbuf[128];
    while (1)
    {
        socklen_t salen = sizeof(addrbuf);
        struct sockaddr* sa = (struct sockaddr*) addrbuf;
#if (HAVE_ACCEPT4)
        fd = accept4(m_fd, sa, &salen,SOCK_NONBLOCK);
#else
        fd = ::accept(m_fd, sa, &salen);
        if (fd != -1 && -1 == make_fd_nonblocking(fd))
        {
            ERROR_LOG("failed to set opt for accept socket");
            ::close(fd);
            return;
        }
#endif
        if (-1 == fd)
        {
            if (errno == EINTR)
            {
                continue;
            }
            else
            {
                return;
            }
        }
        ClientSocketChannel * ch = GetService().NewClientSocketChannel();
//		if (aeCreateFileEvent(serv.GetRawEventLoop(), fd, AE_READABLE,
//				Channel::IOEventCallback, ch) == AE_ERR)
//		{
//			int err = errno;
//			ERROR_LOG(
//					"Failed to add event for accepted client for fd:%d for reason:%s", fd, strerror(err));
//			serv.DeleteChannel(ch);
//			::close(fd);
//			return;
//		}

        ch->m_fd = fd;
        if (m_user_configed)
        {
            ch->Configure(m_options);
        }
        if (NULL != m_pipeline_initializor)
        {
            DEBUG_LOG("Inherit pipeline initializor from server socket.");
            ch->SetChannelPipelineInitializor(m_pipeline_initializor, m_pipeline_initailizor_user_data);
        }
        if (NULL != m_pipeline_finallizer)
        {
            DEBUG_LOG("Inherit pipeline finalizer from server socket.");
            ch->SetChannelPipelineFinalizer(m_pipeline_finallizer, m_pipeline_finallizer_user_data);
        }
        ch->SetParent(this);
        if (sa->sa_family != AF_UNIX)
        {
            SocketHostAddress* remote = NULL;
            NEW(remote, SocketHostAddress(get_host_address(*sa)));
            ch->setRemoteAddress(remote);
        }

        DEBUG_LOG("Server channel(%u) Accept a client channel(%u) for fd:%d", GetID(), ch->GetID(), fd);
//		fire_channel_open(ch);
//		fire_channel_connected(ch);
        m_connected_socks++;
        GetService().GetIdlestChannelService(m_pool_min, m_pool_max).AttachAcceptedChannel(ch);
    }

}

void ServerSocketChannel::BindThreadPool(uint32 min, uint32 max)
{
    m_pool_min = min;
    m_pool_max = max;
}

ServerSocketChannel::~ServerSocketChannel()
{
}

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
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <fcntl.h>

using namespace ardb;

bool SocketChannel::DoConfigure(const ChannelOptions& options)
{
    if (!Channel::DoConfigure(options))
    {
        return false;
    }
    if (m_fd < 0)
    {
        ERROR_LOG("Socket fd is not created before config.");
        return false;
    }
    if (options.receive_buffer_size > 0)
    {
        int ret = setsockopt(m_fd, SOL_SOCKET, SO_RCVBUF, (const char*) &options.receive_buffer_size, sizeof(int));
        if (ret != 0)
        {
            WARN_LOG("Failed to set recv buf size(%u) for socket.", options.receive_buffer_size);
            //return false;
        }
    }
    if (options.send_buffer_size > 0)
    {
        int ret = setsockopt(m_fd, SOL_SOCKET, SO_SNDBUF, (const char*) &options.send_buffer_size, sizeof(int));
        if (ret != 0)
        {
            WARN_LOG("Failed to set send buf size(%u) for socket.", options.send_buffer_size);
            //return false;
        }
    }

    int flag = 1;
    bool set_tcp_dealy = false;
    if(m_options.tcp_nodelay != options.tcp_nodelay)
    {
        set_tcp_dealy = true;
        flag = options.tcp_nodelay? 1:0;
    }
    if (set_tcp_dealy && (setsockopt(m_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0))
    {
        WARN_LOG("init_sock_opt: could not disable/enable Nagle: %s", strerror(errno));
    }


    if (options.keep_alive > 0)
    {
        if (setsockopt(m_fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag)) < 0)
        {
            WARN_LOG("init_sock_opt: could not set keepalive: %s", strerror(errno));
            return false;
        }
#ifdef __linux__
        /* Default settings are more or less garbage, with the keepalive time
         * set to 7200 by default on Linux. Modify settings to make the feature
         * actually useful. */

        /* Send first probe after interval. */
        int val = options.keep_alive;
        if (setsockopt(m_fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0)
        {
            WARN_LOG("setsockopt TCP_KEEPIDLE: %s", strerror(errno));
            return false;
        }

        /* Send next probes after the specified interval. Note that we set the
         * delay as interval / 3, as we send three probes before detecting
         * an error (see the next setsockopt call). */
        val = options.keep_alive/3;
        if (val == 0) val = 1;
        if (setsockopt(m_fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0)
        {
            WARN_LOG("setsockopt TCP_KEEPINTVL: %s", strerror(errno));
            return false;
        }

        /* Consider the socket in error state after three we send three ACK
         * probes without getting a reply. */
        val = 3;
        if (setsockopt(m_fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0)
        {
            WARN_LOG("setsockopt TCP_KEEPCNT: %s", strerror(errno));
            return false;
        }
#endif
    }
    return true;
}

int32 SocketChannel::HandleExceptionEvent(int32 event)
{
    if (event & CHANNEL_EVENT_EOF)
    {
        return HandleIOError(ECONNRESET);
    }
    return 0;
}

bool SocketChannel::DoOpen()
{
    return true;
}

bool SocketChannel::DoBind(Address* local)
{
    if (NULL == local)
    {
        return false;
    }
    SocketInetAddress addr;
    if (InstanceOf<SocketHostAddress>(local).OK)
    {
        SocketHostAddress* host_addr = static_cast<SocketHostAddress*>(local);
        addr = get_inet_address(host_addr->GetHost(), host_addr->GetPort());
    }
    else if (InstanceOf<SocketInetAddress>(local).OK)
    {
        SocketInetAddress* inet_addr = static_cast<SocketInetAddress*>(local);
        addr = (*inet_addr);
    }
    else if (InstanceOf<SocketUnixAddress>(local).OK)
    {
        SocketUnixAddress* unix_addr = (SocketUnixAddress*) local;
        unlink(unix_addr->GetPath().c_str()); // in case it already exists
        addr = get_inet_address(*unix_addr);
    }
    else
    {
        return false;
    }
    if (addr.IsUnix())
    {
        struct sockaddr_un* pun = (struct sockaddr_un*) &(addr.GetRawSockAddr());
        DEBUG_LOG("Bind on %s", pun->sun_path);
    }
    int fd = GetSocketFD(addr.GetDomain());
    if (0 == ::bind(fd, &(addr.GetRawSockAddr()), addr.GetRawSockAddrSize()))
    {
        return true;
    }
    int e = errno;
    ERROR_LOG("Failed to bind address for reason:%s", strerror(e));
    return false;
}

bool SocketChannel::DoConnect(Address* remote)
{
    if (NULL == remote)
    {
        return false;
    }
    SocketInetAddress addr;
    if (InstanceOf<SocketHostAddress>(remote).OK)
    {
        SocketHostAddress* host_addr = static_cast<SocketHostAddress*>(remote);
        addr = get_inet_address(host_addr->GetHost(), host_addr->GetPort());
    }
    else if (InstanceOf<SocketInetAddress>(remote).OK)
    {
        SocketInetAddress* inet_addr = static_cast<SocketInetAddress*>(remote);
        addr = (*inet_addr);
    }
    else if (InstanceOf<SocketUnixAddress>(remote).OK)
    {
        SocketUnixAddress* unix_addr = static_cast<SocketUnixAddress*>(remote);
        addr = get_inet_address(*unix_addr);
    }
    else
    {
        return false;
    }
    int fd = GetSocketFD(addr.GetDomain());
    int ret = ::connect(fd, const_cast<sockaddr*>(&(addr.GetRawSockAddr())), addr.GetRawSockAddrSize());
    if (ret < 0)
    {
        int e = errno;
        if (((e) == EINTR || (e) == EINPROGRESS))
        {
            //connecting
        }
        else
        {
            ERROR_LOG("Failed to connect remote server for reason:%s", strerror(e));
            return false;
        }

    }

    m_state = SOCK_CONNECTING;
    //DEBUG_LOG("###Switch to %d", m_state);
    return true;
}

const Address* SocketChannel::GetLocalAddress()
{
    if (NULL == m_localAddr)
    {
        try
        {
            //			SocketHostAddress local = getHostAddress(m_fd);
            //			NEW(m_localAddr, SocketHostAddress(local));

            SocketInetAddress inet = get_socket_inet_address(m_fd);
            if (inet.IsUnix())
            {
                SocketUnixAddress local = get_unix_address(inet);
                NEW(m_localAddr, SocketUnixAddress(local));
            }
            else
            {
                SocketHostAddress local = get_host_address(inet);
                NEW(m_localAddr, SocketHostAddress(local));
            }
        } catch (...)
        {

        }
    }
    return m_localAddr;
}
const Address* SocketChannel::GetRemoteAddress()
{
    if (NULL == m_remoteAddr)
    {
        try
        {

            SocketInetAddress inet = get_remote_inet_address(m_fd);
            if (inet.IsUnix())
            {
                SocketUnixAddress remote = get_unix_address(inet);
                NEW(m_remoteAddr, SocketUnixAddress(remote));
            }
            else
            {
                SocketHostAddress remote = get_host_address(inet);
                NEW(m_remoteAddr, SocketHostAddress(remote));
            }

        } catch (...)
        {

        }
    }
    return m_remoteAddr;
}

void SocketChannel::setRemoteAddress(Address* addr)
{
    DELETE(m_remoteAddr);
    m_remoteAddr = addr;
}

int SocketChannel::GetSocketFD(int domain)
{
    //DEBUG_LOG("###FD = %d", m_fd);
    if (m_fd < 0)
    {
        int fd = ::socket(domain, GetProtocol(), 0);
        if (fd < 0)
        {
            return fd;
        }
        int flags;
        if ((flags = fcntl(fd, F_GETFL)) < 0)
        {
            ::close(fd);
            return -1;
        }
        if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0)
        {
            ::close(fd);
            return -1;
        }
        if (make_fd_nonblocking(fd) < 0)
        {
            ::close(fd);
            return -1;
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
        if (aeCreateFileEvent(GetService().GetRawEventLoop(), fd,
        AE_READABLE | AE_WRITABLE, Channel::IOEventCallback, this) == AE_ERR)
        {
            ::close(fd);
            return -1;
        }
        m_fd = fd;
        //DEBUG_LOG("###Create FD = %d", m_fd);
    }
    return m_fd;
}

void SocketChannel::OnWrite()
{
    //DEBUG_LOG("############st is %d", m_state);
    if (SOCK_CONNECTING == m_state || SOCK_INIT == m_state)
    {
        aeDeleteFileEvent(GetService().GetRawEventLoop(), m_fd, AE_WRITABLE);
        m_state = SOCK_CONNECTED;
        fire_channel_connected(this);
    }
    else
    {
        Channel::OnWrite();
    }
}

bool SocketChannel::DoClose()
{
    if (-1 != m_fd)
    {
        //if (m_user_configed && m_options.reset_timedout_connection)
        {
            //            struct linger linger;
            //            linger.l_onoff = 1;
            //            linger.l_linger = 15;
            //            if (setsockopt(m_fd, SOL_SOCKET, SO_LINGER, (const void *) &linger,
            //                    sizeof(struct linger)) == -1)
            //            {
            //                WARN_LOG("setsockopt(SO_LINGER) failed");
            //            }
        }
    }
    if (Channel::DoClose())
    {
        m_state = SOCK_CLOSED;
        return true;
    }
    return false;

}

void SocketChannel::OnAccepted()
{
    if (aeCreateFileEvent(GetService().GetRawEventLoop(), m_fd, AE_READABLE, Channel::IOEventCallback, this) == AE_ERR)
    {
        int err = errno;
        ERROR_LOG("Failed to add event for accepted client for fd:%d for reason:%s", m_fd, strerror(err));
        Close();
        return;
    }
    m_detached = false;
    m_state = SOCK_CONNECTED;
    fire_channel_open(this);
    fire_channel_connected(this);

    //INFO_LOG("Accepted on tid:%d", Thread::CurrentThreadID());
}

std::string SocketChannel::GetRemoteStringAddress()
{
    const Address* remote = GetRemoteAddress();
    if (InstanceOf<SocketUnixAddress>(remote).OK)
    {
        const SocketUnixAddress* addr = (const SocketUnixAddress*) remote;
        return addr->GetPath();
    }
    else if (InstanceOf<SocketHostAddress>(remote).OK)
    {
        const SocketHostAddress* addr = (const SocketHostAddress*) remote;
        char tmp[256];
        sprintf(tmp, "%s:%u", addr->GetHost().c_str(), addr->GetPort());
        return tmp;
    }
    return "";
}

SocketChannel::~SocketChannel()
{
    DELETE(m_localAddr);
    DELETE(m_remoteAddr);
}

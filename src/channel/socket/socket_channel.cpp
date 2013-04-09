/*
 * SocketChannel.cpp
 *
 *  Created on: 2011-1-12
 *      Author: qiyingwang
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
		int ret = setsockopt(m_fd, SOL_SOCKET, SO_RCVBUF,
				(const char*) &options.receive_buffer_size, sizeof(int));
		if (ret != 0)
		{
			WARN_LOG(
					"Failed to set recv buf size(%u) for socket.", options.receive_buffer_size);
			//return false;
		}
	}
	if (options.send_buffer_size > 0)
	{
		int ret = setsockopt(m_fd, SOL_SOCKET, SO_SNDBUF,
				(const char*) &options.send_buffer_size, sizeof(int));
		if (ret != 0)
		{
			WARN_LOG(
					"Failed to set send buf size(%u) for socket.", options.send_buffer_size);
			//return false;
		}
	}

	int flag = 1;
	if (options.tcp_nodelay
			&& (setsockopt(m_fd, SOL_SOCKET, TCP_NODELAY, &flag, sizeof(flag))
					< 0))
	{
		WARN_LOG("init_sock_opt: could not disable Nagle: %s", strerror(errno));
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
	} else if (InstanceOf<SocketInetAddress>(local).OK)
	{
		SocketInetAddress* inet_addr = static_cast<SocketInetAddress*>(local);
		addr = (*inet_addr);
	} else if (InstanceOf<SocketUnixAddress>(local).OK)
	{
		SocketUnixAddress* unix_addr = (SocketUnixAddress*) local;
		unlink(unix_addr->GetPath().c_str()); // in case it already exists
		addr = get_inet_address(*unix_addr);
	} else
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
	} else if (InstanceOf<SocketInetAddress>(remote).OK)
	{
		SocketInetAddress* inet_addr = static_cast<SocketInetAddress*>(remote);
		addr = (*inet_addr);
	} else if (InstanceOf<SocketUnixAddress>(remote).OK)
	{
		SocketUnixAddress* unix_addr = static_cast<SocketUnixAddress*>(remote);
		addr = get_inet_address(*unix_addr);
	} else
	{
		return false;
	}
	int fd = GetSocketFD(addr.GetDomain());
	int ret = ::connect(fd, const_cast<sockaddr*>(&(addr.GetRawSockAddr())),
			addr.GetRawSockAddrSize());
	if (ret < 0)
	{
		int e = errno;
		if (((e) == EINTR || (e) == EINPROGRESS))
		{
			//connecting
		} else
		{
			ERROR_LOG(
					"Failed to connect remote server for reason:%s", strerror(e));
			return false;
		}

	}
	m_state = SOCK_CONNECTING;
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
			} else
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
			} else
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
	if (m_fd < 0)
	{
		//int fd = ::socket(isIPV6 ? AF_INET6 : AF_INET, GetProtocol(), 0);
		int fd = ::socket(domain, GetProtocol(), 0);
//		if (domain == AF_UNIX)
//		{
//			int nZero = 0;
//			setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *) &nZero,
//			        sizeof(nZero));
//			setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *) &nZero, sizeof(int));
//		}
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
		if (aeCreateFileEvent(m_service.GetRawEventLoop(), fd,
				AE_READABLE | AE_WRITABLE, Channel::IOEventCallback,
				this) == AE_ERR)
		{
			::close(fd);
			return -1;
		}
		m_fd = fd;
	}
	return m_fd;
}

void SocketChannel::OnWrite()
{
	if (SOCK_CONNECTING == m_state)
	{
		aeDeleteFileEvent(m_service.GetRawEventLoop(), m_fd, AE_WRITABLE);
		m_state = SOCK_CONNECTED;
		fire_channel_connected(this);
	} else
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

SocketChannel::~SocketChannel()
{
	DELETE(m_localAddr);
	DELETE(m_remoteAddr);
}

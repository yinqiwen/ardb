/*
 * NetworkHelper.cpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */
#include "network_helper.hpp"
#include "util/exception/api_exception.hpp"
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <net/if.h>
#include <string.h>
#include <stdio.h>

namespace ardb
{

	static const uint16 kMaxNICCount = 16;
	SocketInetAddress get_inet_address(const SocketHostAddress& addr)
	{
		return get_inet_address(addr.GetHost().c_str(), addr.GetPort());
	}

	SocketInetAddress get_inet_address(const SocketUnixAddress& unix_addr)
	{
		struct sockaddr_un addr;
		memset(&addr, 0, sizeof(addr));

		addr.sun_family = AF_UNIX;
		strcpy(addr.sun_path, unix_addr.GetPath().c_str());
		//SocketInetAddress ret(addr);
		return SocketInetAddress(addr);
	}

	SocketInetAddress get_inet_address(const string& host, uint16 port)
	{
		struct addrinfo hints;
		struct addrinfo* res = NULL;
		//struct sockaddr addr;
		//memset(&addr, 0, sizeof(addr));
		memset(&hints, 0, sizeof(hints));
		hints.ai_flags = 0;
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = 0;
		hints.ai_protocol = 0;

		char szPort[10];
		sprintf(szPort, "%d", port);
		int retvalue = getaddrinfo(host.c_str(), szPort, &hints, &res);
		SocketInetAddress ret;
		if (0 != retvalue)
		{
			freeaddrinfo(res);
			//throw APIException(errno);
			return ret;
		}

		if (NULL != res)
		{
			struct sockaddr* addr = res->ai_addr;
			if (addr->sa_family == AF_INET)
			{
				sockaddr_in* in = (sockaddr_in*) (addr);
				ret = SocketInetAddress(*in);
			} else if (addr->sa_family == AF_INET6)
			{
				sockaddr_in6* in6 = (sockaddr_in6*) (addr);
				ret = SocketInetAddress(*in6);
			} else if (addr->sa_family == AF_UNIX)
			{
				sockaddr_un* un = (sockaddr_un*) (addr);
				ret = SocketInetAddress(*un);
			}
		}
		freeaddrinfo(res);
		return ret;
	}

	SocketHostAddress get_host_address(const sockaddr& addr)
	{
		SocketHostAddress invalid("", 0);
		int size;
		if (addr.sa_family == AF_INET || addr.sa_family != AF_INET6)
		{
			//ipv4
			char host[INET_ADDRSTRLEN + 1];
			size = sizeof(sockaddr_in);
			sockaddr_in* paddr = (sockaddr_in*) (&addr);
			if (NULL != inet_ntop(AF_INET, &(paddr->sin_addr), host, size))
			{
				return SocketHostAddress(host, ntohs(paddr->sin_port) );
			}
			return invalid;
		} else
		{
			//ipv6
			char host[INET6_ADDRSTRLEN + 1];
			size = sizeof(sockaddr_in6);
			sockaddr_in6* paddr = (sockaddr_in6*) (&addr);
			if (NULL != inet_ntop(AF_INET6, &(paddr->sin6_addr), host, size))
			{
				sockaddr_in6* paddr = (sockaddr_in6*) (&addr);
				return SocketHostAddress(host, ntohs(paddr->sin6_port) );
			}
			return invalid;
		}
	}

	SocketUnixAddress get_unix_address(const sockaddr& addr)
	{
		struct sockaddr_un* pun = (struct sockaddr_un*) &addr;
		const char* path = pun->sun_path;
		return SocketUnixAddress(path);
	}
	SocketUnixAddress get_unix_address(const SocketInetAddress& addr)
	{
		return get_unix_address(addr.GetRawSockAddr());
	}

	SocketHostAddress get_host_address(const SocketInetAddress& addr)
	{
		return get_host_address(addr.GetRawSockAddr());
	}

	SocketInetAddress get_socket_inet_address(int32 fd)
	{
		char temp[256];
		memset(&temp, 0, sizeof(temp));
		sockaddr* addr = (sockaddr*) temp;
		socklen_t socklen = sizeof(temp);
		if (0 == getsockname(fd, addr, &socklen))
		{
			if (addr->sa_family == AF_INET)
			{
				sockaddr_in* in = (sockaddr_in*) addr;
				return SocketInetAddress(*in);
			} else if (addr->sa_family == AF_INET6)
			{
				sockaddr_in6* in6 = (sockaddr_in6*) addr;
				return SocketInetAddress(*in6);
			} else if (addr->sa_family == AF_UNIX)
			{
				sockaddr_un* un = (sockaddr_un*) addr;
				return SocketInetAddress(*un);
			}
		}
		return SocketInetAddress();
	}

	SocketHostAddress get_host_address(int32 fd)
	{
		SocketInetAddress inetaddr = get_socket_inet_address(fd);
		return get_host_address(inetaddr);
	}

	SocketInetAddress get_remote_inet_address(int32 fd)
	{

		char temp[256];
		memset(&temp, 0, sizeof(temp));
		sockaddr* addr = (sockaddr*) temp;
		socklen_t socklen = sizeof(temp);
		if (0 == getpeername(fd, addr, &socklen))
		{
			if (addr->sa_family == AF_INET)
			{
				sockaddr_in* in = (sockaddr_in*) addr;
				return SocketInetAddress(*in);
			} else if (addr->sa_family == AF_INET6)
			{
				sockaddr_in6* in6 = (sockaddr_in6*) addr;
				return SocketInetAddress(*in6);
			} else if (addr->sa_family == AF_UNIX)
			{
				sockaddr_un* un = (sockaddr_un*) addr;
				return SocketInetAddress(*un);
			}

		}
		return SocketInetAddress();
	}

	SocketHostAddress get_remote_host_address(int32 fd)
	{
		SocketInetAddress inetaddr = get_remote_inet_address(fd);
		return get_host_address(inetaddr);
	}

	uint64 htonll(uint64 v)
	{
		int num = 42;
		//big or little
		if (*(char *) &num == 42)
		{
			uint64_t temp = htonl(v & 0xFFFFFFFF);
			temp <<= 32;
			return temp | htonl(v >> 32);
		} else
		{
			return v;
		}

	}

	uint64 ntohll(uint64 v)
	{
		return htonll(v);
	}

	int get_ip_by_nic_name(const std::string& ifName, std::string& ip)
	{
		int fd;
		int intrface;
		struct ifconf ifc;
		struct ifreq ifr[kMaxNICCount];

		if (-1 == (fd = socket(AF_INET, SOCK_DGRAM, 0)))
		{
			return -1;
		}

		ifc.ifc_len = sizeof(ifr);
		ifc.ifc_buf = (caddr_t) ifr;

		if (-1 == ioctl(fd, SIOCGIFCONF, (char *) &ifc))
		{
			::close(fd);
			return -1;
		}

		intrface = ifc.ifc_len / sizeof(struct ifreq);
		while (intrface-- > 0)
		{
			/*Get IP of the net card */
			if (-1 == ioctl(fd, SIOCGIFADDR, (char *) &ifr[intrface]))
				continue;
			if (NULL == ifr[intrface].ifr_name)
				continue;

			if (0 == strcmp(ifName.c_str(), ifr[intrface].ifr_name))
			{
				SocketHostAddress addr = get_host_address(
						ifr[intrface].ifr_addr);
				ip = addr.GetHost();
				::close(fd);
				return 0;
			}
		}

		close(fd);
		return -1;
	}

	int get_local_host_ip_list(std::vector<std::string>& iplist)
	{
		int fd;
		int intrface;
		struct ifconf ifc;
		struct ifreq ifr[kMaxNICCount];

		if (-1 == (fd = socket(AF_INET, SOCK_DGRAM, 0)))
		{
			return -1;
		}

		ifc.ifc_len = sizeof(ifr);
		ifc.ifc_buf = (caddr_t) ifr;

		if (-1 == ioctl(fd, SIOCGIFCONF, (char *) &ifc))
		{
			::close(fd);
			return -1;
		}

		intrface = ifc.ifc_len / sizeof(struct ifreq);
		while (intrface-- > 0)
		{
			/*Get IP of the net card */
			if (-1 == ioctl(fd, SIOCGIFADDR, (char *) &ifr[intrface]))
				continue;
			if (NULL == ifr[intrface].ifr_name)
				continue;

			SocketHostAddress addr = get_host_address(ifr[intrface].ifr_addr);
			std::string ip = addr.GetHost();
			iplist.push_back(ip);
		}

		close(fd);
		return -1;
	}
}

